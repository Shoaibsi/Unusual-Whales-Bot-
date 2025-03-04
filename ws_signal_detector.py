import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from loguru import logger

@dataclass
class SignalConfig:
    min_gamma_exposure: float = 500000
    min_volume_spike: float = 2.0
    min_dark_pool_size: int = 50000
    min_net_premium: float = 25000
    min_options_volume: float = 5000
    min_open_interest: int = 500
    confirmation_window: int = 10

@dataclass
class TradingSignal:
    ticker: str
    signal_type: str
    confidence: float
    suggested_direction: str
    risk_score: float
    primary_data: Dict
    confirmation_data: Dict
    timestamp: datetime = datetime.now()
    options_data: Optional[List[Dict]] = None
    alert_data: Optional[Dict] = None

class BaseSignalDetector:
    def __init__(self, api_client, config: SignalConfig):
        self.api_client = api_client
        self.config = config
        self.logger = logger
        
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        raise NotImplementedError("Subclasses must implement this method")
        
    async def _get_recommended_contracts(self, ticker: str, direction: str) -> List[Dict]:
        try:
            options_data = await self.api_client.get_options_contracts(ticker)
            if not options_data or 'data' not in options_data:
                self.logger.info(f"No options data for {ticker}")
                return []
            options_list = options_data.get('data', [])
            if not options_list:
                return []
            option_type = 'call' if direction.upper() == 'LONG' else 'put'
            recommended = []
            for opt in options_list:
                # Skip options with empty expiration dates
                expiration = opt.get('expiration', '')
                if not expiration:
                    continue
                
                try:
                    dte = (datetime.strptime(expiration, '%Y-%m-%d') - datetime.now()).days
                    oi = opt.get('open_interest', 0)
                    vol = opt.get('volume', 0)
                    
                    # Convert implied_volatility to float if it's a string
                    implied_vol = opt.get('implied_volatility', 0)
                    if isinstance(implied_vol, str):
                        try:
                            implied_vol = float(implied_vol)
                        except (ValueError, TypeError):
                            implied_vol = 0
                    
                    if (opt.get('option_type', '').lower() == option_type.lower() and
                        7 <= dte <= 45 and vol > 100 and oi > self.config.min_open_interest):
                        score = (vol / 5000) * 0.4 + (oi / 5000) * 0.4 + (1 - implied_vol) * 0.2
                        recommended.append({
                            'type': opt.get('option_type', option_type),
                            'strike': opt.get('strike'),
                            'expiry': expiration,
                            'volume': vol,
                            'open_interest': oi,
                            'implied_volatility': implied_vol,
                            'score': score
                        })
                except ValueError as e:
                    self.logger.warning(f"Invalid date format for {ticker} option: {expiration}")
                    continue
                    
            return sorted(recommended, key=lambda x: x['score'], reverse=True)[:3]
        except Exception as e:
            self.logger.error(f"Error getting contracts for {ticker}: {str(e)}")
            return []

class GammaSqueezeDetector(BaseSignalDetector):
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        try:
            greek_flow = await self.api_client.get_greek_flow(ticker)
            expiry = None
            if greek_flow and 'data' in greek_flow and greek_flow['data']:
                expiry = greek_flow['data'][0].get('expiration')
            # Get expiry-specific Greek flow if an expiry is available
            greek_flow_expiry = await self.api_client.get_greek_flow_expiry(ticker, expiry) if expiry else None
            vol_data = await self.api_client.get_realized_volatility(ticker)
            if not greek_flow or 'data' not in greek_flow or not vol_data or 'data' not in vol_data:
                return None
            latest_flow = greek_flow['data'][0] if greek_flow['data'] else None
            vol = vol_data['data'][0] if vol_data['data'] and isinstance(vol_data['data'], list) else vol_data['data']
            if not latest_flow or not vol:
                return None
            total_gamma = float(latest_flow.get('total_vega_flow', 0))
            volume = int(latest_flow.get('volume', 0))
            transactions = int(latest_flow.get('transactions', 0))
            realized_vol = float(vol.get('realizedVolatility30d', 0))
            # Use expiry-specific data if available for refinement
            if greek_flow_expiry and 'data' in greek_flow_expiry and greek_flow_expiry['data']:
                expiry_flow = greek_flow_expiry['data'][0]
                total_gamma = max(total_gamma, float(expiry_flow.get('total_vega_flow', 0)))
            if total_gamma > self.config.min_gamma_exposure and volume > 500 and transactions > 100:
                delta_flow = float(latest_flow.get('dir_delta_flow', 0))
                direction = "LONG" if delta_flow > 0 else "SHORT"
                confidence = min(1.0, (total_gamma / self.config.min_gamma_exposure) * 0.3 +
                               (volume / 2500) * 0.3 + (transactions / 250) * 0.2 +
                               (1 - abs(realized_vol - 0.5)) * 0.2)
                risk_score = min(1.0, abs(delta_flow) / 100000)
                options_data = await self._get_recommended_contracts(ticker, direction)
                return TradingSignal(
                    ticker=ticker,
                    signal_type="GAMMA_SQUEEZE",
                    confidence=confidence,
                    suggested_direction=direction,
                    risk_score=risk_score,
                    primary_data=latest_flow,
                    confirmation_data={'volume': volume, 'transactions': transactions, 'realized_vol': realized_vol},
                    options_data=options_data,
                    alert_data=alert
                )
        except Exception as e:
            self.logger.error(f"Gamma squeeze error for {ticker}: {str(e)}")
        return None

class OptionsFlowAnomalyDetector(BaseSignalDetector):
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        try:
            # Get flow alerts
            flow_alerts = await self.api_client.get_flow_alerts({'ticker_symbol': ticker})
            if not flow_alerts:
                return None
            
            # Filter for significant alerts based on premium
            significant_alerts = [a for a in flow_alerts if a.get('premium', 0) > self.config.min_net_premium]
            if not significant_alerts:
                return None
            
            # Get volume and open interest data
            vol_oi_data = await self.api_client.get_volume_oi_by_expiry(ticker)
            if not vol_oi_data or 'data' not in vol_oi_data:
                return None
            
            # Get net premium ticks for additional confirmation
            net_premium_data = await self.api_client.get_net_premium_ticks(ticker)
            
            # Get options volume data to check for call/put skew
            options_volume = await self.api_client.get_options_volume(ticker)
            
            # Determine if we're seeing sweep orders (multiple smaller orders) vs block orders (single large order)
            sweep_orders = [a for a in significant_alerts if a.get('is_sweep', False)]
            block_orders = [a for a in significant_alerts if not a.get('is_sweep', False) and a.get('premium', 0) > self.config.min_net_premium * 2]
            
            # Calculate call/put ratio to detect skew
            call_volume = options_volume.get('call_volume', 0) if options_volume else 0
            put_volume = options_volume.get('put_volume', 0) if options_volume else 0
            volume_skew = call_volume / max(put_volume, 1) if call_volume and put_volume else 1.0
            
            # Determine if the skew is unusual (significantly different from 1.0)
            unusual_skew = abs(volume_skew - 1.0) > 0.5
            
            # Determine direction based on multiple factors
            call_premium = sum(a.get('premium', 0) for a in significant_alerts if a.get('option_type', '').lower() == 'call')
            put_premium = sum(a.get('premium', 0) for a in significant_alerts if a.get('option_type', '').lower() == 'put')
            
            # Direction is based on which has higher premium: calls or puts
            direction = "LONG" if call_premium > put_premium else "SHORT"
            
            # Calculate volume across all alerts
            volume = sum(a.get('volume', 0) for a in flow_alerts)
            
            # Find maximum open interest across all expiries
            max_oi = max((d.get('open_interest', 0) for d in vol_oi_data['data'] if isinstance(d, dict)), default=0)
            
            # Calculate volume to open interest ratio - higher ratios indicate unusual activity
            vol_oi_ratio = volume / max(max_oi, 1)
            
            # Calculate confidence based on multiple factors
            latest_alert = significant_alerts[0]
            confidence = min(1.0, 
                           (latest_alert.get('premium', 0) / self.config.min_net_premium) * 0.3 +  # Premium size
                           (volume / 1000) * 0.2 +  # Volume
                           (max_oi / 5000) * 0.1 +  # Open interest
                           (len(sweep_orders) / max(len(significant_alerts), 1)) * 0.2 +  # Sweep ratio
                           (vol_oi_ratio * 0.1) +  # Volume to OI ratio
                           (abs(volume_skew - 1.0) * 0.1))  # Call/put skew
            
            # Calculate risk score
            risk_score = min(1.0, latest_alert.get('premium', 0) / 500000)
            
            # Get recommended options contracts
            options_data = await self._get_recommended_contracts(ticker, direction)
            
            # Create detailed primary and confirmation data
            primary_data = {
                'latest_alert': latest_alert,
                'call_premium': call_premium,
                'put_premium': put_premium,
                'call_put_ratio': volume_skew,
                'sweep_count': len(sweep_orders),
                'block_count': len(block_orders)
            }
            
            confirmation_data = {
                'volume': volume,
                'max_open_interest': max_oi,
                'volume_oi_ratio': vol_oi_ratio,
                'unusual_skew': unusual_skew
            }
            
            # Add net premium data if available
            if net_premium_data and 'data' in net_premium_data:
                confirmation_data['net_premium_trend'] = net_premium_data.get('data', [])[:5]  # Last 5 data points
            
            return TradingSignal(
                ticker=ticker, 
                signal_type="OPTIONS_FLOW_ANOMALY", 
                confidence=confidence,
                suggested_direction=direction, 
                risk_score=risk_score, 
                primary_data=primary_data,
                confirmation_data=confirmation_data,
                options_data=options_data, 
                alert_data=alert
            )
        except Exception as e:
            self.logger.error(f"Options flow anomaly error for {ticker}: {str(e)}")
        return None

class DarkPoolSignalDetector(BaseSignalDetector):
    async def detect(self, ticker: str, alert: Optional[Dict] = None) -> Optional[TradingSignal]:
        try:
            dark_pool_data = await self.api_client.get_dark_pool_activity(ticker)
            if not dark_pool_data or not dark_pool_data.get('data'):
                self.logger.debug(f"No dark pool data for {ticker}, skipping")
                return None
            # Aggregate dark pool trades exceeding the threshold
            recent_trades = [trade for trade in dark_pool_data.get('data', []) if int(trade.get('size', 0)) > self.config.min_dark_pool_size]
            if not recent_trades:
                return None
            total_volume = sum(int(trade.get('size', 0)) for trade in recent_trades)
            avg_buy_sell = sum(float(trade.get('buy_sell_ratio', 1.0)) for trade in recent_trades) / len(recent_trades)
            direction = "LONG" if avg_buy_sell > 1 else "SHORT"
            vol_oi_data = await self.api_client.get_volume_oi_by_expiry(ticker)
            max_oi = max((d.get('open_interest', 0) for d in vol_oi_data.get('data', []) if isinstance(d, dict)), default=0)
            confidence = min(1.0, (total_volume / self.config.min_dark_pool_size) * 0.5 +
                           abs(avg_buy_sell - 1) * 0.3 + (max_oi / 5000) * 0.2)
            risk_score = min(1.0, abs(avg_buy_sell - 1) * 2)
            options_data = await self._get_recommended_contracts(ticker, direction)
            return TradingSignal(
                ticker=ticker,
                signal_type="DARK_POOL_ACTIVITY",
                confidence=confidence,
                suggested_direction=direction,
                risk_score=risk_score,
                primary_data={'aggregated_volume': total_volume, 'avg_buy_sell_ratio': avg_buy_sell},
                confirmation_data={'max_oi': max_oi},
                options_data=options_data,
                alert_data=alert
            )
        except Exception as e:
            self.logger.warning(f"Dark pool detection failed for {ticker}: {str(e)}")
        return None

class WSSignalDetector:
    def __init__(self, api_client, config: SignalConfig = None):
        self.api_client = api_client
        self.config = config or SignalConfig()
        self.logger = logger
        self.detectors = [
            GammaSqueezeDetector(api_client, self.config),
            OptionsFlowAnomalyDetector(api_client, self.config),
            DarkPoolSignalDetector(api_client, self.config)
        ]
        
    async def detect_signals(self, ticker: str, alert: Optional[Dict] = None) -> List[TradingSignal]:
        signals = [s async for s in self._detect_signals_async(ticker, alert)]
        return signals
        
    async def _detect_signals_async(self, ticker: str, alert: Optional[Dict] = None):
        for detector in self.detectors:
            signal = await detector.detect(ticker, alert)
            if signal:
                yield signal
                
    async def scan_market(self) -> List[TradingSignal]:
        signals = []
        flow_alerts = await self.api_client.get_flow_alerts()
        if not flow_alerts:
            self.logger.warning("No flow alerts retrieved")
            return signals
        tickers = list(set(alert['ticker'] for alert in flow_alerts if alert.get('ticker')))
        self.logger.info(f"Scanning {len(tickers)} tickers from flow alerts")
        tasks = [self.detect_signals(ticker, next((a for a in flow_alerts if a.get('ticker') == ticker), None))
                 for ticker in tickers[:20]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                signals.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Signal detection error: {str(result)}")
        self.logger.info(f"Found {len(signals)} signals from flow alerts and related data")
        return signals