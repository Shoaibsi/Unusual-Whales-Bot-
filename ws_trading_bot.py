import os
import json
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional
from loguru import logger
from dotenv import load_dotenv
import math

load_dotenv()

from ws_api_client import WSUnusualWhalesClient
from ws_signal_detector import WSSignalDetector, SignalConfig, TradingSignal
from telegram_notifier import TelegramNotifier
from tastytrade_client import TastytradeClient
from grok_analyzer import GrokSignalAnalyzer
from performance_tracker import PerformanceTracker
from chart_generator import ChartGenerator

class WSTradingBot:
    def __init__(self, config_path: str = None, signal_config: SignalConfig = None):
        self.logger = logger
        self.config = self._load_config(config_path or "config/config.json")
        self.watchlist = self.config.get("watchlist", [])
        self.market_data = {}

        self.telegram = TelegramNotifier(
            os.getenv("TELEGRAM_BOT_TOKEN") or self.config.get("telegram_token"),
            os.getenv("TELEGRAM_CHAT_ID") or self.config.get("telegram_chat_id")
        )
        self.api_client = WSUnusualWhalesClient(notifier=self.telegram)
        self.signal_detector = WSSignalDetector(self.api_client, signal_config or SignalConfig(**self.config.get("signal_config", {})))
        self.grok_analyzer = GrokSignalAnalyzer()
        self.performance_tracker = PerformanceTracker()
        self.tastytrade = TastytradeClient(paper_trading=True)  # Explicitly set for sandbox
        self.chart_generator = ChartGenerator()

        self.trade_history = []
        self.active_trades = {}
        self.running = False
        self.analyze_with_ai = self.config.get("analyze_with_ai", False)
        self.execute_trades = self.config.get("execute_trades", False)

    async def initialize(self) -> bool:
        """Initialize trading bot components"""
        logger.info("Initializing trading bot components...")
        try:
            # Initialize API client - This is critical, so we fail if it can't initialize
            if self.api_client and not await self.api_client.initialize():
                raise RuntimeError("Failed to initialize Unusual Whales client")
                
            # Initialize Tastytrade client - This can fail but we continue with limited functionality
            if self.tastytrade:
                tastytrade_initialized = await self.tastytrade.initialize()
                if not tastytrade_initialized:
                    logger.error("Tastytrade client initialization failed; proceeding with limited functionality")
                    self.execute_trades = False  # Disable trade execution since API is not working
                else:
                    logger.info("Tastytrade client initialized successfully")
                    self.execute_trades = self.config.get("execute_trades", False)
            
            # Initialize Telegram notifier - Not critical, just log warning if it fails
            if self.telegram and not await self.telegram.initialize():
                logger.warning("Telegram notifier initialization failed")
            else:
                if self.telegram:
                    await self.telegram.send_system_message("🚀 Unusual Whales Trading Bot started")
                
            # Initialize Grok analyzer - Not critical, just log warning if it fails  
            if self.grok_analyzer and not await self.grok_analyzer.initialize():
                logger.warning("Grok analyzer initialization failed; AI analysis will be disabled")
                self.analyze_with_ai = False
                
            logger.info("All components initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            return False

    async def cleanup(self):
        """Clean up resources before shutdown"""
        logger.info("Cleaning up trading bot resources...")
        # Clean up in reverse order of initialization
        cleanup_tasks = []
        
        if self.api_client:
            cleanup_tasks.append(self.api_client.cleanup())
        if self.grok_analyzer:
            cleanup_tasks.append(self.grok_analyzer.cleanup())
        if self.telegram:
            cleanup_tasks.append(self.telegram.cleanup())
        if self.tastytrade:
            cleanup_tasks.append(self.tastytrade.cleanup())
            
        if cleanup_tasks:
            try:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
                
        logger.info("Trading bot cleanup complete")

    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load config from {config_path}: {str(e)}")
            return {}

    async def _execute_option_trade(self, signal: TradingSignal, grok_analysis: Dict = {}) -> bool:
        try:
            chain_data = await self.tastytrade.get_option_chain(signal.ticker)
            if not chain_data or 'contracts' not in chain_data or not chain_data['contracts']:
                self.logger.warning(f"No valid contracts for {signal.ticker}, falling back to signal options data")
                contract = signal.options_data[0] if signal.options_data and isinstance(signal.options_data, list) else None
            else:
                contract = self._select_option_contract(chain_data, signal)
            if not contract:
                self.logger.error(f"No suitable options contract for {signal.ticker}")
                return False
            mark_price = contract.get('mark_price', 0)
            self.logger.debug(f"Contract mark_price for {signal.ticker}: {mark_price}")
            if mark_price <= 0:
                self.logger.error(f"Invalid mark price ({mark_price}) for {signal.ticker}, cannot execute trade")
                return False
            quantity = self._calculate_position_size(mark_price, signal.risk_score)
            if quantity <= 0:
                self.logger.warning(f"Calculated quantity ({quantity}) is invalid for {signal.ticker}, skipping trade")
                return False
            trade_result = await self._execute_option_trade(
                symbol=signal.ticker,
                expiration=contract.get('expiration', ''),
                strike=contract.get('strike', 0.0),
                option_type=contract.get('option_type', 'call'),
                quantity=quantity,
                price_limit=contract.get('ask', mark_price) or mark_price
            ) if signal.suggested_direction == "LONG" else await self._execute_option_trade(
                symbol=signal.ticker,
                expiration=contract.get('expiration', ''),
                strike=contract.get('strike', 0.0),
                option_type=contract.get('option_type', 'call'),
                quantity=quantity,
                price_limit=contract.get('bid', mark_price) or mark_price
            )
            if trade_result and trade_result.get('status') == "placed":
                self.logger.info(f"Trade executed for {signal.ticker}")
                self._record_trade(signal, contract, quantity, trade_result)
                return True
            return False
        except Exception as e:
            self.logger.error(f"Trade execution error for {signal.ticker}: {str(e)}")
            return False

    async def _execute_option_trade(self, symbol: str, expiration: str, strike: float, option_type: str, quantity: int, price_limit: float):
        """Execute an option trade"""
        if not hasattr(self, 'tastytrade') or not self.tastytrade or not hasattr(self.tastytrade, 'authenticated') or not self.tastytrade.authenticated:
            self.logger.error(f"Cannot execute trade: Tastytrade client not properly initialized")
            return None
        
        try:
            # Prepare option order parameters
            order_params = {
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "option_type": option_type,
                "quantity": quantity,
                "price_limit": price_limit,
                "order_type": "limit",
                "time_in_force": "day"
            }
            
            # Execute the option order
            self.logger.info(f"Executing option order: {order_params}")
            order_result = await self.tastytrade.place_option_order(**order_params)
            
            if order_result:
                self.logger.info(f"Order placed successfully: {order_result}")
                # Send notification
                await self.telegram.send_message(
                    f"🚨 TRADE EXECUTED 🚨\n\n"
                    f"Symbol: {symbol}\n"
                    f"Type: {option_type.upper()}\n"
                    f"Strike: ${strike}\n"
                    f"Expiration: {expiration}\n"
                    f"Quantity: {quantity}\n"
                    f"Price: ${price_limit}\n"
                    f"Total Cost: ${round(price_limit * quantity * 100, 2)}"
                )
                return order_result
            else:
                self.logger.error("Order placement failed with unknown error")
                return None
                
        except Exception as e:
            self.logger.error(f"Error executing option trade: {e}")
            return None

    def _find_optimal_contract(self, contracts: List[Dict], signal: TradingSignal) -> Optional[Dict]:
        try:
            # Find the optimal contract based on the signal
            optimal_contract = None
            max_score = 0
            for contract in contracts:
                score = self._score_contract(contract, signal.suggested_direction)
                if score > max_score:
                    max_score = score
                    optimal_contract = contract
            return optimal_contract
        except Exception as e:
            self.logger.error(f"Error finding optimal contract for {signal.ticker}: {str(e)}")
            return None

    def _score_contract(self, contract: Dict, direction: str) -> float:
        try:
            dte = (datetime.strptime(contract.get('expiration', ''), '%Y-%m-%d') - datetime.now()).days
            score = 0
            if 30 <= dte <= 45: score += 2
            elif 15 <= dte <= 60: score += 1
            abs_delta = abs(float(contract.get('delta', 0)))
            if 0.4 <= abs_delta <= 0.6: score += 2
            elif 0.2 <= abs_delta <= 0.8: score += 1
            if contract.get('volume', 0) > 100 and contract.get('open_interest', 0) > 500: score += 1
            return score
        except Exception as e:
            self.logger.warning(f"Scoring error for contract: {str(e)}")
            return 0

    def _calculate_position_size(self, option_price: float, risk_score: float) -> int:
        try:
            if option_price <= 0:
                self.logger.error(f"Invalid option price ({option_price}) for position size calculation")
                return 0
            account_value = float(self.config.get("account_value", 100000))
            max_position_pct = float(self.config.get("max_position_size", 0.25))
            adjusted_value = account_value * max_position_pct * (1 - risk_score * 0.5)  # Use risk_score directly
            quantity = max(1, int(adjusted_value / (option_price * 100)))
            self.logger.debug(f"Calculated position size: {quantity} for price {option_price}, risk_score {risk_score}")
            return quantity
        except Exception as e:
            self.logger.error(f"Error in position size calculation: {str(e)}")
            return 0

    async def _handle_signal(self, signal: TradingSignal):
        try:
            self.logger.info(f"New signal detected for {signal.ticker}: {signal.signal_type}, Confidence: {signal.confidence:.2f}%")
            
            # Check if Tastytrade client is properly initialized
            if not hasattr(self, 'tastytrade') or not self.tastytrade or not hasattr(self.tastytrade, 'authenticated') or not self.tastytrade.authenticated:
                self.logger.error(f"Tastytrade client not properly initialized, skipping signal for {signal.ticker}")
                return
            
            # Use GPT-4 for analysis
            if self.grok_analyzer and self.analyze_with_ai:
                analysis_result = await self.grok_analyzer.analyze_signal(signal)
                self.logger.debug(f"AI Analysis: {analysis_result}")
                if "avoid" in analysis_result.lower() or "skip" in analysis_result.lower():
                    self.logger.info(f"Skipping signal for {signal.ticker} based on AI analysis: {analysis_result}")
                    return
            
            # Determine option type (call/put), for basic signals use strategy
            if signal.signal_type in ["UNUSUAL_ACTIVITY", "SWEEPS", "BLOCK_TRADE", "GAMMA_SQUEEZE", "DARK_POOL_ACTIVITY"]:
                # For basic signals, use our strategy
                option_type = self._determine_option_type(signal.ticker)
            else:
                # For directional signals, use the signal direction
                option_type = "call" if signal.suggested_direction == "LONG" else "put"
                
            # Get option expiration
            expiration = self._get_expiration_date()
            
            # Get strike price based on ticker
            try:
                current_price = await self._get_stock_price(signal.ticker)
                if current_price is None:
                    self.logger.warning(f"Could not get current price for {signal.ticker}, skipping signal")
                    return
                    
                strike_price = self._calculate_strike_price(current_price, option_type)
                
                # Format for TastyTrade
                expiration_formatted = expiration.strftime("%Y-%m-%d")
                
                # Log the planned trade
                self.logger.info(f"Planning trade: {signal.ticker} {expiration_formatted} {strike_price} {option_type}")
                
                if self.execute_trades:
                    # Try to get option quote
                    try:
                        option_quote = await self.tastytrade.get_option_quote(
                            symbol=signal.ticker,
                            expiration=expiration_formatted,
                            strike=strike_price,
                            option_type=option_type
                        )
                        
                        if option_quote:
                            mid_price = option_quote.get("mid_price", 0)
                            # Adjust price to be slightly higher for better fill probability
                            limit_price = round(mid_price * 1.05, 2)
                            limit_price = max(limit_price, 0.05)  # Minimum $0.05
                            
                            # Execute the trade with calculated parameters
                            await self._execute_option_trade(
                                symbol=signal.ticker,
                                expiration=expiration_formatted,
                                strike=strike_price,
                                option_type=option_type,
                                quantity=self._calculate_position_size(signal.ticker, limit_price),
                                price_limit=limit_price
                            )
                        else:
                            self.logger.warning(f"No valid option quote returned for {signal.ticker}, skipping trade")
                    except Exception as e:
                        self.logger.error(f"Signal handling error for {signal.ticker}: {str(e)}")
                        if "404" in str(e) and "Not Found" in str(e):
                            self.logger.warning(f"The option contract for {signal.ticker} {expiration_formatted} {strike_price} {option_type} may not exist. Trying an alternative strike...")
                            # Try a more conservative strike price
                            try:
                                if option_type == "call":
                                    alt_strike = math.floor(current_price * 0.95)  # 5% lower strike for calls
                                else:
                                    alt_strike = math.ceil(current_price * 1.05)   # 5% higher strike for puts
                                    
                                self.logger.info(f"Trying alternative strike: {signal.ticker} {expiration_formatted} {alt_strike} {option_type}")
                                
                                option_quote = await self.tastytrade.get_option_quote(
                                    symbol=signal.ticker,
                                    expiration=expiration_formatted,
                                    strike=alt_strike,
                                    option_type=option_type
                                )
                                
                                if option_quote:
                                    mid_price = option_quote.get("mid_price", 0)
                                    limit_price = round(mid_price * 1.05, 2)
                                    limit_price = max(limit_price, 0.05)  # Minimum $0.05
                                    
                                    await self._execute_option_trade(
                                        symbol=signal.ticker,
                                        expiration=expiration_formatted,
                                        strike=alt_strike,
                                        option_type=option_type,
                                        quantity=self._calculate_position_size(signal.ticker, limit_price),
                                        price_limit=limit_price
                                    )
                                else:
                                    self.logger.warning(f"No valid option quote for alternative strike on {signal.ticker}, skipping trade")
                            except Exception as alt_e:
                                self.logger.error(f"Failed with alternative strike for {signal.ticker}: {str(alt_e)}")
                else:
                    self.logger.info(f"Trade execution disabled, not placing trade for {signal.ticker}")
            except Exception as e:
                self.logger.error(f"Error processing signal for {signal.ticker}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error handling signal for {signal.ticker if hasattr(signal, 'ticker') else 'unknown ticker'}: {str(e)}")

    def _determine_option_type(self, ticker: str) -> str:
        # Implement your strategy to determine option type
        pass

    def _get_expiration_date(self) -> datetime:
        # Implement your strategy to get expiration date
        pass

    def _calculate_strike_price(self, current_price: float, option_type: str) -> float:
        # Implement your strategy to calculate strike price
        pass

    def _get_stock_price(self, ticker: str) -> float:
        # Implement your strategy to get stock price
        pass

    async def _process_ticker_signals(self, ticker: str):
        try:
            market_data = await self.api_client.get_stock_market_data(ticker)
            signals = await self.signal_detector.detect_signals(ticker, market_data)
            for signal in signals:
                await self._handle_signal(signal)
            return signals
        except Exception as e:
            self.logger.error(f"Error processing signals for {ticker}: {str(e)}")
            return []

    async def start(self, scan_interval: int = 60):
        self.logger.info(f"Starting WS Trading Bot (scan interval: {scan_interval}s)...")
        self.running = True
        try:
            if not await self.api_client.initialize():
                self.logger.error("Failed to initialize API client. Check your API key.")
                return
            await self.telegram.send_system_message("🚀 Unusual Whales Trading Bot started")
            while self.running:
                try:
                    self.logger.info("Scanning market for trading signals...")
                    signals = await self.signal_detector.scan_market()
                    if signals:
                        self.logger.info(f"Found {len(signals)} potential trading signals")
                        for signal in sorted(signals, key=lambda s: s.confidence, reverse=True):
                            self.performance_tracker.record_signal(signal)
                            if signal.confidence >= 0.5:
                                self.logger.info(f"Processing signal for {signal.ticker}: {signal.signal_type} ({signal.confidence:.2f})")
                                await self._handle_signal(signal)
                            else:
                                self.logger.info(f"Skipping low confidence signal for {signal.ticker}: {signal.signal_type} ({signal.confidence:.2f})")
                    else:
                        self.logger.info("No trading signals detected in this scan")
                    self.logger.info(f"Waiting {scan_interval} seconds before next scan...")
                    await asyncio.sleep(scan_interval)
                except Exception as e:
                    self.logger.error(f"Error during market scan: {str(e)}")
                    await asyncio.sleep(scan_interval)
        except Exception as e:
            self.logger.error(f"Bot encountered an error: {str(e)}")
            raise
        finally:
            await self.cleanup()

    async def run(self, tickers: Optional[List[str]] = None):
        await self.initialize()
        try:
            await self.telegram.send_system_message("🚀 Unusual Whales Trading Bot started")
            while True:
                flow_alerts = await self.api_client.get_flow_alerts()
                if not flow_alerts:
                    self.logger.warning("No flow alerts retrieved")
                    await asyncio.sleep(self.config.get("scan_interval", 120))
                    continue
                self.logger.debug(f"Flow alerts retrieved: {len(flow_alerts)} entries")
                try:
                    # Handle both list and dictionary formats from the API
                    if isinstance(flow_alerts, dict):
                        alert_data = flow_alerts.get('data', [])
                    else:
                        # If flow_alerts is already a list, use it directly
                        alert_data = flow_alerts
                    
                    tickers_from_alerts = list(set(alert.get('ticker') for alert in alert_data if alert and isinstance(alert, dict) and alert.get('ticker')))
                    self.logger.info(f"Found {len(tickers_from_alerts)} unique tickers in flow alerts")
                    signals = await self.signal_detector.scan_market()
                    if signals:
                        self.logger.info(f"Detected {len(signals)} signals")
                        for signal in sorted(signals, key=lambda s: s.confidence, reverse=True):
                            await self._handle_signal(signal)
                except Exception as e:
                    self.logger.error(f"Error processing flow alerts: {str(e)}")
                
                await asyncio.sleep(self.config.get("scan_interval", 120))
        except Exception as e:
            self.logger.error(f"Bot error: {str(e)}")
            await self.telegram.send_system_message(f"🚨 Bot Error: {str(e)}", is_error=True)
        finally:
            await self.cleanup()
            await self.telegram.send_system_message("🔌 Trading Bot Shutdown Complete")