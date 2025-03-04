import os
import json
import time
import logging
import aiohttp
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
from dotenv import load_dotenv
import uuid
import importlib.util
import sys

# Dynamically import the TastytradeStreamer to avoid circular imports
try:
    if importlib.util.find_spec("ws_bot.src.tastytrade_streamer") is not None:
        from ws_bot.src.tastytrade_streamer import TastytradeStreamer
    else:
        # Try relative import path
        path = os.path.join(os.path.dirname(__file__), "tastytrade_streamer.py")
        spec = importlib.util.spec_from_file_location("tastytrade_streamer", path)
        streamer_module = importlib.util.module_from_spec(spec)
        sys.modules["tastytrade_streamer"] = streamer_module
        spec.loader.exec_module(streamer_module)
        TastytradeStreamer = streamer_module.TastytradeStreamer
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("TastytradeStreamer module not found, streaming features will be disabled")
    TastytradeStreamer = None

logger = logging.getLogger(__name__)

class TastytradeClient:
    """
    Client for interacting with the Tastytrade API.
    
    Handles authentication, account information, market data, and trade execution using real API calls.
    """
    
    def __init__(self, paper_trading: bool = True):
        """
        Initialize the Tastytrade client.
        
        Args:
            paper_trading: Whether to use the paper trading API
        """
        load_dotenv()
        self.username = os.getenv("TASTYWORKS_USERNAME")
        self.password = os.getenv("TASTYWORKS_PASSWORD")
        self.account_id = os.getenv("TASTYWORKS_ACCOUNT_ID")  # Can be None, will be fetched if needed
        
        self.paper_trading = paper_trading
        self.base_url = "https://api.tastytrade.com" if not paper_trading else "https://api.cert.tastyworks.com"
        self.session = None
        self.session_token = None
        self.authenticated = False
        self.remember_token = None
        self.streamer = None
        self.streaming_enabled = True  # Flag to track if streaming should be attempted
        
        self.last_rate_limited_time = 0
        self.rate_limit_window = 1  # seconds
        self.rate_limit_count = 0
        self.rate_limit_max = 10  # requests per window
        self.auth_url = f"{self.base_url}/sessions"
        self.accounts_url = f"{self.base_url}/customers/me/accounts"
        self.option_chains_url = f"{self.base_url}/option-chains/:symbol/nested"
        self.market_data_url = f"{self.base_url}/market-data"
        self.min_request_interval = 0.2  # 200ms between requests
        logger.info(f"Tastytrade client initialized (paper trading: {paper_trading})")

    async def initialize(self) -> bool:
        """Initialize the client session and authenticate"""
        try:
            self.session = aiohttp.ClientSession(headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "UnusualWhalesBot/1.0"  # Ensure User-Agent is set
            })
            
            logger.info(f"Attempting to authenticate with {self.auth_url}")
            if not await self.authenticate():
                logger.error("Failed to authenticate with Tastytrade")
                return False
                
            if not self.account_id:
                accounts = await self._get_accounts()
                if not accounts:
                    logger.error("No accounts found for this user")
                    return False
                self.account_id = accounts[0].get("account-number")
                logger.info(f"Selected account: {self.account_id}")
                
            # Initialize the streamer client for real-time market data
            if TastytradeStreamer and self.streaming_enabled:
                self.streamer = TastytradeStreamer(self)
                streamer_init = await self.streamer.initialize()
                if streamer_init:
                    streamer_connect = await self.streamer.connect()
                    if not streamer_connect:
                        logger.warning("Failed to connect to streaming service, will use REST API fallback for quotes")
                        self.streaming_enabled = False  # Disable streaming for this session
                else:
                    logger.warning("Failed to initialize streaming service, will use REST API fallback for quotes")
                    self.streaming_enabled = False  # Disable streaming for this session
            else:
                if not TastytradeStreamer:
                    logger.warning("TastytradeStreamer module not found, streaming features will be disabled")
                else:
                    logger.warning("Streaming is disabled due to previous failures")
                
            logger.info("Tastytrade client initialization complete")
            return True
        except Exception as e:
            logger.error(f"Error initializing Tastytrade client: {str(e)}")
            return False

    async def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'streamer') and self.streamer:
            await self.streamer.cleanup()
            
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Tastytrade session closed")

    async def authenticate(self) -> bool:
        """
        Authenticate with Tastytrade API.
        
        Returns:
            True if authentication was successful, False otherwise
        """
        if not self.username or not self.password:
            logger.error("Missing Tastytrade credentials")
            return False
        try:
            payload = {
                "login": self.username,
                "password": self.password,
                "remember-me": True
            }
            logger.info(f"Attempting authentication with {self.auth_url}, payload: {payload}")
            async with self.session.post(self.auth_url, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as response:
                response_text = await response.text()
                logger.debug(f"Authentication response: Status {response.status}, Text: {response_text}")
                if response.status in [200, 201]:
                    data = await response.json()
                    self.session_token = data.get("data", {}).get("session-token")
                    if self.session_token:
                        self.session.headers.update({"Authorization": f"Bearer {self.session_token}"})
                        os.environ["tt_auth_token"] = self.session_token
                        self.authenticated = True
                        logger.info("Successfully authenticated with Tastytrade")
                        return True
                    else:
                        logger.error(f"No session token in response: {response_text}")
                        return False
                else:
                    logger.error(f"Authentication failed: {response.status} - {response_text}")
                    return False
        except asyncio.TimeoutError:
            logger.error(f"Authentication timed out connecting to {self.auth_url} - Check network to 172.20.218.96:443")
            return False
        except Exception as e:
            logger.error(f"Authentication error: {str(e)} - Verify network or proxy settings")
            return False

    async def _get_accounts(self) -> List[Dict[str, Any]]:
        """
        Get account information from Tastytrade API.
        
        Returns:
            List of account data dictionaries
        """
        if not self.authenticated:
            logger.error("Not authenticated. Cannot get account information.")
            return []
        try:
            await self._wait_for_rate_limit()
            async with self.session.get(self.accounts_url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("data", {}).get("items", [])
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get accounts: {response.status} - {error_text}")
                    return []
        except Exception as e:
            logger.error(f"Error retrieving accounts: {e}")
            return []

    async def get_account_value(self) -> float:
        """
        Get current account value from Tastytrade API.
        
        Returns:
            Account equity value as a float
        """
        if not self.authenticated or not self.account_id:
            logger.error("Not authenticated or no account ID.")
            return 0.0
        try:
            await self._wait_for_rate_limit()
            balances_url = f"{self.base_url}/accounts/{self.account_id}/balances"
            async with self.session.get(balances_url) as response:
                if response.status == 200:
                    data = await response.json()
                    account_value = data.get("data", {}).get("equity", 0)
                    return float(account_value)
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get account value: {response.status} - {error_text}")
                    return 0.0
        except Exception as e:
            logger.error(f"Error retrieving account value: {e}")
            return 0.0

    async def get_option_chain(self, symbol: str, expiration: str = None) -> Dict[str, Any]:
        """
        Retrieve option chain for a given symbol.
        
        Args:
            symbol: Stock ticker symbol
            expiration: Optional specific expiration date (YYYY-MM-DD)
            
        Returns:
            Dictionary with option chain data
        """
        if not self.authenticated:
            logger.error("Not authenticated. Cannot get option chain.")
            return {}
        
        try:
            await self._wait_for_rate_limit()
            
            # Construct URL with optional expiration filter
            base_url = f"{self.base_url}/option-chains/{symbol}/nested"
            
            async with self.session.get(base_url) as response:
                if response.status == 200:
                    data = await response.json()
                    result = {'symbol': symbol, 'contracts': []}
                    
                    # Filter by expiration if specified
                    for exp_group in data.get('data', {}).get('items', []):
                        exp_date = exp_group.get('expiration-date')
                        
                        # Skip if specific expiration is requested and doesn't match
                        if expiration and exp_date != expiration:
                            continue
                        
                        for option_type in ['call', 'put']:
                            options = exp_group.get(f'{option_type}s', [])
                            for option in options:
                                try:
                                    contract = {
                                        'symbol': symbol,
                                        'expiration': exp_date,
                                        'strike': float(option.get('strike-price', 0)),
                                        'option_type': option_type,
                                        'bid': float(option.get('bid', 0)),
                                        'ask': float(option.get('ask', 0)),
                                        'mark_price': float(option.get('mark', 0)),
                                        'delta': float(option.get('delta', 0)),
                                        'gamma': float(option.get('gamma', 0)),
                                        'theta': float(option.get('theta', 0)),
                                        'vega': float(option.get('vega', 0)),
                                        'implied_volatility': float(option.get('implied-volatility', 0)),
                                        'volume': int(option.get('volume', 0)),
                                        'open_interest': int(option.get('open-interest', 0))
                                    }
                                    
                                    # Only add contracts with non-zero mark price and open interest
                                    if contract['mark_price'] > 0 and contract['open_interest'] > 0:
                                        result['contracts'].append(contract)
                                except (TypeError, ValueError) as e:
                                    logger.warning(f"Could not parse option contract: {e}")
                    
                    # Log if no valid contracts found
                    if not result['contracts']:
                        logger.warning(f"No valid option contracts found for {symbol}")
                    
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get option chain: {response.status} - {error_text}")
                    return {}
        except Exception as e:
            logger.error(f"Error retrieving option chain for {symbol}: {e}")
            return {}

    async def get_option_quote(self, symbol: str, expiration: str, strike: float, option_type: str) -> Optional[Dict[str, Any]]:
        """
        Get option quote for a specific option contract from Tastytrade API.
        
        Args:
            symbol: Stock ticker symbol
            expiration: Option expiration date (YYYY-MM-DD)
            strike: Option strike price
            option_type: Option type (call/put)
            
        Returns:
            Dictionary with option quote data or None
        """
        if not self.authenticated:
            logger.error("Not authenticated. Cannot get option quote.")
            raise Exception("Authentication required")
        try:
            await self._wait_for_rate_limit()
            
            # Format the expiration date correctly to YYMMDD format
            exp_date = expiration.replace("-", "")[:6]
            
            # Format the strike price correctly - multiply by 1000 and zero-pad to 8 digits
            strike_int = int(float(strike) * 1000)
            strike_padded = f"{strike_int:08d}"
            
            # Format the option type correctly
            option_char = 'C' if option_type.lower() == 'call' else 'P'
            
            # Construct the option symbol in the format: /SPY230616C00123000
            # Ensure there's a leading slash
            option_symbol = f"/{symbol.upper()}{exp_date}{option_char}{strike_padded}"
            
            # Use the correct market data URL
            url = f"{self.market_data_url}/options/{option_symbol}/quotes"
            
            logger.debug(f"Fetching quote for option symbol: {option_symbol}, URL: {url}")
            
            async with self.session.get(url) as response:
                response_text = await response.text()
                if response.status == 200:
                    data = await response.json()
                    quote = data.get("data", {}).get("items", [{}])[0]
                    return {
                        "symbol": option_symbol,
                        "underlying": symbol,
                        "expiration": expiration,
                        "strike": strike,
                        "option_type": option_type,
                        "bid": float(quote.get("bid", 0)),
                        "ask": float(quote.get("ask", 0)),
                        "mid_price": (float(quote.get("bid", 0)) + float(quote.get("ask", 0))) / 2 if quote.get("bid", 0) and quote.get("ask", 0) else 0,
                        "volume": int(quote.get("volume", 0)),
                        "open_interest": int(quote.get("open-interest", 0))
                    }
                else:
                    logger.error(f"Failed to get option quote for {symbol}: {response.status} - {response_text}")
                    # Try fallback to option chains if quote fails
                    chain_data = await self.get_option_chain(symbol)
                    if chain_data and 'contracts' in chain_data:
                        # Find the matching contract
                        for contract in chain_data['contracts']:
                            if (contract.get('expiration') == expiration and 
                                abs(contract.get('strike', 0) - strike) < 0.01 and 
                                contract.get('option_type', '').lower() == option_type.lower()):
                                logger.info(f"Found matching contract from option chain for {symbol}")
                                return contract
                    raise Exception(f"Option quote fetch failed for {symbol}: {response_text}")
        except Exception as e:
            logger.error(f"Error retrieving option quote for {symbol}: {e}")
            raise

    async def get_option_greeks(self, symbol: str, expiration: str, strike: float, option_type: str) -> Dict[str, float]:
        """
        Get option Greeks for a specific option contract from Tastytrade API.
        
        Args:
            symbol: Stock ticker symbol
            expiration: Option expiration date (YYYY-MM-DD)
            strike: Option strike price
            option_type: Option type (call/put)
            
        Returns:
            Dictionary with option Greeks
        """
        if not self.authenticated:
            logger.error("Not authenticated. Cannot get option greeks.")
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "implied_volatility": 0}
        try:
            await self._wait_for_rate_limit()
            exp_date = expiration.replace("-", "")
            strike_int = int(strike * 1000)
            strike_formatted = f"{strike_int:05d}"
            option_char = 'C' if option_type.lower() == 'call' else 'P'
            option_symbol = f"{symbol.upper()} {exp_date[2:]}{option_char}{strike_formatted}"
            
            # Construct the option symbol in the format: SPY230616C00123000
            url = f"{self.base_url}/market-metrics/options/{option_symbol}/metrics"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    metrics = data.get("data", {})
                    return {
                        "delta": float(metrics.get("delta", 0)),
                        "gamma": float(metrics.get("gamma", 0)),
                        "theta": float(metrics.get("theta", 0)),
                        "vega": float(metrics.get("vega", 0)),
                        "implied_volatility": float(metrics.get("implied-volatility", 0))
                    }
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get option greeks: {response.status} - {error_text}")
                    return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "implied_volatility": 0}
        except Exception as e:
            logger.error(f"Error retrieving option greeks for {symbol}: {e}")
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "implied_volatility": 0}

    async def get_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get stock data from Tastytrade API.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Dictionary with stock data
        """
        if not self.authenticated:
            logger.error("Not authenticated. Cannot get stock data.")
            return {"symbol": symbol, "price": 0, "change": 0, "change_percent": 0, "volume": 0, "average_volume": 0, "relative_volume": 0, "bid": 0, "ask": 0}
        try:
            await self._wait_for_rate_limit()
            url = f"{self.market_data_url}/quotes/{symbol}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    quote = data.get("data", {}).get("items", [{}])[0]
                    return {
                        "symbol": symbol,
                        "price": float(quote.get("last", 0)),
                        "change": float(quote.get("net-change", 0)),
                        "change_percent": float(quote.get("net-change-percent", 0)),
                        "volume": int(quote.get("volume", 0)),
                        "average_volume": int(quote.get("average-volume", 0) or 0),
                        "relative_volume": float(quote.get("volume", 0) / (quote.get("average-volume", 1) or 1)),
                        "bid": float(quote.get("bid", 0)),
                        "ask": float(quote.get("ask", 0))
                    }
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get stock data: {response.status} - {error_text}")
                    return {"symbol": symbol, "price": 0, "change": 0, "change_percent": 0, "volume": 0, "average_volume": 0, "relative_volume": 0, "bid": 0, "ask": 0}
        except Exception as e:
            logger.error(f"Error retrieving stock data for {symbol}: {e}")
            return {"symbol": symbol, "price": 0, "change": 0, "change_percent": 0, "volume": 0, "average_volume": 0, "relative_volume": 0, "bid": 0, "ask": 0}

    async def get_market_data(self, symbol: str) -> Dict[str, Any]:
        return await self.get_stock_data(symbol)

    async def buy_option(self, symbol: str, expiration: str, strike: float, option_type: str, 
                       quantity: int, price_limit: float) -> Dict[str, Any]:
        """
        Place a buy order for a specific option contract.
        
        Args:
            symbol: Stock ticker symbol
            expiration: Option expiration date (YYYY-MM-DD)
            strike: Option strike price
            option_type: Option type (call/put)
            quantity: Number of contracts to buy
            price_limit: Maximum price per contract
            
        Returns:
            Dictionary with order details and status
        """
        if not self.authenticated or not self.account_id:
            logger.error("Not authenticated or no account ID. Cannot place order.")
            raise Exception("Authentication or account ID required")
        try:
            await self._wait_for_rate_limit()
            
            # Format the expiration date correctly
            exp_date = expiration.replace("-", "")[:6]  # YYMMDD format
            
            # Format the strike price correctly
            strike_int = int(strike * 1000)  # Convert to cents
            strike_padded = f"{strike_int:08d}"  # 8-digit strike padding
            
            # Format the option type correctly
            option_char = 'C' if option_type.lower() == 'call' else 'P'
            
            # Construct the option symbol with leading slash
            option_symbol = f"/{symbol.upper()}{exp_date}{option_char}{strike_padded}"  # Corrected format
            
            # Validate that the option contract exists in the chain
            chain = await self.get_option_chain(symbol)
            if not chain or 'contracts' not in chain or not chain['contracts']:
                logger.warning(f"No valid contracts for {symbol}, check symbol or Tastytrade API: {chain}")
                return {
                    "status": "failed", 
                    "reason": "No valid option contracts", 
                    "symbol": symbol, 
                    "expiration": expiration, 
                    "strike": strike, 
                    "option_type": option_type, 
                    "quantity": quantity, 
                    "price": price_limit, 
                    "side": "buy", 
                    "time": datetime.now().isoformat()
                }
            
            # Prepare the order payload
            order_payload = {
                "account-id": self.account_id,
                "source": "API",
                "order-type": "Limit",
                "price": price_limit,
                "price-effect": "Debit",
                "time-in-force": "Day",
                "legs": [
                    {
                        "instrument-type": "Equity Option", 
                        "symbol": option_symbol, 
                        "quantity": quantity, 
                        "action": "Buy to Open"
                    }
                ]
            }
            
            url = f"{self.base_url}/accounts/{self.account_id}/orders"
            logger.debug(f"Submitting buy option order payload: {order_payload}")
            
            async with self.session.post(url, headers=self.session.headers, json=order_payload) as response:
                response_text = await response.text()
                logger.debug(f"Buy option response: {response_text}")
                
                if response.status in [200, 201]:
                    data = await response.json()
                    order_id = data.get("data", {}).get("order", {}).get("id")
                    return {
                        "order_id": order_id, 
                        "status": "placed", 
                        "symbol": symbol, 
                        "option_symbol": option_symbol, 
                        "expiration": expiration, 
                        "strike": strike, 
                        "option_type": option_type, 
                        "quantity": quantity, 
                        "price": price_limit, 
                        "side": "buy", 
                        "time": datetime.now().isoformat()
                    }
                else:
                    logger.error(f"Failed to place buy option order: {response.status} - {response_text}")
                    raise Exception(f"Buy option order failed: {response_text}")
        except Exception as e:
            logger.error(f"Error placing buy option order: {e}")
            raise

    async def sell_option(self, symbol: str, expiration: str, strike: float, option_type: str, 
                         quantity: int, price_limit: float) -> Dict[str, Any]:
        """
        Submit a sell order for an option contract.
        
        Args:
            symbol: Stock ticker symbol
            expiration: Option expiration date (YYYY-MM-DD)
            strike: Option strike price
            option_type: Option type (call/put)
            quantity: Number of contracts to sell
            price_limit: Minimum price willing to accept per contract
            
        Returns:
            Dictionary with order details
        """
        if not self.authenticated or not self.account_id:
            logger.error("Not authenticated or no account ID. Cannot place sell order.")
            return {"status": "failed", "reason": "Not authenticated", "symbol": symbol, "expiration": expiration, "strike": strike, "option_type": option_type, "quantity": quantity, "price": price_limit, "side": "sell", "time": datetime.now().isoformat()}
        try:
            await self._wait_for_rate_limit()
            exp_date = expiration.replace("-", "")
            strike_int = int(strike * 1000)
            strike_formatted = f"{strike_int:05d}"
            option_char = 'C' if option_type.lower() == 'call' else 'P'
            option_symbol = f"{symbol.upper()} {exp_date[2:]}{option_char}{strike_formatted}"
            
            # Prepare order payload according to API spec
            order_payload = {
                "order-type": "Limit",
                "price": price_limit,
                "price-effect": "Credit",
                "time-in-force": "Day",
                "legs": [
                    {
                        "instrument-type": "Equity Option",
                        "action": "Sell to Close",
                        "quantity": quantity,
                        "symbol": option_symbol
                    }
                ]
            }
            
            # First validate the order with dry-run
            dry_run_url = f"{self.base_url}/accounts/{self.account_id}/orders/dry-run"
            async with self.session.post(dry_run_url, json=order_payload) as response:
                if response.status != 201:
                    error_text = await response.text()
                    logger.error(f"Order validation failed: {response.status} - {error_text}")
                    return {"status": "failed", "reason": f"Validation error: {error_text}", "symbol": symbol, "expiration": expiration, "strike": strike, "option_type": option_type, "quantity": quantity, "price": price_limit, "side": "sell", "time": datetime.now().isoformat()}
                
                # Get validation response
                validation = await response.json()
                if not validation.get("data", {}).get("valid", False):
                    logger.error(f"Order validation failed: {validation}")
                    return {"status": "failed", "reason": "Order validation failed", "validation": validation, "symbol": symbol, "expiration": expiration, "strike": strike, "option_type": option_type, "quantity": quantity, "price": price_limit, "side": "sell", "time": datetime.now().isoformat()}
            
            # Place the actual order
            order_url = f"{self.base_url}/accounts/{self.account_id}/orders"
            async with self.session.post(order_url, json=order_payload) as response:
                if response.status in [200, 201]:
                    data = await response.json()
                    order_id = data.get("data", {}).get("order", {}).get("id")
                    return {
                        "order_id": order_id,
                        "status": "placed",
                        "symbol": symbol,
                        "option_symbol": option_symbol,
                        "expiration": expiration,
                        "strike": strike,
                        "option_type": option_type,
                        "quantity": quantity,
                        "price": price_limit,
                        "side": "sell",
                        "time": datetime.now().isoformat()
                    }
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to place sell order: {response.status} - {error_text}")
                    return {"status": "failed", "reason": f"API error: {error_text}", "symbol": symbol, "expiration": expiration, "strike": strike, "option_type": option_type, "quantity": quantity, "price": price_limit, "side": "sell", "time": datetime.now().isoformat()}
        except Exception as e:
            logger.error(f"Error placing sell order: {e}")
            return {"status": "failed", "reason": f"Exception: {str(e)}", "symbol": symbol, "expiration": expiration, "strike": strike, "option_type": option_type, "quantity": quantity, "price": price_limit, "side": "sell", "time": datetime.now().isoformat()}

    async def get_quote_token(self) -> Optional[Dict[str, Any]]:
        """Get a quote token for streaming market data"""
        try:
            # The correct endpoint is /sessions/quote-token
            url = f"{self.base_url}/sessions/quote-token"
            
            async with self.session.post(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("data", {})
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to get quote token: {response.status} - {response_text}")
                    return None
        except Exception as e:
            logger.error(f"Error getting quote token: {e}")
            return None

    async def _wait_for_rate_limit(self):
        """Wait if needed to respect rate limits"""
        current_time = time.time()
        if current_time - self.last_rate_limited_time < self.rate_limit_window:
            await asyncio.sleep(self.rate_limit_window - (current_time - self.last_rate_limited_time))
        self.last_rate_limited_time = time.time()