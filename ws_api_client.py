import os
import aiohttp
import asyncio
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class CircuitBreaker:
    """
    Circuit breaker implementation to track failing endpoints and prevent repeated calls
    """
    def __init__(self, failure_threshold: int = 3, reset_timeout: int = 3600):
        self.failure_threshold = failure_threshold  # Number of failures before circuit opens
        self.reset_timeout = reset_timeout  # Time in seconds before trying again
        self.failed_endpoints = {}  # Map of endpoint to (failure_count, last_failure_time)
        self.open_circuits = set()  # Set of endpoints with open circuits
        
    def record_failure(self, endpoint: str, ticker: str = None) -> bool:
        """
        Record a failure for an endpoint and return whether the circuit should be opened
        """
        key = f"{endpoint}:{ticker}" if ticker else endpoint
        current_time = time.time()
        
        # Check if we need to reset a previously failed endpoint
        if key in self.failed_endpoints:
            failure_count, last_failure_time = self.failed_endpoints[key]
            # If it's been long enough since the last failure, reset the count
            if current_time - last_failure_time > self.reset_timeout:
                failure_count = 0
            failure_count += 1
            self.failed_endpoints[key] = (failure_count, current_time)
            
            # If we've hit the threshold, open the circuit
            if failure_count >= self.failure_threshold:
                logger.warning(f"Circuit breaker opened for {key} after {failure_count} failures")
                self.open_circuits.add(key)
                return True
        else:
            # First failure for this endpoint
            self.failed_endpoints[key] = (1, current_time)
            
        return False
        
    def record_success(self, endpoint: str, ticker: str = None):
        """
        Record a successful call and reset the failure count
        """
        key = f"{endpoint}:{ticker}" if ticker else endpoint
        if key in self.failed_endpoints:
            del self.failed_endpoints[key]
        if key in self.open_circuits:
            self.open_circuits.remove(key)
            logger.info(f"Circuit breaker closed for {key} after successful call")
            
    def is_open(self, endpoint: str, ticker: str = None) -> bool:
        """
        Check if the circuit is open for this endpoint
        """
        key = f"{endpoint}:{ticker}" if ticker else endpoint
        if key in self.open_circuits:
            # Check if it's time to try again
            failure_count, last_failure_time = self.failed_endpoints.get(key, (0, 0))
            if time.time() - last_failure_time > self.reset_timeout:
                # It's been long enough, let's try again (half-open state)
                self.open_circuits.remove(key)
                logger.info(f"Circuit breaker half-open for {key}, allowing retry")
                return False
            return True
        return False

class WSUnusualWhalesClient:
    def __init__(self, notifier=None):
        self.api_key = os.getenv('UNUSUAL_WHALES_API_KEY') or os.getenv('UW_API_KEY')
        if not self.api_key:
            raise ValueError("No Unusual Whales API key found")
        self.base_url = "https://api.unusualwhales.com/api"
        self.session = None
        self.notifier = notifier
        self.rate_limit = 120  # Updated rate limit
        self.rate_window = 45  # Reduced from 60 to 45 seconds
        self.request_timestamps = []
        self.semaphore = asyncio.Semaphore(1)
        self.retry_attempts = 3
        self.disabled_endpoints = {}  # Track disabled endpoints by ticker
        self.circuit_breaker = CircuitBreaker()  # Add circuit breaker

    async def initialize(self) -> bool:
        self.session = aiohttp.ClientSession(headers={"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"})
        return True

    async def cleanup(self):
        """Clean up resources"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Unusual Whales API session closed")

    async def _wait_for_rate_limit(self):
        current_time = time.time()
        # Remove timestamps older than the rate window
        self.request_timestamps = [ts for ts in self.request_timestamps if current_time - ts < self.rate_window]
        
        # If we're approaching the rate limit (within 30 requests of the limit)
        if len(self.request_timestamps) >= self.rate_limit - 30:
            # Calculate how long to wait until the oldest request falls out of the window
            if self.request_timestamps:  # Make sure there are timestamps to check
                wait_time = self.rate_window - (current_time - self.request_timestamps[0])
                if wait_time > 0:
                    # Cap the wait time to a reasonable maximum (e.g., 30 seconds)
                    wait_time = min(wait_time, 30)
                    logger.warning(f"Rate limit approaching. Waiting {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)

    async def _make_request(self, endpoint: str, params: Dict = None, timeout: int = 10) -> Optional[Dict]:
        # Extract ticker from endpoint if present
        ticker = None
        if "/stock/" in endpoint:
            parts = endpoint.split("/")
            ticker_index = parts.index("stock") + 1
            if ticker_index < len(parts):
                ticker = parts[ticker_index]
        
        # Check if circuit is open for this endpoint
        if self.circuit_breaker.is_open(endpoint, ticker):
            logger.warning(f"Circuit open for {endpoint} with ticker {ticker}, skipping request")
            return {"data": []} if endpoint.endswith(("options", "option-contracts", "oi-per-strike", "oi-per-expiry", "greeks")) else {"data": {}}
        
        if not self.session:
            logger.error("Session not initialized")
            return None
        
        async with self.semaphore:  # Ensures only one request at a time
            for attempt in range(self.retry_attempts):
                try:
                    await self._wait_for_rate_limit()
                    
                    full_url = f"{self.base_url}{endpoint}"
                    logger.debug(f"Making request to {full_url} with params: {params}")
                    self.request_timestamps.append(time.time())
                    
                    async with self.session.get(full_url, params=params, timeout=timeout) as response:
                        if response.status == 200:
                            result = await response.json()
                            # Record success in circuit breaker
                            self.circuit_breaker.record_success(endpoint, ticker)
                            
                            # Log response structure for debugging
                            if result:
                                if isinstance(result, dict):
                                    keys = list(result.keys())
                                    has_data = 'data' in keys
                                    data_type = type(result.get('data')).__name__ if has_data else 'None'
                                    data_length = len(result.get('data', [])) if has_data and isinstance(result.get('data'), (list, dict)) else 0
                                    logger.debug(f"Response from {endpoint}: keys={keys}, has_data={has_data}, data_type={data_type}, data_length={data_length}")
                                else:
                                    logger.debug(f"Response from {endpoint}: type={type(result).__name__}")
                            
                            return result
                        elif response.status == 429:  # Rate limit exceeded
                            retry_after = min(int(response.headers.get('Retry-After', '30')), 30)
                            logger.warning(f"Rate limit exceeded for {endpoint}. Retry after {retry_after}s")
                            await asyncio.sleep(retry_after)
                        else:
                            response_text = await response.text()
                            logger.error(f"API request to {endpoint} failed with status {response.status}: {response_text}")
                            # Record failure in circuit breaker
                            self.circuit_breaker.record_failure(endpoint, ticker)
                            if attempt < self.retry_attempts - 1:
                                wait_time = 1 * (attempt + 1)
                                logger.warning(f"Retrying {endpoint} in {wait_time}s... (Attempt {attempt + 1}/{self.retry_attempts})")
                                await asyncio.sleep(wait_time)
                            else:
                                return None
                except asyncio.TimeoutError:
                    logger.error(f"Request timed out for {endpoint} with ticker {ticker}")
                    # Record timeout in circuit breaker
                    circuit_opened = self.circuit_breaker.record_failure(endpoint, ticker)
                    if circuit_opened:
                        logger.warning(f"Circuit breaker opened for {endpoint} with ticker {ticker}")
                    if attempt < self.retry_attempts - 1:
                        wait_time = 1 * (attempt + 1)
                        logger.warning(f"Retrying {endpoint} in {wait_time}s... (Attempt {attempt + 1}/{self.retry_attempts})")
                        await asyncio.sleep(wait_time)
                    else:
                        return None
                except Exception as e:
                    logger.error(f"Error making request to {endpoint} with ticker {ticker}: {str(e)}")
                    # Record general exception in circuit breaker
                    self.circuit_breaker.record_failure(endpoint, ticker)
                    if attempt < self.retry_attempts - 1:
                        wait_time = 1 * (attempt + 1)
                        logger.warning(f"Retrying {endpoint} in {wait_time}s... (Attempt {attempt + 1}/{self.retry_attempts})")
                        await asyncio.sleep(wait_time)
                    else:
                        return None
            
            logger.error(f"All retries failed for {endpoint} with ticker {ticker}")
            return None

    async def get_flow_alerts(self, params: Dict = None) -> Optional[List[Dict]]:
        """Get the latest flow alerts from Unusual Whales"""
        result = await self._make_request("/option-trades/flow-alerts", params)
        if result and "data" in result:
            return result["data"]
        return None

    async def get_greek_flow(self, ticker: str, date: str = None) -> Optional[Dict]:
        """Get Greek flow data for a ticker"""
        params = {"date": date} if date else None
        return await self._make_request(f"/stock/{ticker}/greek-flow", params)

    async def get_greek_flow_expiry(self, ticker: str, expiry: str) -> Optional[Dict]:
        """Get Greek flow data for a ticker by expiry date"""
        return await self._make_request(f"/stock/{ticker}/greek-flow/{expiry}")

    async def get_options_contracts(self, ticker: str) -> Optional[Dict]:
        """Get options contracts for a ticker"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use the correct endpoint from the API spec
                result = await self._make_request(f"/stock/{ticker}/option-contracts")
                if result and 'data' in result:
                    # Parse option symbols to extract missing fields
                    if isinstance(result['data'], list):
                        for contract in result['data']:
                            if 'option_symbol' in contract:
                                parsed_data = self._parse_option_symbol(contract['option_symbol'])
                                if parsed_data:
                                    # Add the parsed fields to the contract data
                                    contract['strike'] = parsed_data['strike']
                                    contract['expiration'] = parsed_data['expiration']
                                    contract['option_type'] = parsed_data['option_type']
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty options data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get options data for {ticker} after {max_retries} attempts")
                    return {"data": []}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting options data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get options data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": []}
        return {"data": []}
    
    def _parse_option_symbol(self, option_symbol: str) -> Optional[Dict]:
        """
        Parse an option symbol like 'AAPL250228C00240000' to extract:
        - ticker (AAPL)
        - expiration (2025-02-28)
        - option_type (call/put)
        - strike (240.00)
        """
        if not option_symbol or not isinstance(option_symbol, str):
            return None
        
        # Common format: {TICKER}{YY}{MM}{DD}{C/P}{STRIKE}
        pattern = r'^([A-Z]+)(\d{2})(\d{2})(\d{2})([CP])(\d+)$'
        match = re.match(pattern, option_symbol)
        
        if not match:
            return None
        
        ticker, yy, mm, dd, option_type, strike_str = match.groups()
        
        # Convert option type
        option_type = 'call' if option_type == 'C' else 'put'
        
        # Convert strike price (remove trailing zeros)
        strike = float(strike_str) / 1000
        
        # Format expiration date
        year = int('20' + yy)
        month = int(mm)
        day = int(dd)
        expiration = f"{year}-{month:02d}-{day:02d}"
        
        return {
            'ticker': ticker,
            'expiration': expiration,
            'option_type': option_type,
            'strike': strike
        }

    async def get_dark_pool_activity(self, ticker: str) -> Optional[Dict]:
        """Get dark pool activity for a ticker"""
        # Check if this endpoint is disabled for this ticker
        if ticker in self.disabled_endpoints.get("dark_pool", []):
            logger.warning(f"Dark pool endpoint disabled due to timeouts for {ticker}")
            return {"data": []}
            
        try:
            result = await self._make_request(f"/darkpool/{ticker}", timeout=5)
            if result:
                return result
            # If request failed, add ticker to disabled endpoints
            if "dark_pool" not in self.disabled_endpoints:
                self.disabled_endpoints["dark_pool"] = []
            self.disabled_endpoints["dark_pool"].append(ticker)
            logger.warning(f"Dark pool endpoint disabled due to timeouts for {ticker}")
            return {"data": []}
        except Exception as e:
            logger.error(f"Error fetching dark pool data for {ticker}: {str(e)}")
            return {"data": []}

    async def get_net_premium_ticks(self, ticker: str) -> Optional[Dict]:
        """Get net premium ticks for a ticker"""
        return await self._make_request(f"/stock/{ticker}/net-prem-ticks")

    async def get_volume_oi_by_expiry(self, ticker: str) -> Optional[Dict]:
        """Get volume and open interest by expiry for a ticker"""
        return await self._make_request(f"/stock/{ticker}/oi-per-expiry")

    async def get_realized_volatility(self, ticker: str) -> Optional[Dict]:
        """Get realized volatility for a ticker"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await self._make_request(f"/stock/{ticker}/volatility/realized")
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty realized volatility data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get realized volatility data for {ticker} after {max_retries} attempts")
                    return {"data": []}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting realized volatility data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get realized volatility data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": []}
        return {"data": []}

    async def get_implied_volatility(self, ticker: str) -> Optional[Dict]:
        """
        Get implied volatility for a ticker using the term-structure endpoint
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing implied volatility data or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use the correct endpoint from the API spec
                result = await self._make_request(f"/stock/{ticker}/volatility/term-structure")
                if result and 'data' in result:
                    # Extract the implied volatility from the term structure data
                    # The term structure endpoint returns data in a different format than the old implied endpoint
                    # We'll transform it to match the expected format for backward compatibility
                    if isinstance(result['data'], list) and result['data']:
                        # Find the 30-day implied volatility if available
                        thirty_day_iv = None
                        for item in result['data']:
                            if isinstance(item, dict):
                                # Try to find a term close to 30 days
                                if 'term' in item and 'iv' in item:
                                    term = item.get('term', 0)
                                    if 25 <= term <= 35:
                                        thirty_day_iv = item.get('iv', 0)
                                        break
                        
                        # If no 30-day IV found, use the first available
                        if thirty_day_iv is None and result['data'][0] and isinstance(result['data'][0], dict):
                            thirty_day_iv = result['data'][0].get('iv', 0)
                        
                        # Create a compatible format with the old endpoint
                        transformed_data = {
                            'data': {
                                'implied_volatility': thirty_day_iv if thirty_day_iv is not None else 0,
                                'value': thirty_day_iv if thirty_day_iv is not None else 0,
                                'term_structure': result['data']
                            }
                        }
                        return transformed_data
                    elif isinstance(result['data'], dict):
                        # If it's already in the expected format, just ensure it has the value field
                        if 'implied_volatility' in result['data'] and 'value' not in result['data']:
                            result['data']['value'] = result['data']['implied_volatility']
                        return result
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty implied volatility data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get implied volatility data for {ticker} after {max_retries} attempts")
                    return {"data": {"implied_volatility": 0, "value": 0, "term_structure": []}}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting implied volatility data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get implied volatility data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": {"implied_volatility": 0, "value": 0, "term_structure": []}}
        return {"data": {"implied_volatility": 0, "value": 0, "term_structure": []}}

    async def get_stock_price(self, ticker: str) -> Optional[float]:
        """
        Get the current stock price for a ticker using the OHLC endpoint
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Current stock price as a float or None if not available
        """
        try:
            # Use the 1d candle size to get the most recent price
            result = await self._make_request(f"/stock/{ticker}/ohlc/1d", {"limit": 1})
            if result and "data" in result and len(result["data"]) > 0:
                # Return the most recent close price
                return float(result["data"][0].get("close", 0))
            
            # Fallback to 1h if 1d doesn't work
            result = await self._make_request(f"/stock/{ticker}/ohlc/1h", {"limit": 1})
            if result and "data" in result and len(result["data"]) > 0:
                return float(result["data"][0].get("close", 0))
                
            logger.warning(f"Could not retrieve stock price for {ticker}")
            return None
        except Exception as e:
            logger.error(f"Error fetching stock price for {ticker}: {str(e)}")
            return None
            
    async def get_stock_state(self, ticker: str) -> Optional[Dict]:
        """
        Get the current stock state including price, volume, and other metrics
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing stock state data or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await self._make_request(f"/stock/{ticker}/stock-state")
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty stock state data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get stock state data for {ticker} after {max_retries} attempts")
                    return {"data": {}}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting stock state data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get stock state data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": {}}
        return {"data": {}}
        
    async def get_max_pain(self, ticker: str, expiry: str = None) -> Optional[Dict]:
        """
        Get max pain data for options
        
        Args:
            ticker: Stock ticker symbol
            expiry: Optional expiry date in YYYY-MM-DD format
            
        Returns:
            Dictionary containing max pain data or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                params = {"expiry": expiry} if expiry else None
                result = await self._make_request(f"/stock/{ticker}/max-pain", params)
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty max pain data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get max pain data for {ticker} after {max_retries} attempts")
                    return {"data": {}}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting max pain data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get max pain data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": {}}
        return {"data": {}}

    async def get_oi_per_strike(self, ticker: str) -> Optional[Dict]:
        """
        Get open interest data per strike price
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing open interest data by strike or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await self._make_request(f"/stock/{ticker}/oi-per-strike")
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty OI per strike data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get OI per strike data for {ticker} after {max_retries} attempts")
                    return {"data": []}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting OI per strike data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get OI per strike data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": []}
        return {"data": []}
    
    async def get_options_volume(self, ticker: str) -> Optional[Dict]:
        """
        Get options volume data
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing options volume data or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await self._make_request(f"/stock/{ticker}/options-volume")
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty options volume data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get options volume data for {ticker} after {max_retries} attempts")
                    return {"data": {}}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting options volume data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get options volume data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": {}}
        return {"data": {}}

    async def get_spot_exposures_expiry_strike(self, ticker: str, expiry: str, calculation_type: str = "oi") -> Optional[Dict]:
        """
        Get spot exposures data by expiry and strike
        
        Args:
            ticker: Stock ticker symbol
            expiry: Option expiry date in YYYY-MM-DD format
            calculation_type: Type of calculation, either 'oi' (open interest) or 'volume'
            
        Returns:
            Dictionary containing spot exposures data or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                params = {"expiry": expiry, "calculation_type": calculation_type}
                result = await self._make_request(f"/stock/{ticker}/spot-exposures/expiry-strike", params)
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty spot exposures data for {ticker}/{expiry}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get spot exposures data for {ticker}/{expiry} after {max_retries} attempts")
                    return {"data": []}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting spot exposures data for {ticker}/{expiry}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get spot exposures data for {ticker}/{expiry} after {max_retries} attempts: {str(e)}")
                    return {"data": []}
        return {"data": []}
    
    async def get_oi_per_expiry(self, ticker: str) -> Optional[Dict]:
        """
        Get open interest data per expiry date
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing open interest data by expiry or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await self._make_request(f"/stock/{ticker}/oi-per-expiry")
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty OI per expiry data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get OI per expiry data for {ticker} after {max_retries} attempts")
                    return {"data": []}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting OI per expiry data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get OI per expiry data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": []}
        return {"data": []}

    async def get_greeks(self, ticker: str, expiry: str = None) -> Optional[Dict]:
        """
        Get options Greeks data for a ticker
        
        Args:
            ticker: Stock ticker symbol
            expiry: Optional expiry date in YYYY-MM-DD format to filter by
            
        Returns:
            Dictionary containing Greeks data or None if not available
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                params = {"expiry": expiry} if expiry else None
                result = await self._make_request(f"/stock/{ticker}/greeks", params)
                if result and 'data' in result:
                    return result
                
                # If we got a result but it's empty or missing data, log and retry
                if attempt < max_retries - 1:
                    logger.warning(f"Empty Greeks data for {ticker}, retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.warning(f"Could not get Greeks data for {ticker} after {max_retries} attempts")
                    return {"data": []}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting Greeks data for {ticker}, retrying ({attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Failed to get Greeks data for {ticker} after {max_retries} attempts: {str(e)}")
                    return {"data": []}
        return {"data": []}

    async def get_stock_market_data(self, ticker: str) -> Dict:
        """
        Retrieve comprehensive market data for a stock
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Dictionary containing market data, including:
            - price data (OHLC)
            - options data
            - stock state
        """
        try:
            price_data = await self.get_stock_price(ticker)
            stock_state = await self.get_stock_state(ticker)
            options_data = await self.get_options_contracts(ticker)
            
            # Combine all data into a single market data object
            market_data = {
                "ticker": ticker,
                "price_data": price_data,
                "stock_state": stock_state,
                "options_data": options_data
            }
            
            # Optional additional data if available
            try:
                market_data["volume_oi"] = await self.get_volume_oi_by_expiry(ticker)
            except Exception:
                pass
                
            try:
                market_data["max_pain"] = await self.get_max_pain(ticker)
            except Exception:
                pass
                
            return market_data
        except Exception as e:
            logger.error(f"Error getting market data for {ticker}: {str(e)}")
            # Return minimal data to prevent complete failure
            return {
                "ticker": ticker,
                "error": str(e),
                "price_data": {},
                "stock_state": {},
                "options_data": {}
            }