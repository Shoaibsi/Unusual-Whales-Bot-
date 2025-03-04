import asyncio
import datetime
import json
import logging
from typing import Callable, Dict, List, Optional, Any

import websockets

logger = logging.getLogger(__name__)

class TastytradeStreamer:
    """Client for Tastytrade's DXLink streaming service"""
    
    def __init__(self, tastytrade_client):
        """
        Initialize the Tastytrade streamer.
        
        Args:
            tastytrade_client: Authenticated TastytradeClient instance
        """
        self.tastytrade_client = tastytrade_client
        self.token = None
        self.dxlink_url = None
        self.websocket = None
        self.connected = False
        self.channel_id = 1  # Channel for market data
        self.subscriptions = {}  # Track subscriptions
        logger.info("Tastytrade streamer initialized")
        
    async def initialize(self) -> bool:
        """
        Initialize the streamer and get required tokens.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            logger.info("Initializing Tastytrade streamer...")
            
            # Get the quote token
            token_data = await self.tastytrade_client.get_quote_token()
            if not token_data:
                logger.error("Failed to get quote token")
                return False
                
            self.token = token_data.get("token")
            # Use the websocket-url from the response
            self.dxlink_url = token_data.get("websocket-url") or token_data.get("dxlink-url")
            
            if not self.token or not self.dxlink_url:
                logger.error(f"Invalid response format from quote-token endpoint: {token_data}")
                return False
                
            logger.info(f"Got quote token: {self.token[:10]}... expires at: {token_data.get('expires-at')}")
            logger.debug(f"DXLink WebSocket URL: {self.dxlink_url}")
            return True
        except Exception as e:
            logger.error(f"Error initializing Tastytrade streamer: {e}")
            return False
    
    async def connect(self) -> bool:
        """
        Connect to the DXLink WebSocket.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        if not self.token or not self.dxlink_url:
            logger.error("Token or URL not set. Call initialize() first.")
            return False
            
        try:
            logger.info(f"Connecting to DXLink WebSocket: {self.dxlink_url}")
            
            # Connect to the WebSocket
            self.websocket = await websockets.connect(self.dxlink_url)
            logger.info("Successfully connected to WebSocket")
            
            # Send SETUP message
            setup_message = {
                "type": "SETUP",
                "channel": 0,
                "version": "0.1-DXF-JS/0.3.0",
                "keepaliveTimeout": 60,
                "acceptKeepaliveTimeout": 60
            }
            await self.websocket.send(json.dumps(setup_message))
            
            # Wait for SETUP response
            response = json.loads(await self.websocket.recv())
            if response.get("type") != "SETUP":
                logger.error(f"Unexpected response to SETUP: {response}")
                await self.websocket.close()
                return False
                
            # Wait for AUTH_STATE message
            response = json.loads(await self.websocket.recv())
            if response.get("type") != "AUTH_STATE" or response.get("state") != "UNAUTHORIZED":
                logger.error(f"Unexpected AUTH_STATE: {response}")
                await self.websocket.close()
                return False
                
            # Send AUTH message
            auth_message = {
                "type": "AUTH",
                "channel": 0,
                "token": self.token
            }
            await self.websocket.send(json.dumps(auth_message))
            
            # Wait for AUTH_STATE response
            response = json.loads(await self.websocket.recv())
            if response.get("type") != "AUTH_STATE" or response.get("state") != "AUTHORIZED":
                logger.error(f"Authorization failed: {response}")
                await self.websocket.close()
                return False
                
            # Request a channel for market data
            channel_request = {
                "type": "CHANNEL_REQUEST",
                "channel": self.channel_id,
                "service": "FEED",
                "parameters": {"contract": "AUTO"}
            }
            await self.websocket.send(json.dumps(channel_request))
            
            # Wait for CHANNEL_OPENED response
            response = json.loads(await self.websocket.recv())
            if response.get("type") != "CHANNEL_OPENED" or response.get("channel") != self.channel_id:
                logger.error(f"Failed to open channel: {response}")
                await self.websocket.close()
                return False
                
            # Configure feed
            feed_setup = {
                "type": "FEED_SETUP",
                "channel": self.channel_id,
                "acceptAggregationPeriod": 0.1,
                "acceptDataFormat": "COMPACT",
                "acceptEventFields": {
                    "Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"],
                    "Greeks": ["eventType", "eventSymbol", "volatility", "delta", "gamma", "theta", "rho", "vega"]
                }
            }
            await self.websocket.send(json.dumps(feed_setup))
            
            # Wait for FEED_CONFIG response
            response = json.loads(await self.websocket.recv())
            if response.get("type") != "FEED_CONFIG":
                logger.error(f"Failed to configure feed: {response}")
                await self.websocket.close()
                return False
                
            # Start listening for incoming messages
            logger.info("Starting WebSocket listener...")
            asyncio.create_task(self._websocket_listener())
            
            # Start keepalive task
            logger.debug("Starting keepalive task...")
            asyncio.create_task(self._keepalive())
            
            self.connected = True
            logger.info("Successfully connected to DXLink WebSocket")
            return True
        except Exception as e:
            logger.error(f"Error connecting to DXLink WebSocket: {e}")
            if self.websocket:
                await self.websocket.close()
            return False
    
    async def _keepalive(self):
        """Send periodic keepalive messages to maintain the WebSocket connection"""
        try:
            while self.connected and self.websocket and not self.websocket.closed:
                # Send a keepalive message every 30 seconds
                await asyncio.sleep(30)
                if self.websocket and not self.websocket.closed:
                    keepalive_message = {
                        "type": "KEEPALIVE",
                        "channel": 0
                    }
                    await self.websocket.send(json.dumps(keepalive_message))
                    logger.debug("Sent keepalive message")
        except asyncio.CancelledError:
            logger.debug("Keepalive task cancelled")
        except Exception as e:
            logger.error(f"Error in keepalive: {e}")
            self.connected = False
            
    async def _websocket_listener(self):
        """Listen for and process streaming events"""
        try:
            while self.connected and self.websocket and not self.websocket.closed:
                try:
                    message = await self.websocket.recv()
                    await self._process_message(message)
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket connection closed")
                    self.connected = False
                    break
                except Exception as e:
                    logger.error(f"Error receiving WebSocket message: {e}")
        except asyncio.CancelledError:
            logger.debug("WebSocket listener task cancelled")
        except Exception as e:
            logger.error(f"Error in WebSocket listener: {e}")
            self.connected = False
            
    async def _process_message(self, message):
        """Process a message received from the WebSocket"""
        try:
            # Parse the message
            data = json.loads(message)
            logger.debug(f"Received message type: {data.get('type')}")
            
            if data.get("type") == "FEED_DATA":
                await self._process_feed_data(data)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse WebSocket message: {message[:100]}...")
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")
            
    async def _process_feed_data(self, data):
        """Process feed data updates"""
        try:
            channel = data.get("channel")
            if channel != self.channel_id:
                return  # Ignore data from other channels
                
            feed_data = data.get("data", [])
            if not feed_data or len(feed_data) < 2:
                return
                
            event_type = feed_data[0]
            events = feed_data[1]
            
            if event_type == "Quote":
                # Process quote events
                i = 0
                while i < len(events):
                    if events[i] == "Quote":
                        symbol = events[i+1]
                        bid_price = events[i+2]
                        ask_price = events[i+3]
                        bid_size = events[i+4]
                        ask_size = events[i+5]
                        
                        # Create quote dictionary
                        quote = {
                            "symbol": symbol,
                            "bid": bid_price,
                            "ask": ask_price,
                            "bid_size": bid_size,
                            "ask_size": ask_size,
                            "timestamp": datetime.datetime.now().isoformat()
                        }
                        
                        # Calculate mark price
                        if bid_price is not None and ask_price is not None:
                            quote["mark"] = (bid_price + ask_price) / 2
                        
                        # If this is for an option we're tracking
                        if symbol in self.subscriptions:
                            subscription = self.subscriptions[symbol]
                            subscription["last_quote"] = quote
                            
                            # Format as an option quote
                            option_quote = {
                                "symbol": subscription["symbol"],
                                "expiration": subscription["expiration"],
                                "strike": subscription["strike"],
                                "option_type": subscription["option_type"],
                                "bid": quote["bid"],
                                "ask": quote["ask"],
                                "mark": quote.get("mark"),
                                "bid_size": quote["bid_size"],
                                "ask_size": quote["ask_size"],
                                "timestamp": quote["timestamp"]
                            }
                            
                            logger.debug(f"Received quote update for {symbol}: bid={bid_price}, ask={ask_price}")
                            
                            # Call the callback if present
                            callback = subscription.get("callback")
                            if callback:
                                asyncio.create_task(callback(option_quote))
                    
                    # Move to the next event (each Quote event has 6 fields)
                    i += 6
            
            elif event_type == "Greeks":
                # Process Greeks events (similar structure)
                pass
                
        except Exception as e:
            logger.error(f"Error processing feed data: {e}")
    
    async def subscribe_to_option_quote(self, symbol: str, expiration: str, strike: float, option_type: str, callback: Optional[Callable] = None) -> str:
        """
        Subscribe to option quote updates for a specific option contract.
        
        Args:
            symbol: The ticker symbol of the underlying asset (e.g., 'SPY')
            expiration: The option expiration date in YYMMDD format (e.g., '230616')
            strike: The option strike price
            option_type: The option type ('call' or 'put')
            callback: Optional callback function to call when quotes are received
            
        Returns:
            The formatted option symbol that was subscribed to
        """
        if not self.connected or not self.websocket:
            logger.error("Cannot subscribe: WebSocket not connected")
            return None
            
        try:
            # Format the option symbol according to TastyTrade/DXFeed format
            formatted_strike = self._format_strike(strike)
            option_char = 'C' if option_type.lower() == 'call' else 'P'
            
            # Format according to the TastyTrade streamer format: .SPYYYMMDDCXXXXX
            option_symbol = f".{symbol}{expiration}{option_char}{formatted_strike}"
            
            logger.debug(f"Subscribing to option quote for {option_symbol}")
            
            # Subscribe to the quote
            subscription_message = {
                "type": "FEED_SUBSCRIPTION",
                "channel": self.channel_id,
                "add": [
                    {"type": "Quote", "symbol": option_symbol},
                    {"type": "Greeks", "symbol": option_symbol}
                ]
            }
            await self.websocket.send(json.dumps(subscription_message))
            
            logger.info(f"Subscribed to option quote for {option_symbol}")
            
            # Add to our subscriptions map with a callback
            self.subscriptions[option_symbol] = {
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "option_type": option_type,
                "last_quote": None,
                "callback": callback
            }
            
            return option_symbol
        except Exception as e:
            logger.error(f"Error subscribing to option quote: {e}")
            return None
            
    async def unsubscribe_from_option_quote(self, option_symbol: str):
        """
        Unsubscribe from an option quote.
        
        Args:
            option_symbol: The option symbol to unsubscribe from
        """
        if not self.connected or not self.websocket:
            logger.error("Cannot unsubscribe: WebSocket not connected")
            return
            
        try:
            # Unsubscribe from the option quote
            subscription_message = {
                "type": "FEED_SUBSCRIPTION",
                "channel": self.channel_id,
                "remove": [
                    {"type": "Quote", "symbol": option_symbol},
                    {"type": "Greeks", "symbol": option_symbol}
                ]
            }
            await self.websocket.send(json.dumps(subscription_message))
            
            # Remove from our subscriptions
            if option_symbol in self.subscriptions:
                del self.subscriptions[option_symbol]
                
            logger.info(f"Unsubscribed from option quote for {option_symbol}")
        except Exception as e:
            logger.error(f"Error unsubscribing from option quote: {e}")
    
    async def get_option_quote(self, symbol: str, expiration: str, strike: float, option_type: str) -> Optional[Dict[str, Any]]:
        """
        Get a single option quote (non-streaming).
        
        Args:
            symbol: The ticker symbol of the underlying asset
            expiration: The option expiration date in YYMMDD format
            strike: The option strike price
            option_type: The option type ('call' or 'put')
            
        Returns:
            The option quote or None if not available
        """
        # Format the option symbol
        formatted_strike = self._format_strike(strike)
        option_char = 'C' if option_type.lower() == 'call' else 'P'
        option_symbol = f".{symbol}{expiration}{option_char}{formatted_strike}"
        
        # Check if we already have a quote for this option
        if option_symbol in self.subscriptions:
            return self.subscriptions[option_symbol].get("last_quote")
            
        # Otherwise subscribe to get the quote
        future = asyncio.Future()
        
        async def callback(quote):
            if not future.done():
                future.set_result(quote)
        
        await self.subscribe_to_option_quote(symbol, expiration, strike, option_type, callback)
        
        try:
            # Wait for the quote with a timeout
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for quote for {option_symbol}")
            return None
        finally:
            # Unsubscribe after we get the quote
            await self.unsubscribe_from_option_quote(option_symbol)
    
    async def cleanup(self):
        """Clean up resources."""
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("WebSocket connection closed")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        self.connected = False
        logger.info("Tastytrade streamer cleaned up")
    
    def _format_strike(self, strike: float) -> str:
        """Format a strike price for a TastyTrade/DXFeed option symbol"""
        # Format strike price with 8 digits, padded with zeros
        # e.g., 123.45 -> "00012345"
        strike_int = int(strike * 100)
        return f"{strike_int:08d}"
