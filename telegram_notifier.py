import os
import aiohttp
import asyncio
import time
from typing import Dict, Optional, List, Union, BinaryIO
from loguru import logger
from datetime import datetime
from collections import deque

class TelegramNotifier:
    """Handles sending notifications to Telegram."""
    
    def __init__(self, token: str = None, chat_id: str = None):
        """Initialize the Telegram notifier."""
        self.token = token or os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = chat_id or os.getenv('TELEGRAM_CHAT_ID')
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.session = None
        
        # Rate limiting
        self.message_timestamps = deque(maxlen=20)  # Track last 20 messages
        self.rate_limit = 20  # Max 20 messages per minute (Telegram's limit)
        self.rate_window = 60  # seconds
        self.min_message_interval = 0.5  # Minimum time between messages in seconds
        self.last_message_time = 0
        
        # Command handlers
        self.command_handlers = {
            "/start": self._handle_start_command,
            "/stop": self._handle_stop_command,
            "/status": self._handle_status_command,
            "/help": self._handle_help_command
        }
        
        # Bot state
        self.is_active = True
        
    async def initialize(self) -> bool:
        """Initialize the aiohttp session."""
        if not self.token or not self.chat_id:
            logger.warning("Telegram credentials not provided, notifications will be disabled")
            return False
            
        try:
            self.session = aiohttp.ClientSession()
            # Test the connection
            test_result = await self.test_connection()
            if test_result:
                logger.info("Telegram connection established successfully")
            else:
                logger.warning("Telegram connection test failed")
            return test_result
        except aiohttp.ClientError as e:
            logger.error(f"Network error initializing Telegram: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize Telegram: {str(e)}")
            return False
        
    async def cleanup(self):
        """Clean up resources"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Telegram session closed")
            
    async def close(self):
        """Close the client session."""
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def _wait_for_rate_limit(self):
        """Wait if needed to respect rate limits."""
        current_time = time.time()
        
        # Enforce minimum time between messages
        time_since_last_message = current_time - self.last_message_time
        if time_since_last_message < self.min_message_interval:
            wait_time = self.min_message_interval - time_since_last_message
            logger.debug(f"Waiting {wait_time:.2f}s between Telegram messages")
            await asyncio.sleep(wait_time)
            
        # Check if we're approaching the rate limit
        if len(self.message_timestamps) >= self.rate_limit:
            # Get the oldest timestamp
            oldest_timestamp = self.message_timestamps[0]
            time_diff = current_time - oldest_timestamp
            
            # If the oldest message was sent less than rate_window seconds ago,
            # we need to wait until it's outside our window
            if time_diff < self.rate_window:
                wait_time = self.rate_window - time_diff
                logger.info(f"Telegram rate limit approaching. Waiting for {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time)
                
        self.last_message_time = time.time()
        self.message_timestamps.append(self.last_message_time)
            
    async def send_message(self, message: str) -> bool:
        """Send a message to the Telegram chat."""
        if not self.session:
            logger.error("Session not initialized")
            return False
            
        if not self.is_active:
            logger.info("Bot is currently inactive, message not sent")
            return False
            
        try:
            # Wait for rate limit
            await self._wait_for_rate_limit()
            
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            logger.debug(f"Sending message to Telegram: {message[:50]}...")
            async with self.session.post(url, json=data) as response:
                response_text = await response.text()
                logger.debug(f"Request to {url} returned status {response.status}: {response_text[:100]}...")
                
                if response.status != 200:
                    logger.error(f"Failed to send Telegram message: {response_text}")
                    return False
                return True
                
        except aiohttp.ClientConnectionError as e:
            logger.error(f"Connection error sending Telegram message: {str(e)}")
            return False
        except aiohttp.ClientResponseError as e:
            logger.error(f"Response error sending Telegram message: {str(e)}")
            return False
        except aiohttp.ClientError as e:
            logger.error(f"Client error sending Telegram message: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error sending Telegram message: {str(e)}")
            return False
            
    async def send_chart(self, chart_path: str, caption: str = None) -> bool:
        """
        Send a chart image to the Telegram chat.
        
        Args:
            chart_path: Path to the chart image file
            caption: Optional caption for the image
            
        Returns:
            True if image was sent successfully, False otherwise
        """
        if not self.session:
            logger.error("Session not initialized")
            return False
            
        if not self.is_active:
            logger.info("Bot is currently inactive, chart not sent")
            return False
            
        try:
            # Wait for rate limit
            await self._wait_for_rate_limit()
            
            url = f"{self.base_url}/sendPhoto"
            
            # Prepare form data
            form_data = aiohttp.FormData()
            form_data.add_field("chat_id", self.chat_id)
            
            # Add caption if provided
            if caption:
                form_data.add_field("caption", caption)
                form_data.add_field("parse_mode", "HTML")
                
            # Add the photo
            with open(chart_path, 'rb') as photo_file:
                form_data.add_field("photo", photo_file, 
                                   filename=os.path.basename(chart_path),
                                   content_type='image/png')
                
                logger.debug(f"Sending chart to Telegram: {chart_path}")
                async with self.session.post(url, data=form_data) as response:
                    response_text = await response.text()
                    logger.debug(f"Request to {url} returned status {response.status}: {response_text[:100]}...")
                    
                    if response.status != 200:
                        logger.error(f"Failed to send Telegram chart: {response_text}")
                        return False
                    return True
                    
        except aiohttp.ClientConnectionError as e:
            logger.error(f"Connection error sending Telegram chart: {str(e)}")
            return False
        except aiohttp.ClientResponseError as e:
            logger.error(f"Response error sending Telegram chart: {str(e)}")
            return False
        except aiohttp.ClientError as e:
            logger.error(f"Client error sending Telegram chart: {str(e)}")
            return False
        except FileNotFoundError as e:
            logger.error(f"Chart file not found: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error sending Telegram chart: {str(e)}")
            return False
            
    async def send_signal_alert(self, signal: Dict, grok_analysis: Dict = None) -> bool:
        """Send a formatted signal alert to Telegram."""
        if not self.session:
            if not await self.initialize():
                logger.warning("Telegram not initialized, skipping signal alert")
                return False
        
        # Format primary data for better readability
        primary_data = signal.get('primary_data', {})
        if not primary_data:
            primary_data = {}
            
        if isinstance(primary_data, dict):
            # Filter out None values and format numeric values
            formatted_items = []
            for k, v in primary_data.items():
                if v is not None:
                    # Format numeric values
                    if isinstance(v, (int, float)):
                        if k.endswith('_ratio'):
                            formatted_value = f"{v:.2f}"
                        elif k.endswith('_flow') or k.endswith('_premium') or k == 'aggregated_volume':
                            formatted_value = f"${v:,.2f}" if abs(v) >= 1 else f"${v:.2f}"
                        else:
                            formatted_value = f"{v:,}" if abs(v) >= 1000 else f"{v}"
                    else:
                        formatted_value = str(v)
                    
                    # Add formatted item
                    formatted_items.append(f"{k.replace('_', ' ').title()}: {formatted_value}")
            
            # If no items after filtering, add a default message
            if not formatted_items:
                primary_data_str = "No detailed metrics available"
            else:
                primary_data_str = "\n".join(formatted_items)
        else:
            primary_data_str = str(primary_data) if primary_data else "No detailed metrics available"
        
        # Extract option recommendation if available
        option_recommendation = ""
        if 'options_data' in signal and signal['options_data']:
            options_data = signal['options_data']
            if isinstance(options_data, list) and len(options_data) > 0:
                # Handle list of options directly
                option_recommendation = "\n\n<b>Recommended Options:</b>\n"
                for i, option in enumerate(options_data[:3]):  # Show top 3 recommendations
                    if not isinstance(option, dict):
                        continue
                        
                    strike = option.get('strike', 'N/A')
                    expiry = option.get('expiry', 'N/A')
                    option_type = option.get('type', 'call').upper()
                    
                    # Handle volume formatting
                    volume = option.get('volume', 'N/A')
                    if isinstance(volume, (int, float)) and volume > 0:
                        volume = f"{volume:,}"
                    
                    # Handle open interest formatting
                    open_interest = option.get('open_interest', 'N/A')
                    if isinstance(open_interest, (int, float)) and open_interest > 0:
                        open_interest = f"{open_interest:,}"
                    
                    # Handle implied volatility formatting
                    implied_vol = option.get('implied_volatility', 'N/A')
                    if isinstance(implied_vol, (int, float)) and implied_vol > 0:
                        implied_vol = f"{implied_vol:.1%}"
                    
                    option_recommendation += f"• {option_type} ${strike} exp {expiry}\n  Vol: {volume} | OI: {open_interest} | IV: {implied_vol}\n"
                    
                    # Add a separator between options except for the last one
                    if i < min(2, len(options_data) - 1):
                        option_recommendation += "  ---------------\n"
            elif isinstance(options_data, dict):
                # Handle dictionary with recommended_options key
                recommended_options = options_data.get('recommended_options', [])
                if recommended_options and isinstance(recommended_options, list) and len(recommended_options) > 0:
                    option_recommendation = "\n\n<b>Recommended Options:</b>\n"
                    for i, option in enumerate(recommended_options[:3]):  # Show top 3 recommendations
                        if not isinstance(option, dict):
                            continue
                            
                        strike = option.get('strike', 'N/A')
                        expiry = option.get('expiry', 'N/A')
                        option_type = option.get('option_type', 'call').upper()
                        
                        # Format confidence
                        confidence = option.get('confidence', 0)
                        if isinstance(confidence, (int, float)):
                            confidence_str = f"{confidence:.1%}"
                        else:
                            confidence_str = "N/A"
                            
                        option_recommendation += f"• {option_type} ${strike} exp {expiry} (Confidence: {confidence_str})\n"
                        
                        # Add a separator between options except for the last one
                        if i < min(2, len(recommended_options) - 1):
                            option_recommendation += "  ---------------\n"
        
        # Format confidence and risk score
        confidence = signal.get('confidence', 0)
        if not isinstance(confidence, (int, float)):
            confidence = 0
            
        risk_score = signal.get('risk_score', 0)
        if not isinstance(risk_score, (int, float)):
            risk_score = 0
            
        # Get direction and format it
        direction = signal.get('suggested_direction', 'UNKNOWN')
        if direction == "LONG":
            direction_emoji = "🟢"
        elif direction == "SHORT":
            direction_emoji = "🔴"
        else:
            direction_emoji = "⚪"
            
        # Format the signal type
        signal_type = signal.get('signal_type', 'UNKNOWN')
        signal_type_formatted = signal_type.replace('_', ' ').title()
        
        message = (
            f"🚨 <b>New Trading Signal Detected!</b>\n\n"
            f"Ticker: <code>{signal.get('ticker', 'UNKNOWN')}</code> {direction_emoji}\n"
            f"Type: {signal_type_formatted}\n"
            f"Direction: {direction}\n"
            f"Confidence: {confidence:.2%}\n"
            f"Risk Score: {risk_score:.2%}\n\n"
            f"<b>Key Metrics:</b>\n{primary_data_str}"
        )
        
        # Add option recommendation if available
        if option_recommendation:
            message += option_recommendation
        
        # Add Grok Analysis if available
        if grok_analysis:
            # Get the analysis data directly from the grok_analysis dictionary
            sentiment = grok_analysis.get('sentiment', 'N/A')
            if sentiment:
                sentiment = sentiment.title()
            
            # Format key factors, risks, and entry points
            key_factors = grok_analysis.get('key_factors', [])
            if isinstance(key_factors, list) and key_factors:
                key_factors_str = "\n• " + "\n• ".join(key_factors)
            else:
                key_factors_str = "\nNone available"
                
            risks = grok_analysis.get('risks', [])
            if isinstance(risks, list) and risks:
                risks_str = "\n• " + "\n• ".join(risks)
            else:
                risks_str = "\nNone available"
                
            entry_points = grok_analysis.get('entry_points', [])
            if isinstance(entry_points, list) and entry_points:
                entry_points_str = "\n• " + "\n• ".join(entry_points)
            else:
                entry_points_str = "\nNone available"
            
            # Extract option recommendations from Grok if available
            option_recommendations = grok_analysis.get('option_recommendations', [])
            if isinstance(option_recommendations, list) and option_recommendations:
                grok_options_str = "\n• " + "\n• ".join(option_recommendations)
                option_rec_section = f"\n\n<b>Grok Option Recommendations:</b>{grok_options_str}\n"
            else:
                option_rec_section = ""
            
            # Get historical context
            historical_context = grok_analysis.get('historical_context', 'N/A')
            if not historical_context or historical_context == 'N/A':
                historical_context = "No historical context available"
            
            message += (
                f"\n\n<b>🤖 Grok AI Analysis:</b>\n"
                f"Sentiment: {sentiment}\n\n"
                f"<b>Key Factors:</b>{key_factors_str}\n\n"
                f"<b>Risks:</b>{risks_str}\n\n"
                f"<b>Entry Points:</b>{entry_points_str}"
                f"{option_rec_section}"
                f"\n<b>Historical Context:</b>\n{historical_context}\n\n"
            )
        
        # Add timestamp
        timestamp = signal.get('timestamp')
        if not timestamp:
            timestamp = datetime.now().isoformat()
            
        message += f"<i>Generated at: {timestamp}</i>"
        
        return await self.send_message(message)
        
    async def send_system_message(self, message: str, is_error: bool = False) -> bool:
        """
        Send a system message to the Telegram chat.
        
        Args:
            message: The message to send
            is_error: Whether this is an error message (will be formatted differently)
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.session:
            if not await self.initialize():
                logger.warning("Telegram not initialized, skipping system message")
                return False
                
        try:
            # Format the message based on type
            if is_error:
                formatted_message = (
                    f"🚨 <b>SYSTEM ERROR</b>\n"
                    f"<pre>{message}</pre>\n"
                    f"<i>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
                )
            else:
                formatted_message = (
                    f"📢 <b>SYSTEM MESSAGE</b>\n"
                    f"{message}\n"
                    f"<i>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
                )
            
            # Send message
            return await self.send_message(formatted_message)
        
        except aiohttp.ClientError as e:
            logger.error(f"Network error sending system message: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Failed to send system message: {str(e)}")
            return False
            
    async def send_error_alert(self, error_message: str, component: str, severity: str = "high") -> bool:
        """
        Send an error alert to the Telegram chat.
        
        Args:
            error_message: The error message
            component: The component that generated the error
            severity: Error severity (low, medium, high, critical)
            
        Returns:
            True if alert was sent successfully, False otherwise
        """
        if not self.session:
            if not await self.initialize():
                logger.warning("Telegram not initialized, skipping error alert")
                return False
                
        try:
            # Determine emoji based on severity
            severity_emoji = {
                "low": "⚠️",
                "medium": "🟠",
                "high": "🔴",
                "critical": "💥"
            }.get(severity.lower(), "⚠️")
            
            # Format the error message
            formatted_message = (
                f"{severity_emoji} <b>ERROR ALERT - {severity.upper()}</b> {severity_emoji}\n\n"
                f"<b>Component:</b> {component}\n"
                f"<b>Error:</b> <pre>{error_message}</pre>\n\n"
                f"<i>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
            )
            
            # Send message
            return await self.send_message(formatted_message)
            
        except aiohttp.ClientError as e:
            logger.error(f"Network error sending error alert: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Failed to send error alert: {str(e)}")
            return False

    async def test_connection(self) -> bool:
        """
        Test Telegram bot connection by sending a test message
        
        :return: True if message sent successfully, False otherwise
        """
        test_message = (
            "🤖 <b>Unusual Whales Trading Bot</b>\n\n"
            "Telegram bot connection test successful! 🎉\n"
            "Bot is ready to send trading alerts."
        )
        
        logger.info("Attempting to send Telegram connection test message")
        
        try:
            result = await self.send_message(test_message)
            logger.info(f"Telegram connection test {'PASSED' if result else 'FAILED'}")
            return result
        except Exception as e:
            logger.error(f"Telegram connection test failed: {str(e)}")
            return False

    async def _handle_start_command(self, message: str) -> bool:
        """Handle the /start command."""
        response = "Welcome to the Unusual Whales Trading Bot! 🤖\n\n"
        response += "This bot will send you trading alerts and updates."
        return await self.send_message(response)

    async def _handle_stop_command(self, message: str) -> bool:
        """Handle the /stop command."""
        self.is_active = False
        response = "Bot stopped. No further messages will be sent."
        return await self.send_message(response)

    async def _handle_status_command(self, message: str) -> bool:
        """Handle the /status command."""
        response = "Bot is currently " + ("active" if self.is_active else "inactive")
        return await self.send_message(response)

    async def _handle_help_command(self, message: str) -> bool:
        """Handle the /help command."""
        response = "Available commands:\n"
        response += "/start - Start the bot\n"
        response += "/stop - Stop the bot\n"
        response += "/status - Check the bot's status\n"
        response += "/help - Show this help message"
        return await self.send_message(response)

    async def process_incoming_message(self, message: Dict) -> bool:
        """
        Process an incoming message from Telegram.
        
        Args:
            message: The message data from Telegram webhook
            
        Returns:
            True if message was processed successfully, False otherwise
        """
        try:
            # Extract message text
            if 'message' not in message or 'text' not in message['message']:
                return False
                
            text = message['message']['text']
            chat_id = message['message']['chat']['id']
            
            # Only process messages from authorized chat_id
            if str(chat_id) != self.chat_id:
                logger.warning(f"Received message from unauthorized chat_id: {chat_id}")
                return False
                
            # Check if it's a command
            if text.startswith('/'):
                command = text.split(' ')[0].lower()  # Extract the command part
                
                if command in self.command_handlers:
                    logger.info(f"Processing command: {command}")
                    return await self.command_handlers[command](text)
                else:
                    await self.send_message(f"Unknown command: {command}\nType /help for available commands")
                    return True
                    
            # Handle regular messages if needed
            return True
            
        except Exception as e:
            logger.error(f"Error processing incoming message: {str(e)}")
            return False
