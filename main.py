import os
import sys
import asyncio
import signal
import logging
import argparse
import json
from dotenv import load_dotenv
from ws_trading_bot import WSTradingBot
from ws_signal_detector import SignalConfig

load_dotenv()
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

async def main():
    """Main entry point for the trading bot"""
    bot = None
    try:
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                   handlers=[logging.StreamHandler(), logging.FileHandler("trading_bot.log")])
        logger = logging.getLogger(__name__)
        asyncio.get_event_loop().set_debug(True)  # Enable debug mode for asyncio
        
        # Load environment variables
        loaded_vars = {
            'XAI_API_KEY': bool(os.getenv('XAI_API_KEY')),
            'TASTYWORKS_USERNAME': bool(os.getenv('TASTYWORKS_USERNAME')),
            'TELEGRAM_BOT_TOKEN': bool(os.getenv('TELEGRAM_BOT_TOKEN')),
            'UNUSUAL_WHALES_API_KEY': bool(os.getenv('UNUSUAL_WHALES_API_KEY'))
        }
        logger.debug(f"Loaded environment variables: {', '.join(f'{k}={v}' for k, v in loaded_vars.items())}")
        
        # Parse arguments
        parser = argparse.ArgumentParser(description="Unusual Whales Trading Bot")
        parser.add_argument("--config", default=os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "ws_bot", "config", "config.json"), help="Config file path")
        parser.add_argument("--timeout", type=int, default=0, help="Timeout in seconds")
        parser.add_argument("--scan-interval", type=int, default=120, help="Scan interval in seconds")
        parser.add_argument("--debug", action="store_true", help="Enable debug logging")
        args = parser.parse_args()
        
        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
        
        # Initialize bot
        logger.info("Attempting to initialize bot...")
        with open(args.config, 'r') as f:
            config = json.load(f)
        signal_config = SignalConfig(**config.get("signal_config", {}))
        bot = WSTradingBot(args.config, signal_config)
        if not await bot.initialize():
            logger.error("Failed to initialize bot")
            return
        logger.info("Bot initialized successfully")
        
        # Set up signal handlers for graceful shutdown
        if os.name != 'nt':  # Not on Windows
            loop = asyncio.get_event_loop()
            signals = (signal.SIGTERM, signal.SIGINT)
            for s in signals:
                loop.add_signal_handler(
                    s, lambda s=s: asyncio.create_task(shutdown(s, loop, bot))
                )
        else:
            # On Windows, we'll handle keyboard interrupt in the main try-except
            signal.signal(signal.SIGINT, lambda sig, frame: asyncio.create_task(shutdown(sig, asyncio.get_event_loop(), bot)))
            signal.signal(signal.SIGTERM, lambda sig, frame: asyncio.create_task(shutdown(sig, asyncio.get_event_loop(), bot)))
        
        try:
            # Start the bot
            bot_task = asyncio.create_task(bot.run())
            if args.timeout > 0:
                logger.info(f"Running for {args.timeout} seconds")
                await asyncio.sleep(args.timeout)
            else:
                await asyncio.gather(bot_task)
        except Exception as e:
            logger.error(f"Error running bot: {e}")
            raise
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        # Clean up resources
        if bot:
            await bot.cleanup()
        logger.info("Bot stopped.")

async def shutdown(sig: signal.Signals, loop: asyncio.AbstractEventLoop, bot: WSTradingBot):
    """Handle graceful shutdown on signal"""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {sig.name}, shutting down...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    try:
        await bot.cleanup()
    except Exception as e:
        logger.error(f"Error during shutdown cleanup: {e}")
        
    logger.info("Stopping bot...")
    loop.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger = logging.getLogger(__name__)
        logger.info("Keyboard interrupt, exiting...")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Unhandled exception: {str(e)}")
        logger.exception("Traceback:")