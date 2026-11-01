import os
import sys
import asyncio
import signal
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from telegram_handler import TelegramBot
from database import init_database
from data_handler import MarketDataHandler
from strategy import StrategyManager
from reporting import ReportingSystem
from logger import setup_logging

logger = setup_logging()

class TradingBotOrchestrator:
    def __init__(self):
        self.telegram_bot = None
        self.market_data = None
        self.strategy_manager = None
        self.reporting = None
        self.running = False
    
    async def initialize(self):
        logger.info("🚀 Initializing Telegram Trading Bot...")
        await init_database()
        self.market_data = MarketDataHandler()
        self.strategy_manager = StrategyManager()
        self.reporting = ReportingSystem()
        self.telegram_bot = TelegramBot(self.market_data, self.strategy_manager, self.reporting)
        logger.info("✅ All components initialized")
    
    async def start(self):
        await self.initialize()
        self.running = True
        logger.info("🎯 Starting Telegram Trading Bot...")
        await asyncio.gather(
            self.telegram_bot.start(),
            self.strategy_manager.start_monitoring(),
            self.reporting.start_daily_reports(),
            self.market_data.start_data_collection()
        )
    
    async def stop(self):
        self.running = False
        if self.telegram_bot: await self.telegram_bot.stop()
        if self.strategy_manager: await self.strategy_manager.stop()
        if self.reporting: await self.reporting.stop()
        if self.market_data: await self.market_data.stop()

async def main():
    while True:
        bot = TradingBotOrchestrator()
        def signal_handler(signum, frame): bot.running = False
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            await bot.start()
        except Exception as e:
            logger.error(f"❌ Bot crashed: {e}. Restarting entire system in 10 seconds...")
            await asyncio.sleep(10)
        finally:
            await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
