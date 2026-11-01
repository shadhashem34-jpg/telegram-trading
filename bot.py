import asyncio
import signal
import logging
import os
import sys

# ── FIX: Add src to path ──
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, "src"))

from dotenv import load_dotenv
from telegram_handler import TelegramBot
from strategy import StrategyManager
from data_handler import MarketDataHandler
from reporting import ReportingSystem
from logger import setup_logging

load_dotenv()
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.running = False
        self.market_data = None
        self.strategy_manager = None
        self.telegram_bot = None
        self.reporting = None

    async def initialize(self):
        self.market_data = MarketDataHandler()
        self.strategy_manager = StrategyManager()
        
        # Reporting System ممکنه ورودی بخواد یا نخواد، هندلش میکنیم
        try:
            self.reporting = ReportingSystem()
        except:
            self.reporting = None
            
        self.telegram_bot = TelegramBot(self.market_data, self.strategy_manager, self.reporting)
        
        # 🚀 اتصال استراتژی به تلگرام و بازار
        self.strategy_manager.telegram_bot = self.telegram_bot
        self.strategy_manager.market_data = self.market_data
            
        logger.info("✅ All components connected successfully")

    async def run_components(self):
        # فقط کامپوننت‌هایی که متد start دارن اجرا میشن (Reporting رو حذف کردیم چون start نداره)
        tasks = [
            asyncio.create_task(self.telegram_bot.start(), name="telegram"),
            asyncio.create_task(self.strategy_manager.start_monitoring(), name="strategy"),
            asyncio.create_task(self.market_data.start_data_collection(), name="data"),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in done:
            exc = task.exception()
            if exc:
                logger.error(f"❌ Task '{task.get_name()}' crashed: {exc}", exc_info=exc)

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def start(self):
        self.running = True
        try:
            await self.initialize()
            logger.info("🚀 Trading bot started")
            await self.run_components()
        finally:
            self.running = False
            await self.stop()

    async def stop(self):
        logger.info("🛑 Shutting down...")
        # فقط کامپوننت‌های فعال رو خاموش میکنیم
        for name, component in [("telegram", self.telegram_bot), ("strategy", self.strategy_manager), ("data", self.market_data)]:
            if component is None: continue
            try:
                stop_fn = getattr(component, "stop", None) or getattr(component, "close", None)
                if stop_fn:
                    result = stop_fn()
                    if asyncio.iscoroutine(result): 
                        await result
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")

async def main():
    setup_logging()
    
    bot = TradingBot()
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: bot.shutdown_event.set())
    except: 
        pass

    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())
