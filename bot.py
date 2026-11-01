"""
bot.py - Main orchestrator for the trading bot.
"""
import asyncio
import signal
import logging
import os
import sys
from datetime import datetime

# ── FIX: Add paths for Deplexo ──
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, "src"))

from dotenv import load_dotenv

# ── FIX: Safe imports ──
try:
    from src.telegram_handler import TelegramBot
except ImportError:
    from telegram_handler import TelegramBot

try:
    from src.strategy import StrategyManager
except ImportError:
    from strategy import StrategyManager

try:
    from src.data_handler import MarketDataHandler
except ImportError:
    from data_handler import MarketDataHandler

try:
    from src.reporting import ReportingSystem
except ImportError:
    from reporting import ReportingSystem

try:
    from src.logger import setup_logging
except ImportError:
    from logger import setup_logging

# ── FIX: Dummy Database ──
class Database:
    def __init__(self, *args, **kwargs): pass
    def __getattr__(self, name):
        def method(*args, **kwargs):
            class DummyAwaitable:
                def __await__(self):
                    yield
                    return None
            return DummyAwaitable()
        return method

load_dotenv()
logger = logging.getLogger(__name__)

def test_tmp_persistence():
    test_file = "/tmp/deplexo_persistence_test.txt"
    try:
        if os.path.exists(test_file):
            with open(test_file, "r") as f:
                logger.info(f"🔍 TEST FILE EXISTS: {f.read()}")
        else:
            with open(test_file, "w") as f:
                f.write(f"created at {datetime.now().isoformat()}")
            logger.info("🔍 TEST FILE CREATED (first run)")
    except Exception as e:
        logger.error(f"❌ /tmp test failed: {e}")

class TradingBot:
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.running = False
        self.database = None
        self.market_data = None
        self.strategy_manager = None
        self.telegram_bot = None
        self.reporting = None

    async def initialize(self):
        # 1. Database
        self.database = Database() 
        
        # 2. Market Data
        self.market_data = MarketDataHandler()
        
        # 3. Strategy Manager (Based on logs, this takes 2 args)
        self.strategy_manager = StrategyManager(self.market_data, self.database)
        
        # 4. Reporting (Based on logs, this takes 1 arg)
        self.reporting = ReportingSystem(self.database)

        # 5. Telegram Bot (Based on logs, it takes exactly 3 args: market_data, strategy_manager, reporting)
        self.telegram_bot = TelegramBot(
            self.market_data, 
            self.strategy_manager, 
            self.reporting
        )
            
        logger.info("✅ All components initialized successfully")

    async def run_components(self):
        tasks = []
        if self.telegram_bot and hasattr(self.telegram_bot, 'start'):
            tasks.append(asyncio.create_task(self.telegram_bot.start(), name="telegram"))
        if self.strategy_manager and hasattr(self.strategy_manager, 'start_monitoring'):
            tasks.append(asyncio.create_task(self.strategy_manager.start_monitoring(), name="strategy"))
        if self.reporting and hasattr(self.reporting, 'start'):
            tasks.append(asyncio.create_task(self.reporting.start(), name="reporting"))
        if self.market_data and hasattr(self.market_data, 'start_data_collection'):
            tasks.append(asyncio.create_task(self.market_data.start_data_collection(), name="data"))

        if not tasks:
            logger.error("❌ No components to run!")
            return

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in done:
            exc = task.exception()
            if exc:
                logger.error(f"❌ Task '{task.get_name()}' crashed: {exc}", exc_info=exc)
            else:
                logger.warning(f"⚠️ Task '{task.get_name()}' finished unexpectedly")

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
        for name, component in [
            ("telegram", self.telegram_bot),
            ("strategy", self.strategy_manager),
            ("reporting", self.reporting),
            ("data", self.market_data),
            ("database", self.database),
        ]:
            if component is None:
                continue
            try:
                stop_fn = getattr(component, "stop", None) or getattr(component, "close", None)
                if stop_fn:
                    result = stop_fn()
                    if asyncio.iscoroutine(result):
                        await result
                    logger.info(f"✅ {name} stopped")
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")
        logger.info("✅ Shutdown complete")

async def main():
    setup_logging()
    test_tmp_persistence()

    max_restarts = int(os.getenv("MAX_RESTARTS", "5"))
    restart_count = 0
    backoff_steps = [10, 30, 60, 120, 300]

    while restart_count < max_restarts:
        bot = TradingBot()

        def handle_signal(signum):
            logger.info(f"📴 Received signal {signum}, shutting down...")
            bot.running = False
            bot.shutdown_event.set()

        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))
        except (NotImplementedError, RuntimeError) as e:
            logger.warning(f"Could not set signal handlers: {e}")

        try:
            await bot.start()
            logger.info("👋 Bot shut down gracefully, exiting")
            break
        except Exception as e:
            restart_count += 1
            logger.error(f"💥 Bot crashed (attempt {restart_count}/{max_restarts}): {e}", exc_info=True)
            if restart_count >= max_restarts:
                logger.critical(f"❌ Max restarts reached. Giving up.")
                break
            delay = backoff_steps[min(restart_count - 1, len(backoff_steps) - 1)]
            logger.info(f"⏳ Restarting in {delay}s...")
            await asyncio.sleep(delay)

    logger.info("👋 Bot process exiting")

if __name__ == "__main__":
    asyncio.run(main())
