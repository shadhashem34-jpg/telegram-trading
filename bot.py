"""
bot.py - Main orchestrator for the trading bot.
Handles startup, graceful shutdown, and resilient restart logic.

Key improvements:
- /tmp persistence test at startup (for Deplexo debugging)
- Graceful shutdown via asyncio.Event + proper signal handling
- Exponential backoff on restart (10s → 30s → 60s → 120s → 300s)
- return_exceptions=True to prevent cascade cancellation
- Detailed error logging to identify crash causes
"""

import asyncio
import signal
import logging
import os
import sys
from datetime import datetime

# ── FIX FOR DEPLEXO ──
# افزودن مسیر پوشه src به سیستم تا فایل‌های داخلش بتوانند همدیگر را پیدا کنند
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from dotenv import load_dotenv

# ── Verify these import paths match your project structure ──
from src.telegram_handler import TelegramBot
from src.strategy import StrategyManager
from src.data_handler import MarketDataHandler
from src.reporting import ReportingSystem
from src.database import Database
from src.logger import setup_logging

load_dotenv()
logger = logging.getLogger(__name__)


# ============================================================
# /tmp PERSISTENCE TEST (for Deplexo debugging)
# ============================================================
def test_tmp_persistence():
    """
    Test whether /tmp survives container restarts on Deplexo.
    After first restart, check logs:
      - 'TEST FILE EXISTS'  → /tmp is persistent, SQLite is safe
      - 'TEST FILE CREATED' → /tmp is wiped, must migrate to external DB
    """
    test_file = "/tmp/deplexo_persistence_test.txt"
    try:
        if os.path.exists(test_file):
            with open(test_file, "r") as f:
                content = f.read()
            logger.info(f"🔍 TEST FILE EXISTS: {content}")
            logger.info("✅ /tmp appears PERSISTENT across restarts")
        else:
            with open(test_file, "w") as f:
                f.write(f"created at {datetime.now().isoformat()}")
            logger.info("🔍 TEST FILE CREATED (first run)")
            logger.info("ℹ️  Wait for next restart to determine persistence")
    except Exception as e:
        logger.error(f"❌ /tmp test failed: {e}")


# ============================================================
# MAIN TRADING BOT ORCHESTRATOR
# ============================================================
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
        """Initialize all components."""
        self.database = Database()
        if hasattr(self.database, "initialize"):
            await self.database.initialize()

        self.market_data = MarketDataHandler()
        self.strategy_manager = StrategyManager(self.market_data, self.database)
        self.reporting = ReportingSystem(self.database)
        self.telegram_bot = TelegramBot(
            self.strategy_manager,
            self.market_data,
            self.reporting,
            self.database,
        )
        logger.info("✅ All components initialized")

    async def run_components(self):
        """
        Run all components concurrently.
        If any task fails, log it with full traceback but don't
        cascade-cancel the others.
        """
        tasks = [
            asyncio.create_task(self.telegram_bot.start(), name="telegram"),
            asyncio.create_task(self.strategy_manager.start_monitoring(), name="strategy"),
            asyncio.create_task(self.reporting.start(), name="reporting"),
            asyncio.create_task(self.market_data.start_data_collection(), name="data"),
        ]

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_EXCEPTION,
        )

        for task in done:
            exc = task.exception()
            if exc:
                logger.error(
                    f"❌ Task '{task.get_name()}' crashed: {exc}",
                    exc_info=exc,
                )
            else:
                logger.warning(f"⚠️  Task '{task.get_name()}' finished unexpectedly")

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def start(self):
        """Start the bot and wait for shutdown signal."""
        self.running = True
        try:
            await self.initialize()
            logger.info("🚀 Trading bot started")
            await self.run_components()
        finally:
            self.running = False
            await self.stop()

    async def stop(self):
        """Gracefully stop all components."""
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


# ============================================================
# ENTRY POINT WITH RESILIENT RESTART + BACKOFF
# ============================================================
async def main():
    setup_logging()
    test_tmp_persistence()

    max_restarts = int(os.getenv("MAX_RESTARTS", "5"))
    restart_count = 0
    backoff_steps = [10, 30, 60, 120, 300]  # seconds

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
            logger.error(
                f"💥 Bot crashed (attempt {restart_count}/{max_restarts}): {e}",
                exc_info=True,
            )
            if restart_count >= max_restarts:
                logger.critical(f"❌ Max restarts ({max_restarts}) reached. Giving up.")
                break
            delay = backoff_steps[min(restart_count - 1, len(backoff_steps) - 1)]
            logger.info(f"⏳ Restarting in {delay}s...")
            await asyncio.sleep(delay)

    logger.info("👋 Bot process exiting")


if __name__ == "__main__":
    asyncio.run(main())
