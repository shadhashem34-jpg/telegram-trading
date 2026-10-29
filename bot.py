"""
Telegram Trading Bot - Main Entry Point
"""
import os
import sys
import asyncio
from threading import Thread
from flask import Flask, jsonify
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
app = Flask(__name__)

@app.route('/health')
def health_check():
    return jsonify({'status': 'healthy', 'service': 'telegram-trading-bot'})

@app.route('/')
def index():
    return jsonify({'message': 'Telegram Trading Bot is running', 'status': 'active'})

class TradingBotOrchestrator:
    def __init__(self):
        self.telegram_bot = None
        self.market_data = None
        self.strategy_manager = None
        self.reporting = None
        self.running = False
    
    async def initialize(self):
        try:
            logger.info("🚀 Initializing Telegram Trading Bot...")
            await init_database()
            self.market_data = MarketDataHandler()
            self.strategy_manager = StrategyManager()
            self.reporting = ReportingSystem()
            self.telegram_bot = TelegramBot(self.market_data, self.strategy_manager, self.reporting)
            logger.info("✅ All components initialized")
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            raise
    
    async def start(self):
        try:
            await self.initialize()
            self.running = True
            logger.info("🎯 Starting Telegram Trading Bot...")
            await asyncio.gather(
                self.telegram_bot.start(),
                self.strategy_manager.start_monitoring(),
                self.reporting.start_daily_reports(),
                self.market_data.start_data_collection()
            )
        except Exception as e:
            logger.error(f"❌ Bot start failed: {e}")
            raise
    
    async def stop(self):
        if self.running:
            self.running = False
            if self.telegram_bot: await self.telegram_bot.stop()
            if self.strategy_manager: await self.strategy_manager.stop()
            if self.reporting: await self.reporting.stop()
            if self.market_data: await self.market_data.stop()

def run_flask_app():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

async def main():
    bot = TradingBotOrchestrator()
    def signal_handler(signum, frame): bot.running = False
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    try:
        Thread(target=run_flask_app, daemon=True).start()
        await bot.start()
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
