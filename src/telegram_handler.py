import os
import asyncio
import socket
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from logger import get_trading_logger
from database import log_interaction

logger = get_trading_logger()

# 🛠️ فیکس مشکل DNS
_original_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return _original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4

class TelegramBot:
    def __init__(self, market_data, strategy_manager, reporting):
        self.market_data = market_data
        self.strategy_manager = strategy_manager
        self.reporting = reporting
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.application = None
        self.running = False

    async def send_signal(self, signal: dict):
        """فرستادن سیگنال به تلگرام"""
        if not self.application or not self.chat_id:
            logger.log_system_event("⚠️ Cannot send signal: App not ready or chat_id missing")
            return
        try:
            text = (
                f"🚨 **SIGNAL: {signal.get('symbol')}**\n\n"
                f"📈 Type: {signal.get('type')}\n"
                f"💰 Entry: {signal.get('entry_price')}\n"
                f"🛑 SL: {signal.get('stop_loss')}\n"
                f"🎯 TP1: {signal.get('take_profit_1')}\n"
                f"🎯 TP2: {signal.get('take_profit_2')}\n"
            )
            await self.application.bot.send_message(chat_id=self.chat_id, text=text, parse_mode='Markdown')
            logger.log_system_event(f"✅ Signal sent to Telegram for {signal.get('symbol')}")
        except Exception as e:
            logger.log_system_event(f"❌ Error sending signal: {e}")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🤖 Trading Bot is running and scanning for signals.")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Available commands:\n/start - Start the bot\n/help - Show this message")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await log_interaction(str(update.message.from_user.id), update.message.text)
        await update.message.reply_text("I received your message. Use /help to see available commands.")

    async def start(self):
        self.running = True
        while self.running:
            try:
                logger.log_system_event("Building Telegram Application...")
                self.application = Application.builder().token(self.token).read_timeout(30).write_timeout(30).connect_timeout(30).build()
                self.application.add_handler(CommandHandler("start", self.start_command))
                self.application.add_handler(CommandHandler("help", self.help_command))
                self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

                await self.application.initialize()
                await self.application.start()
                await self.application.updater.start_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
                
                logger.log_system_event("✅ Bot is live and polling for updates.")
                
                if self.chat_id:
                    await self.application.bot.send_message(chat_id=self.chat_id, text="🤖 Trading Bot Started\n✅ Ready to scan for signals.")
                
                while self.running and self.application.updater.running:
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.log_system_event(f"⚠️ Connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            finally:
                if self.application:
                    try:
                        if self.application.updater.running: await self.application.updater.stop()
                        if self.application.running: await self.application.stop(); await self.application.shutdown()
                    except: pass
                    self.application = None

    async def stop(self):
        self.running = False
