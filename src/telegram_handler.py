import os
import asyncio
import socket

# 🛠️ فیکس قطعی مشکل DNS و IPv6 در سرورهای ابری
_original_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return _original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4

import telegram
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from logger import get_trading_logger
from database import log_interaction

logger = get_trading_logger()

class TelegramBot:
    def __init__(self, market_data, strategy_manager, reporting):
        self.market_data = market_data
        self.strategy_manager = strategy_manager
        self.reporting = reporting
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.application = None
        self.running = False

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🤖 Trading Bot Started\n\n✅ System initialized\n📊 Ready to generate signals\n\nUse /help for available commands")

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
                self.application = (
                    Application.builder()
                    .token(self.token)
                    .read_timeout(30)
                    .write_timeout(30)
                    .connect_timeout(30)
                    .build()
                )
                
                self.application.add_handler(CommandHandler("start", self.start_command))
                self.application.add_handler(CommandHandler("help", self.help_command))
                self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

                try:
                    await self.application.bot.delete_webhook(drop_pending_updates=True)
                except Exception as e:
                    logger.log_system_event(f"Webhook warning: {e}")

                await self.application.initialize()
                await self.application.start()
                await self.application.updater.start_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
                
                logger.log_system_event("✅ Bot is live and polling for updates.")
                
                while self.running and self.application.updater.running:
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.log_system_event(f"⚠️ Connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            finally:
                if self.application:
                    try:
                        if self.application.updater.running:
                            await self.application.updater.stop()
                        if self.application.running:
                            await self.application.stop()
                            await self.application.shutdown()
                    except Exception:
                        pass
                    self.application = None

    async def stop(self):
        self.running = False
