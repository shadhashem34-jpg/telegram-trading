import os
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from logger import get_trading_logger
from database import log_interaction

logger = get_trading_logger()

class TelegramBot:
    def __init__(self, market_data, strategy_manager, reporting):
        self.market_data = market_data; self.strategy_manager = strategy_manager; self.reporting = reporting
        self.token = os.getenv('TELEGRAM_BOT_TOKEN'); self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.application = None; self.running = False

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🤖 Trading Bot Started\n\n✅ System initialized\n📊 Ready to generate signals\n\nUse /help for available commands")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Available commands:\n/start - Start the bot\n/help - Show this message")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await log_interaction(str(update.message.from_user.id), update.message.text)
        await update.message.reply_text("I received your message. Use /help to see available commands.")

    async def start(self):
        logger.log_system_event("Starting Telegram Bot...")
        self.application = Application.builder().token(self.token).build()
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

        logger.log_system_event("Clearing old Telegram webhooks to prevent Conflict...")
        await self.application.bot.delete_webhook(drop_pending_updates=True)
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        self.running = True
        
        if self.chat_id:
            try: await self.application.bot.send_message(chat_id=self.chat_id, text="🤖 Trading Bot Started\n\n✅ System initialized\n📊 Ready to generate signals")
            except Exception: pass

    async def stop(self):
        self.running = False
        if self.application:
            await self.application.updater.stop(); await self.application.stop(); await self.application.shutdown()
