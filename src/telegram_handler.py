import os
import asyncio
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
                # هر بار از صفر ساخته میشه تا هیچ حافظه‌ای خراب نمونه
                self.application = (
                    Application.builder()
                    .token(self.token)
                    .read_timeout(60)
                    .write_timeout(60)
                    .connect_timeout(60)
                    .pool_timeout(60)
                    .build()
                )
                
                self.application.add_handler(CommandHandler("start", self.start_command))
                self.application.add_handler(CommandHandler("help", self.help_command))
                self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

                logger.log_system_event("Clearing old Telegram webhooks...")
                try:
                    await self.application.bot.delete_webhook(drop_pending_updates=True)
                except Exception as e:
                    logger.log_system_event(f"Webhook deletion warning: {e}")

                logger.log_system_event("Initializing and starting polling...")
                await self.application.initialize()
                await self.application.start()
                
                # تایم‌اوت ۶۰ ثانیه‌ای برای دریافت پیام‌ها
                await self.application.updater.start_polling(
                    drop_pending_updates=True, 
                    allowed_updates=Update.ALL_TYPES,
                    timeout=60
                )
                
                logger.log_system_event("✅ Bot is live and polling for updates.")
                
                while self.running and self.application.updater.running:
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.log_system_event(f"⚠️ Telegram connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            finally:
                # پاکسازی کامل قبل از تلاش بعدی
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
