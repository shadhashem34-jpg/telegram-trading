import aiosqlite
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
DB_PATH = "/tmp/trading_bot.db"

async def init_db():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_interactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    text TEXT,
                    timestamp DATETIME
                )
            """)
            await db.commit()
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

async def log_interaction(user_id: str, text: str):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # 🛠️ FIX: Create table right before inserting, to prevent "no such table" errors
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_interactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    text TEXT,
                    timestamp DATETIME
                )
            """)
            await db.execute(
                "INSERT INTO user_interactions (user_id, text, timestamp) VALUES (?, ?, ?)",
                (user_id, text, datetime.now().isoformat())
            )
            await db.commit()
    except Exception as e:
        logger.error(f"Database error in log_interaction: {e}")

class Database:
    def __init__(self):
        pass
        
    async def initialize(self):
        await init_db()
