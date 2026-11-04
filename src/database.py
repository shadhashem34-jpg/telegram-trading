import sqlite3
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

class DatabaseManager:
    def __init__(self, db_name="trading_bot.db"):
        self.db_name = db_name
        self._create_table()

    def _get_connection(self):
        # check_same_thread=False allows the bot to use the DB from different async tasks
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _create_table(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    entry_price REAL,
                    stop_loss REAL,
                    take_profit_1 REAL,
                    take_profit_2 REAL,
                    strategy TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()

    def save_signal(self, signal_data: dict):
        """ذخیره سیگنال جدید در دیتابیس"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO signals (symbol, signal_type, entry_price, stop_loss, take_profit_1, take_profit_2, strategy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    signal_data.get('symbol'),
                    signal_data.get('type'),
                    signal_data.get('entry_price'),
                    signal_data.get('stop_loss'),
                    signal_data.get('take_profit_1'),
                    signal_data.get('take_profit_2', 0),
                    signal_data.get('strategy', 'Golden_V6')
                ))
                conn.commit()
                logger.log_system_event(f"💾 Signal for {signal_data.get('symbol')} saved to database.")
        except Exception as e:
            logger.log_system_event(f"Database Error (save_signal): {e}")

    def get_today_signal_count(self) -> int:
        """تعداد سیگنال‌های ارسال شده امروز (به وقت UTC) را برمی‌گرداند"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # Get today's date in UTC string format YYYY-MM-DD
                today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                cursor.execute(
                    "SELECT COUNT(*) as count FROM signals WHERE date(timestamp) = ?", 
                    (today_date,)
                )
                row = cursor.fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.log_system_event(f"Database Error (get_today_signal_count): {e}")
            return 0

    def get_last_signal_time(self, symbol: str = None):
        """زمان آخرین سیگنال را برمی‌گرداند (برای مدیریت فاصله بین سیگنال‌ها)"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                if symbol:
                    cursor.execute(
                        "SELECT timestamp FROM signals WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1", 
                        (symbol,)
                    )
                else:
                    cursor.execute(
                        "SELECT timestamp FROM signals ORDER BY timestamp DESC LIMIT 1"
                    )
                row = cursor.fetchone()
                if row:
                    # Convert string back to datetime object
                    return datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                return None
        except Exception as e:
            logger.log_system_event(f"Database Error (get_last_signal_time): {e}")
            return None
