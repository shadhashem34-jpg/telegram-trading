import asyncio
from datetime import datetime, timezone
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()

# ===================== تنظیمات =====================
CANDLES_4H = 400        # تعداد کندل بسته‌شده ۴ ساعته (حدود ۶۷ روز)
ATR_PERIOD = 14
EMA_PERIOD = 200
H4_MS = 4 * 60 * 60 * 1000


class MarketDataHandler:
    def __init__(self):
        self.session = HTTP(testnet=False)
        self.symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

    async def start_data_collection(self):
        while True:
            await asyncio.sleep(60)

    @staticmethod
    def _calculate_ema(prices, period):
        """EMA واقعی. اگر دیتا کمتر از period باشد، با دیتای موجود محاسبه و period واقعی برگردانده می‌شود."""
        if not prices:
            return 0.0, 0
        used = min(period, len(prices))
        multiplier = 2 / (used + 1)
        ema = sum(prices[:used]) / used
        for price in prices[used:]:
            ema = (price - ema) * multiplier + ema
        return ema, used

    @staticmethod
    def _calculate_atr(highs, lows, closes, period=ATR_PERIOD):
        if len(closes) < period + 1:
            return 0.0
        trs = [
            max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            for i in range(1, len(closes))
        ]
        return sum(trs[-period:]) / period

    async def get_market_data(self, symbol):
        try:
            # pybit همزمان (sync) است؛ در thread اجرا می‌شود تا event loop بلاک نشود
            resp = await asyncio.to_thread(
                self.session.get_kline,
                category="linear",
                symbol=symbol,
                interval="240",
                limit=CANDLES_4H + 1,
            )

            if resp.get('retCode', 0) != 0:
                logger.log_system_event(f"Bybit error for {symbol}: {resp.get('retMsg')}")
                return None

            rows = resp['result']['list']
            if not rows:
                logger.log_system_event(f"No 4H candles returned for {symbol}")
                return None

            # فقط کندل‌های کاملاً بسته‌شده (شروع + ۴ ساعت <= الان)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            rows = [r for r in rows if int(r[0]) + H4_MS <= now_ms]
            rows.sort(key=lambda r: int(r[0]))  # قدیمی -> جدید

            if len(rows) < 50:
                logger.log_system_event(f"Too few closed 4H candles for {symbol}: {len(rows)}")
                return None

            times = [int(r[0]) for r in rows]
            opens = [float(r[1]) for r in rows]
            highs = [float(r[2]) for r in rows]
            lows = [float(r[3]) for r in rows]
            closes = [float(r[4]) for r in rows]
            volumes = [float(r[5]) for r in rows]

            ema_200, ema_used = self._calculate_ema(closes, EMA_PERIOD)
            atr = self._calculate_atr(highs, lows, closes)

            return {
                'h4': {
                    'times': times,
                    'opens': opens,
                    'highs': highs,
                    'lows': lows,
                    'closes': closes,
                    'volumes': volumes,
                    'close': closes[-1],
                    'ema_200': ema_200,
                    'ema_period_used': ema_used,
                    'atr': atr,
                    'count': len(rows),
                }
            }
        except Exception as e:
            logger.log_system_event(f"Error fetching data for {symbol}: {e}")
            return None
