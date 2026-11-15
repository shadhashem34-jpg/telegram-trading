import asyncio
import time
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()

CANDLES_4H = 400
ATR_PERIOD = 14
EMA_PERIOD = 200
H4_MS = 4 * 60 * 60 * 1000
CANDLE_GRACE_SEC = 15
RETRY_AFTER_FAIL_SEC = 90

class MarketDataHandler:
    def __init__(self):
        self.session = HTTP(testnet=False)
        self.symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        self._cache = {}
        self._next_fetch = {}

    async def start_data_collection(self):
        """متد اضافه شده برای هماهنگی با bot.py"""
        while True:
            await asyncio.sleep(60)

    @staticmethod
    def _calculate_ema(prices, period):
        if not prices: return 0.0, 0
        used = min(period, len(prices))
        multiplier = 2 / (used + 1)
        ema = sum(prices[:used]) / used
        for price in prices[used:]:
            ema = (price - ema) * multiplier + ema
        return ema, used

    @staticmethod
    def _calculate_atr(highs, lows, closes, period=ATR_PERIOD):
        if len(closes) < period + 1: return 0.0
        trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
        return sum(trs[-period:]) / period

    async def _fetch_candles(self, symbol, interval="240", limit=400):
        """دیتای کندل‌ها را می‌گیرد. interval می‌تواند 240، 60 یا 15 باشد."""
        resp = await asyncio.to_thread(
            self.session.get_kline, category="linear", symbol=symbol, interval=interval, limit=limit
        )
        if resp.get('retCode', 0) != 0:
            logger.log_system_event(f"Bybit error for {symbol} ({interval}m): {resp.get('retMsg')}")
            return None

        rows = resp['result']['list']
        if not rows: return None

        # فقط کندل‌های بسته شده
        now_ms = int(time.time() * 1000)
        interval_ms = int(interval) * 60 * 1000
        rows = [r for r in rows if int(r[0]) + interval_ms <= now_ms]
        rows.sort(key=lambda r: int(r[0]))

        closes = [float(r[4]) for r in rows]
        highs = [float(r[2]) for r in rows]
        lows = [float(r[3]) for r in rows]
        
        ema_200, ema_used = (self._calculate_ema(closes, EMA_PERIOD) if interval == "240" else (0, 0))
        atr = self._calculate_atr(highs, lows, closes) if interval == "240" else 0.0

        return {
            'times': [int(r[0]) for r in rows],
            'opens': [float(r[1]) for r in rows],
            'highs': highs, 'lows': lows, 'closes': closes,
            'close': closes[-1],
            'ema_200': ema_200, 'ema_period_used': ema_used,
            'atr': atr, 'count': len(rows)
        }

    async def _fetch_price(self, symbol):
        try:
            resp = await asyncio.to_thread(self.session.get_tickers, category="linear", symbol=symbol)
            if resp.get('retCode', 0) != 0: return None
            lst = resp['result']['list']
            return float(lst[0]['lastPrice']) if lst else None
        except Exception as e:
            return None

    async def get_4h_data(self, symbol):
        """دیتای ۴ ساعته (با کش و زمان‌بندی)"""
        try:
            now = time.time()
            if now >= self._next_fetch.get(symbol, 0):
                candles = await self._fetch_candles(symbol, "240", CANDLES_4H + 1)
                if candles:
                    self._cache[symbol] = candles
                    next_close = (candles['times'][-1] + 2 * H4_MS) / 1000
                    self._next_fetch[symbol] = next_close + CANDLE_GRACE_SEC
                else:
                    self._next_fetch[symbol] = now + RETRY_AFTER_FAIL_SEC

            cached = self._cache.get(symbol)
            if not cached: return None

            price = await self._fetch_price(symbol)
            h4 = dict(cached)
            h4['price'] = price if price else cached['close']
            h4['price_live'] = bool(price)
            return {'h4': h4}
        except Exception as e:
            logger.log_system_event(f"Error fetching 4H data for {symbol}: {e}")
            return None

    async def get_lower_tf_data(self, symbol, interval="60", limit=50):
        """دیتای تایم‌فریم پایین‌تر (۱ ساعته یا ۱۵ دقیقه) - فقط در زمان نیاز فراخوانی شود"""
        try:
            return await self._fetch_candles(symbol, interval, limit)
        except Exception as e:
            logger.log_system_event(f"Error fetching {interval}m data for {symbol}: {e}")
            return None
