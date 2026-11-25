import asyncio
import time
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()


class MarketDataHandler:
    """دیتای کندل بسته‌شده + قیمت زنده از Bybit.
    تایم‌فریم‌های مورد نیاز استراتژی V90: 240 (فقط EMA200 برای جهت روند)، 15 (تأیید)، 5 (ورود)."""

    def __init__(self):
        self.session = HTTP(testnet=False)
        self.symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        self._cache = {}
        self._next_fetch = {}
        self._price_cache = {}
        self.EMA_PERIOD = 200
        self.ATR_PERIOD = 14
        self.LIMITS = {"240": 500, "60": 300, "15": 300, "5": 300, "D": 180, "W": 60}
        self.DEFAULT_LIMIT = 200
        self.PRICE_TTL_SEC = 3

    async def start_data_collection(self):
        while True:
            await asyncio.sleep(60)

    @staticmethod
    def _interval_ms(interval):
        if interval == "D":
            return 24 * 60 * 60 * 1000
        if interval == "W":
            return 7 * 24 * 60 * 60 * 1000
        if interval == "M":
            return 30 * 24 * 60 * 60 * 1000
        return int(interval) * 60 * 1000

    @staticmethod
    def _calculate_ema(prices, period):
        if not prices:
            return 0.0
        used = min(period, len(prices))
        multiplier = 2 / (period + 1)
        ema = sum(prices[:used]) / used
        for price in prices[used:]:
            ema = (price - ema) * multiplier + ema
        return ema

    @staticmethod
    def _calculate_atr(highs, lows, closes, period=14):
        if len(closes) < period + 1:
            return 0.0
        trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                   abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
        atr = sum(trs[:period]) / period
        for i in range(period, len(trs)):
            atr = ((atr * (period - 1)) + trs[i]) / period
        return atr

    async def _fetch_candles(self, symbol, interval="240", limit=100):
        resp = await asyncio.to_thread(
            self.session.get_kline, category="linear", symbol=symbol,
            interval=interval, limit=limit
        )
        if resp.get('retCode', 0) != 0:
            logger.log_system_event(
                f"Bybit error for {symbol} ({interval}): {resp.get('retMsg')}"
            )
            return None
        rows = resp.get('result', {}).get('list', [])
        if not rows:
            return None
        interval_ms = self._interval_ms(interval)
        now_ms = int(time.time() * 1000)
        # فقط کندل‌های بسته‌شده (کندل در حال تشکیل حذف می‌شود)
        rows = [r for r in rows if int(r[0]) + interval_ms <= now_ms]
        rows.sort(key=lambda r: int(r[0]))
        if not rows:
            return None
        closes = [float(r[4]) for r in rows]
        highs = [float(r[2]) for r in rows]
        lows = [float(r[3]) for r in rows]
        opens = [float(r[1]) for r in rows]
        volumes = [float(r[5]) for r in rows]
        ema_200 = self._calculate_ema(closes, self.EMA_PERIOD) if interval == "240" else 0.0
        atr = self._calculate_atr(highs, lows, closes, self.ATR_PERIOD)
        return {
            'times': [int(r[0]) for r in rows], 'ms': interval_ms,
            'opens': opens, 'highs': highs, 'lows': lows, 'closes': closes,
            'volumes': volumes,
            'close': closes[-1], 'ema_200': ema_200, 'atr': atr,
            'count': len(rows),
        }

    async def _get_price(self, symbol):
        now = time.time()
        cached = self._price_cache.get(symbol)
        if cached and now - cached[0] < self.PRICE_TTL_SEC:
            return cached[1]
        try:
            resp = await asyncio.to_thread(
                self.session.get_tickers, category="linear", symbol=symbol
            )
            if resp.get('retCode', 0) != 0:
                return cached[1] if cached else None
            lst = resp.get('result', {}).get('list', [])
            price = float(lst[0]['lastPrice']) if lst else None
            if price and price > 0:
                self._price_cache[symbol] = (now, price)
            return price
        except Exception as e:
            logger.log_system_event(f"Ticker error for {symbol}: {e}")
            return cached[1] if cached else None

    async def get_tf_data(self, symbol, interval):
        key = f"{symbol}_{interval}"
        cached = self._cache.get(key)
        if cached is None or time.time() >= self._next_fetch.get(key, 0):
            try:
                candles = await self._fetch_candles(
                    symbol, interval, self.LIMITS.get(interval, self.DEFAULT_LIMIT)
                )
            except Exception as e:
                logger.log_system_event(f"Error fetching {symbol} {interval}: {e}")
                candles = None
            if candles:
                self._cache[key] = candles
                cached = candles
                # آخرین کندل بسته‌شده در open+interval بسته شده؛ کندل بعدی در open+2*interval بسته می‌شود.
                # همان لحظه (+۲ ثانیه) دوباره دریافت می‌کنیم.
                next_close = (candles['times'][-1] + 2 * candles['ms']) / 1000
                self._next_fetch[key] = next_close + 2
            else:
                self._next_fetch[key] = time.time() + 30
        if not cached:
            return None
        out = dict(cached)      # کپی سطحی تا قیمت زنده‌ی یک نماد روی کش مشترک نوشته نشود
        out['price'] = await self._get_price(symbol) or cached['close']
        return out
