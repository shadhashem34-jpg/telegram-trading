import asyncio
import time
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()


class MarketDataHandler:
    def __init__(self):
        self.session = HTTP(testnet=False)
        self.symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        self._cache = {}
        self._next_fetch = {}
        self.EMA_PERIOD = 200
        self.ATR_PERIOD = 14
        # تعداد کندل درخواستی: ۴ ساعته ۱۰۰۰ کندل و روزانه ۱۸۰ کندل (۶ ماه)
        self.LIMITS = {"240": 1000, "D": 180}
        self.DEFAULT_LIMIT = 200

    async def start_data_collection(self):
        while True:
            await asyncio.sleep(60)

    @staticmethod
    def _calculate_ema(prices, period):
        if not prices:
            return 0.0
        used = min(period, len(prices))
        multiplier = 2 / (used + 1)
        ema = sum(prices[:used]) / used
        for price in prices[used:]:
            ema = (price - ema) * multiplier + ema
        return ema

    @staticmethod
    def _calculate_atr(highs, lows, closes, period=14):
        if len(closes) < period + 1:
            return 0.0
        trs = [
            max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            for i in range(1, len(closes))
        ]
        atr = sum(trs[:period]) / period
        for i in range(period, len(trs)):
            atr = ((atr * (period - 1)) + trs[i]) / period
        return atr

    async def _fetch_candles(self, symbol, interval="240", limit=100):
        resp = await asyncio.to_thread(
            self.session.get_kline, category="linear", symbol=symbol, interval=interval, limit=limit
        )
        if resp.get('retCode', 0) != 0:
            logger.log_system_event(f"Bybit error for {symbol} ({interval}): {resp.get('retMsg')}")
            return None

        rows = resp['result']['list']
        if not rows:
            return None

        # 🛠️ اصلاح محاسبه زمان برای تایم‌فریم روزانه ("D")
        if interval == "D":
            interval_ms = 24 * 60 * 60 * 1000
        else:
            interval_ms = int(interval) * 60 * 1000

        now_ms = int(time.time() * 1000)
        rows = [r for r in rows if int(r[0]) + interval_ms <= now_ms]
        rows.sort(key=lambda r: int(r[0]))
        if not rows:
            return None

        closes = [float(r[4]) for r in rows]
        highs = [float(r[2]) for r in rows]
        lows = [float(r[3]) for r in rows]
        opens = [float(r[1]) for r in rows]

        ema_200 = self._calculate_ema(closes, self.EMA_PERIOD) if interval == "240" else 0.0
        atr = self._calculate_atr(highs, lows, closes, self.ATR_PERIOD)

        return {
            'times': [int(r[0]) for r in rows],
            'opens': opens, 'highs': highs, 'lows': lows, 'closes': closes,
            'close': closes[-1],
            'ema_200': ema_200,
            'atr': atr, 'count': len(rows),
        }

    async def _fetch_price(self, symbol):
        try:
            resp = await asyncio.to_thread(self.session.get_tickers, category="linear", symbol=symbol)
            if resp.get('retCode', 0) != 0:
                return None
            lst = resp['result']['list']
            return float(lst[0]['lastPrice']) if lst else None
        except Exception:
            return None

    async def get_tf_data(self, symbol, interval):
        cache_key = f"{symbol}_{interval}"

        if time.time() < self._next_fetch.get(cache_key, 0):
            cached = self._cache.get(cache_key)
            if cached:
                cached['price'] = await self._fetch_price(symbol) or cached['close']
                return cached

        limit = self.LIMITS.get(interval, self.DEFAULT_LIMIT)
        candles = await self._fetch_candles(symbol, interval, limit)
        if candles:
            self._cache[cache_key] = candles
            
            # 🛠️ اصلاح محاسبه زمان برای تایم‌فریم روزانه ("D")
            if interval == "D":
                interval_ms = 24 * 60 * 60 * 1000
            else:
                interval_ms = int(interval) * 60 * 1000
                
            next_close = (candles['times'][-1] + 2 * interval_ms) / 1000
            self._next_fetch[cache_key] = next_close + 15
            candles['price'] = await self._fetch_price(symbol) or candles['close']
            return candles
