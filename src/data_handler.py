import asyncio
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()

class MarketDataHandler:
    def __init__(self):
        self.session = HTTP(testnet=False)
        self.symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

    async def start_data_collection(self):
        while True:
            await asyncio.sleep(60)

    def _calculate_ema(self, prices, period):
        if len(prices) < period: return prices[-1] if prices else 0
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    def _calculate_atr(self, highs, lows, closes, period=14):
        if len(closes) < period + 1: return 0
        trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
        return sum(trs[-period:]) / period

    def _find_swing_points(self, highs, lows, left=1, right=1):
        swing_highs, swing_lows = [], []
        for i in range(left, len(highs) - right):
            is_high = all(highs[i] > highs[i-j] and highs[i] > highs[i+j] for j in range(1, left+1))
            is_low = all(lows[i] < lows[i-j] and lows[i] < lows[i+j] for j in range(1, left+1))
            if is_high: swing_highs.append(highs[i])
            if is_low: swing_lows.append(lows[i])
        return swing_highs, swing_lows

    async def get_market_data(self, symbol):
        try:
            category = "linear"
            
            # کاهش Limit برای جلوگیری از خطای Rate Limit
            kline_4h = self.session.get_kline(category=category, symbol=symbol, interval="240", limit=60)['result']['list']
            kline_1h = self.session.get_kline(category=category, symbol=symbol, interval="60", limit=50)['result']['list']
            kline_15m = self.session.get_kline(category=category, symbol=symbol, interval="15", limit=50)['result']['list']

            # حذف کندل باز شده (Incomplete)
            kline_4h = kline_4h[1:]; kline_1h = kline_1h[1:]; kline_15m = kline_15m[1:]

            k4h = kline_4h[::-1]; k1h = kline_1h[::-1]; k15m = kline_15m[::-1]

            if not k4h or not k1h or not k15m: return None

            # 4H Data
            h4_closes = [float(k[4]) for k in k4h]
            ema_200_4h = self._calculate_ema(h4_closes, min(200, len(h4_closes)))

            # 1H Data
            h1_closes = [float(k[4]) for k in k1h]
            h1_highs = [float(k[2]) for k in k1h]
            h1_lows = [float(k[3]) for k in k1h]
            h1_vols = [float(k[5]) for k in k1h]
            
            swing_highs, swing_lows = self._find_swing_points(h1_highs, h1_lows, left=1, right=1)
            if not swing_highs: swing_highs = [max(h1_highs[-24:])]
            if not swing_lows: swing_lows = [min(h1_lows[-24:])]

            pdh = max(h1_highs[-24:])
            pdl = min(h1_lows[-24:])

            # 15M Data
            m15_closes = [float(k[4]) for k in k15m]
            m15_highs = [float(k[2]) for k in k15m]
            m15_lows = [float(k[3]) for k in k15m]
            m15_vols = [float(k[5]) for k in k15m]
            atr_15m = self._calculate_atr(m15_highs, m15_lows, m15_closes)

            return {
                'h4': { 'close': h4_closes[-1], 'ema_200': ema_200_4h },
                'h1': {
                    'close': h1_closes[-1],
                    'swing_highs': swing_highs,
                    'swing_lows': swing_lows,
                    'pdh': pdh, 'pdl': pdl,
                    'volume': h1_vols[-1], 'volume_avg_20': sum(h1_vols[-21:-1]) / 20
                },
                'm15': {
                    'close': m15_closes[-1], 'high': m15_highs[-1], 'low': m15_lows[-1],
                    'open': float(k15m[-1][1]),
                    'volume': m15_vols[-1], 'volume_avg_20': sum(m15_vols[-21:-1]) / 20,
                    'atr': atr_15m
                }
            }
        except Exception as e:
            logger.log_system_event(f"Error fetching data for {symbol}: {e}")
            return None
