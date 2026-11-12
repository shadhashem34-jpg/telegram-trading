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

    async def get_market_data(self, symbol):
        try:
            category = "linear"
            
            kline_4h = self.session.get_kline(category=category, symbol=symbol, interval="240", limit=60)['result']['list']
            kline_1h = self.session.get_kline(category=category, symbol=symbol, interval="60", limit=50)['result']['list']
            kline_15m = self.session.get_kline(category=category, symbol=symbol, interval="15", limit=50)['result']['list']

            kline_4h = kline_4h[1:]; kline_1h = kline_1h[1:]; kline_15m = kline_15m[1:]

            k4h = kline_4h[::-1]; k1h = kline_1h[::-1]; k15m = kline_15m[::-1]

            if not k4h or not k1h or not k15m: return None

            # 4H Data
            h4_closes = [float(k[4]) for k in k4h]
            h4_highs = [float(k[2]) for k in k4h]
            h4_lows = [float(k[3]) for k in k4h]
            ema_200_4h = self._calculate_ema(h4_closes, min(200, len(h4_closes)))

            # 1H Data
            h1_highs = [float(k[2]) for k in k1h]
            h1_lows = [float(k[3]) for k in k1h]
            h1_closes = [float(k[4]) for k in k1h]

            # 15M Data
            m15_closes = [float(k[4]) for k in k15m]
            m15_highs = [float(k[2]) for k in k15m]
            m15_lows = [float(k[3]) for k in k15m]
            atr_15m = self._calculate_atr(m15_highs, m15_lows, m15_closes)

            return {
                'h4': { 
                    'close': h4_closes[-1], 
                    'highs': h4_highs[-30:], # ۳۰ کندل اخیر ۴ ساعته
                    'lows': h4_lows[-30:], 
                    'ema_200': ema_200_4h 
                },
                'h1': {
                    'highs': h1_highs[-15:], # ۱۵ کندل اخیر ۱ ساعته
                    'lows': h1_lows[-15:],
                    'closes': h1_closes[-15:]
                },
                'm15': {
                    'close': m15_closes[-1], 
                    'high': m15_highs[-1], 
                    'low': m15_lows[-1],
                    'open': float(k15m[-1][1]),
                    'atr': atr_15m
                }
            }
        except Exception as e:
            logger.log_system_event(f"Error fetching data for {symbol}: {e}")
            return None
