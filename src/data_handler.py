import asyncio
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()

class MarketDataHandler:
    def __init__(self):
        self.session = HTTP(testnet=False)
        self.symbols = [
            "XAUUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
            "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "DOGEUSDT", 
            "LTCUSDT", "BCHUSDT", "TRXUSDT", "UNIUSDT", "ATOMUSDT", 
            "ETCUSDT", "FILUSDT", "APTUSDT", "SUIUSDT", "ARBUSDT", 
            "OPUSDT", "NEARUSDT", "INJUSDT", "MATICUSDT"
        ]

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
        trs = []
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            trs.append(tr)
        return sum(trs[-period:]) / period

    def _calculate_adx(self, highs, lows, closes, period=14):
        if len(closes) < period * 2: return 25.0
        trs, plus_dms, minus_dms = [], [], []
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            trs.append(tr)
            up_move = highs[i] - highs[i-1]
            down_move = lows[i-1] - lows[i]
            plus_dm = up_move if (up_move > down_move and up_move > 0) else 0
            minus_dm = down_move if (down_move > up_move and down_move > 0) else 0
            plus_dms.append(plus_dm)
            minus_dms.append(minus_dm)
        
        smooth_tr = sum(trs[-period:])
        smooth_plus_dm = sum(plus_dms[-period:])
        smooth_minus_dm = sum(minus_dms[-period:])
        
        if smooth_tr == 0: return 25.0
        plus_di = 100 * (smooth_plus_dm / smooth_tr)
        minus_di = 100 * (smooth_minus_dm / smooth_tr)
        
        if (plus_di + minus_di) == 0: return 25.0
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        return dx

    def _find_pivots(self, highs, lows, left=2, right=2):
        pivot_highs, pivot_lows = [], []
        for i in range(left, len(highs) - right):
            is_high = all(highs[i] > highs[i-j] and highs[i] > highs[i+j] for j in range(1, left+1))
            is_low = all(lows[i] < lows[i-j] and lows[i] < lows[i+j] for j in range(1, left+1))
            if is_high: pivot_highs.append(highs[i])
            if is_low: pivot_lows.append(lows[i])
        return pivot_highs, pivot_lows

    async def get_market_data(self, symbol):
        try:
            category = "linear"
            
            kline_4h = self.session.get_kline(category=category, symbol=symbol, interval="240", limit=200)['result']['list']
            kline_1h = self.session.get_kline(category=category, symbol=symbol, interval="60", limit=100)['result']['list']
            kline_15m = self.session.get_kline(category=category, symbol=symbol, interval="15", limit=100)['result']['list']

            kline_4h = kline_4h[1:] 
            kline_1h = kline_1h[1:]
            kline_15m = kline_15m[1:]

            k4h = kline_4h[::-1]
            k1h = kline_1h[::-1]
            k15m = kline_15m[::-1]

            if not k4h or not k1h or not k15m: return None

            # 4H Data
            h4_closes = [float(k[4]) for k in k4h]
            h4_highs = [float(k[2]) for k in k4h]
            h4_lows = [float(k[3]) for k in k4h]
            ema_200_4h = self._calculate_ema(h4_closes, 200)
            adx_4h = self._calculate_adx(h4_highs, h4_lows, h4_closes)
            h4_pivot_highs, h4_pivot_lows = self._find_pivots(h4_highs[-30:], h4_lows[-30:])

            # 1H Data (Added ATR for Zones)
            h1_highs = [float(k[2]) for k in k1h]
            h1_lows = [float(k[3]) for k in k1h]
            h1_closes = [float(k[4]) for k in k1h]
            atr_1h = self._calculate_atr(h1_highs, h1_lows, h1_closes)
            h1_pivot_highs, h1_pivot_lows = self._find_pivots(h1_highs[-24:], h1_lows[-24:])
            
            if not h1_pivot_highs: h1_pivot_highs = [max(h1_highs[-24:])]
            if not h1_pivot_lows: h1_pivot_lows = [min(h1_lows[-24:])]

            # 15M Data
            m15_closes = [float(k[4]) for k in k15m]
            m15_highs = [float(k[2]) for k in k15m]
            m15_lows = [float(k[3]) for k in k15m]
            m15_vols = [float(k[5]) for k in k15m]
            atr_15m = self._calculate_atr(m15_highs, m15_lows, m15_closes)

            return {
                'h4': {
                    'close': h4_closes[-1], 'ema_200': ema_200_4h, 'adx': adx_4h,
                    'pivot_highs': h4_pivot_highs, 'pivot_lows': h4_pivot_lows
                },
                'h1': {
                    'close': h1_closes[-1], 
                    'atr': atr_1h,
                    'pivot_highs': h1_pivot_highs, 
                    'pivot_lows': h1_pivot_lows
                },
                'm15': {
                    'close': m15_closes[-1], 
                    'high': m15_highs[-1], 
                    'low': m15_lows[-1],
                    'volume': m15_vols[-1],
                    'volume_avg_20': sum(m15_vols[-21:-1]) / 20,
                    'atr': atr_15m
                }
            }
        except Exception as e:
            logger.log_system_event(f"Error fetching data for {symbol}: {e}")
            return None
