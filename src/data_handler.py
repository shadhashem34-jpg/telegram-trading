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
            
            kline_2h = self.session.get_kline(category=category, symbol=symbol, interval="120", limit=200)['result']['list']
            kline_30m = self.session.get_kline(category=category, symbol=symbol, interval="30", limit=200)['result']['list']
            kline_15m = self.session.get_kline(category=category, symbol=symbol, interval="15", limit=200)['result']['list']

            # Drop unclosed candles
            kline_2h = kline_2h[1:] 
            kline_30m = kline_30m[1:]
            kline_15m = kline_15m[1:]

            k2h = kline_2h[::-1]
            k30m = kline_30m[::-1]
            k15m = kline_15m[::-1]

            if not k2h or not k30m or not k15m: return None

            # 2H Data
            h2_closes = [float(k[4]) for k in k2h]
            h2_highs = [float(k[2]) for k in k2h]
            h2_lows = [float(k[3]) for k in k2h]
            ema_200_2h = self._calculate_ema(h2_closes, 200)
            adx_2h = self._calculate_adx(h2_highs, h2_lows, h2_closes)
            
            # Find 48h-72h Pivots (24 to 36 candles of 2h)
            recent_h2_highs = h2_highs[-36:]
            recent_h2_lows = h2_lows[-36:]
            pivot_highs, pivot_lows = self._find_pivots(recent_h2_highs, recent_h2_lows)
            
            if not pivot_highs: pivot_highs = [max(recent_h2_highs)]
            if not pivot_lows: pivot_lows = [min(recent_h2_lows)]

            # 30M Data
            m30_closes = [float(k[4]) for k in k30m]
            ema_20_30m = self._calculate_ema(m30_closes, 20)
            atr_30m = self._calculate_atr([float(k[2]) for k in k30m], [float(k[3]) for k in k30m], m30_closes)
            
            # 15M Data
            m15_closes = [float(k[4]) for k in k15m]
            m15_highs = [float(k[2]) for k in k15m]
            m15_lows = [float(k[3]) for k in k15m]
            m15_vols = [float(k[5]) for k in k15m]
            atr_15m = self._calculate_atr(m15_highs, m15_lows, m15_closes)

            return {
                'h2': {
                    'close': h2_closes[-1], 'ema_200': ema_200_2h, 'adx': adx_2h,
                    'pivot_highs': pivot_highs,
                    'pivot_lows': pivot_lows
                },
                'm30': {
                    'close': m30_closes[-1], 
                    'ema_20': ema_20_30m,
                    'atr': atr_30m
                },
                'm15': {
                    'close': m15_closes[-1], 
                    'high': m15_highs[-1], 
                    'low': m15_lows[-1],
                    'atr': atr_15m,
                    'volume': m15_vols[-1],
                    'volume_avg_20': sum(m15_vols[-21:-1]) / 20
                }
            }
        except Exception as e:
            logger.log_system_event(f"Error fetching data for {symbol}: {e}")
            return None
