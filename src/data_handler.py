import asyncio
from pybit.unified_trading import HTTP
from logger import get_trading_logger

logger = get_trading_logger()

class MarketDataHandler:
    def __init__(self):
        self.session = HTTP(testnet=False)
        # نماد طلا در بای‌بیت XAUUSDT است، نه XAUUSD
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

    async def get_market_data(self, symbol):
        try:
            # همه نمادها در بای‌بیت از نوع linear (پیرپچوال) هستند
            category = "linear"
            
            kline_4h = self.session.get_kline(category=category, symbol=symbol, interval="240", limit=200)['result']['list']
            kline_1h = self.session.get_kline(category=category, symbol=symbol, interval="60", limit=200)['result']['list']
            kline_30m = self.session.get_kline(category=category, symbol=symbol, interval="30", limit=200)['result']['list']

            # 🚨 FIX: Drop the current unclosed candle
            kline_4h = kline_4h[1:] 
            kline_1h = kline_1h[1:]
            kline_30m = kline_30m[1:]

            k4h = kline_4h[::-1]
            k1h = kline_1h[::-1]
            k30m = kline_30m[::-1]

            if not k4h or not k1h or not k30m: return None

            h4_closes = [float(k[4]) for k in k4h]
            h4_highs = [float(k[2]) for k in k4h]
            h4_lows = [float(k[3]) for k in k4h]
            ema_200_4h = self._calculate_ema(h4_closes, 200)
            adx_4h = self._calculate_adx(h4_highs, h4_lows, h4_closes)
            atr_4h = self._calculate_atr(h4_highs, h4_lows, h4_closes)
            
            h1_closes = [float(k[4]) for k in k1h]
            h1_highs = [float(k[2]) for k in k1h]
            h1_lows = [float(k[3]) for k in k1h]
            ema_20_1h = self._calculate_ema(h1_closes, 20)
            atr_1h = self._calculate_atr(h1_highs, h1_lows, h1_closes)
            
            m30_opens = [float(k[1]) for k in k30m]
            m30_closes = [float(k[4]) for k in k30m]
            m30_highs = [float(k[2]) for k in k30m]
            m30_lows = [float(k[3]) for k in k30m]
            m30_volumes = [float(k[5]) for k in k30m]

            return {
                'h4': {
                    'close': h4_closes[-1], 'ema_200': ema_200_4h, 'adx': adx_4h, 'atr': atr_4h,
                    'high_100': max(h4_highs[-100:]), 'low_100': min(h4_lows[-100:])
                },
                'h1': {
                    'close': h1_closes[-1], 'ema_20': ema_20_1h, 'atr': atr_1h,
                    'high_20': max(h1_highs[-20:]), 'low_20': min(h1_lows[-20:])
                },
                'm30': {
                    'open': m30_opens[-1], 'close': m30_closes[-1], 'high': m30_highs[-1], 'low': m30_lows[-1],
                    'high_5': max(m30_highs[-6:-1]), 'low_5': min(m30_lows[-6:-1]),   
                    'volume': m30_volumes[-1], 'volume_avg_20': sum(m30_volumes[-20:]) / 20
                }
            }
        except Exception as e:
            logger.log_system_event(f"Error fetching data for {symbol}: {e}")
            return None
