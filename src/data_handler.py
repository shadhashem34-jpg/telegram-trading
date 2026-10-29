import os, asyncio
from typing import Dict
from datetime import datetime, timezone
from logger import get_trading_logger

try:
    from pybit.unified_trading import HTTP as BybitClient
except ImportError:
    BybitClient = None

logger = get_trading_logger()

class MarketDataHandler:
    def __init__(self):
        self.running = False
        self.data_sources = {}
        self.crypto_pairs = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'DOTUSDT', 'DOGEUSDT', 'LTCUSDT', 'BCHUSDT', 'TRXUSDT', 'UNIUSDT', 'ATOMUSDT', 'ETCUSDT', 'FILUSDT', 'APTUSDT', 'SUIUSDT', 'ARBUSDT', 'OPUSDT', 'NEARUSDT', 'INJUSDT', 'MATICUSDT']
        self.forex_pairs = ['XAUUSD']
        self._initialize_clients()

    def _initialize_clients(self):
        try:
            if os.getenv('BYBIT_API_KEY') and os.getenv('BYBIT_SECRET_KEY'):
                self.data_sources['bybit'] = BybitClient(testnet=False, api_key=os.getenv('BYBIT_API_KEY'), api_secret=os.getenv('BYBIT_SECRET_KEY'))
        except Exception: pass

    async def start_data_collection(self):
        self.running = True
        while self.running: await asyncio.sleep(10)
    async def stop(self): self.running = False

    def calculate_indicators(self, klines: list) -> dict:
        if not klines or len(klines) < 20: return {}
        klines.reverse()
        closes = [float(k[4]) for k in klines]; highs = [float(k[2]) for k in klines]; lows = [float(k[3]) for k in klines]; volumes = [float(k[5]) for k in klines]
        def ema(data, period):
            multiplier = 2 / (period + 1); ema_val = data[0]
            for price in data[1:]: ema_val = (price - ema_val) * multiplier + ema_val
            return ema_val
        trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(klines))]
        atr = sum(trs[-14:]) / 14 if len(trs) >= 14 else (trs[-1] if trs else 0)
        current_close = closes[-1]
        prev_h20 = max(highs[-21:-1]) if len(highs) > 21 else max(highs)
        prev_l20 = min(lows[-21:-1]) if len(lows) > 21 else min(lows)
        return {'close': current_close, 'ema_200': ema(closes, 200), 'ema_50': ema(closes, 50), 'adx': 25.0, 'atr': atr, 'volume': volumes[-1], 'volume_avg_20': sum(volumes[-20:]) / 20, 'high_5': max(highs[-5:]), 'low_5': min(lows[-5:]), 'high_20': max(highs[-20:]), 'low_20': min(lows[-20:]), 'high_100': max(highs[-100:]) if len(highs) >= 100 else max(highs), 'low_100': min(lows[-100:]) if len(lows) >= 100 else min(lows), 'near_ema_50': abs(current_close - ema(closes, 50)) < (atr * 2.5), 'near_level': (abs(current_close - prev_h20) < (atr * 2.5)) or (abs(current_close - prev_l20) < (atr * 2.5))}

    async def get_market_data(self, symbol: str) -> dict:
        try:
            if 'bybit' not in self.data_sources: return {}
            client = self.data_sources['bybit']
            h4_res = client.get_kline(category="linear", symbol=symbol, interval="240", limit=200)
            h1_res = client.get_kline(category="linear", symbol=symbol, interval="60", limit=200)
            m15_res = client.get_kline(category="linear", symbol=symbol, interval="15", limit=200)
            if h4_res.get('retCode') == 0 and h1_res.get('retCode') == 0 and m15_res.get('retCode') == 0:
                return {'h4': self.calculate_indicators(h4_res.get('result', {}).get('list', [])), 'h1': self.calculate_indicators(h1_res.get('result', {}).get('list', [])), 'm15': self.calculate_indicators(m15_res.get('result', {}).get('list', []))}
        except Exception as e: logger.log_error(e, {"context": f"fetch_{symbol}"})
        return {}
