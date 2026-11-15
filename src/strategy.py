import asyncio
from typing import Optional
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

# ===================== تنظیمات استراتژی =====================
SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
ZONE_PROXIMITY_ATR = 1.5
MAX_ZONE_WIDTH_PCT = 0.005  # حداکثر عرض مجاز زون 0.5 درصد قیمت

def _fmt(p: float) -> str:
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.3f}"
    return f"{p:.6f}"

def _price(h2: dict) -> float:
    return h2.get('price') or h2['close']

class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.zones = {}
        self._last_signal_time = {} 
        
    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 MTF Strategy Started (2H OBs & 10-Candle Levels -> 1H/15m Triggers)")

        while self.running:
            for symbol in SCAN_SYMBOLS:
                try:
                    data = await self.market_data.get_2h_data(symbol)
                    if data:
                        await self.process_symbol(symbol, data)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(10)
            
            await asyncio.sleep(300) 

    async def process_symbol(self, symbol, data):
        h2 = data['h2']
        if h2['count'] < 20: return

        price = _price(h2)
        atr = h2['atr']
        
        # 🟢 تشخیص روند اصلاح شده
        trend = "UP" if price > h2['ema_200'] else "DOWN"

        # ۱. پیدا کردن سطوح (اوردر بلاک‌های تاریخی + سطوح داینامیک ۱۰ کندل اخیر)
        obs = self.find_order_blocks(h2)
        
        # محاسبه بالاترین و پایین‌ترین سقف/کف ۱۰ کندل اخیر (سطوح داینامیک)
        recent_highs = h2['highs'][-10:]
        recent_lows = h2['lows'][-10:]
        dyn_high = max(recent_highs) if recent_highs else price
        dyn_low = min(recent_lows) if recent_lows else price

        # ترکیب همه سطوح
        all_zones = obs + [
            {'low': dyn_low, 'high': dyn_low + (atr*0.1), 'valid': True, 'type': 'dynamic_low'},
            {'low': dyn_high - (atr*0.1), 'high': dyn_high, 'valid': True, 'type': 'dynamic_high'}
        ]
        
        valid_zones = [z for z in all_zones if z['valid']]
        self.zones[symbol] = valid_zones

        # 🟢 لاگ‌گیری کامل (نمایش روند، سطوح داینامیک و ۳ اوردر بلاک نزدیک)
        if obs:
            sorted_obs = sorted(obs, key=lambda z: abs(price - (z['high'] + z['low']) / 2))
            obs_str = " | ".join([f"[{_fmt(z['low'])} - {_fmt(z['high'])}]" for z in sorted_obs[:3]])
        else:
            obs_str = "None"

        logger.log_system_event(f"📡 [MTF RADAR] {symbol} | Price: {_fmt(price)} | Trend: {trend}")
        logger.log_system_event(f"   ↳ Dyn 10C High: {_fmt(dyn_high)} | Dyn 10C Low: {_fmt(dyn_low)}")
        logger.log_system_event(f"   ↳ Historical OBs: {obs_str}")

        # ۲. بررسی نزدیکی قیمت به زون‌ها
        for zone in valid_zones:
            dist = abs(price - (zone['high'] + zone['low']) / 2)
            if dist <= (ZONE_PROXIMITY_ATR * atr):
                await self.check_mtf_setup(symbol, h2, zone, price, trend)

    def find_order_blocks(self, h2):
        """تشخیص اوردر بلاک با عرض باریک (بر اساس بدنه)"""
        opens, highs, lows, closes = h2['opens'], h2['highs'], h2['lows'], h2['closes']
        atr = h2['atr']
        obs = []
        n = len(closes)
        
        # حداکثر عرض مجاز زون (مثلا 0.5% قیمت یا نیم ATR)
        max_width = _price(h2) * MAX_ZONE_WIDTH_PCT
        
        for i in range(1, n - 5):
            # اوردر بلاک صعودی (آخرین کندل قرمز قبل از حرکت صعودی)
            if closes[i] < opens[i]:
                move_high = max(highs[i+1:i+6])
                if move_high > highs[i] + (atr * 0.5):
                    ob_low = min(opens[i], closes[i]) # استفاده از بدنه
                    ob_high = max(opens[i], closes[i])
                    if (ob_high - ob_low) > max_width: # اگر خیلی گشاد بود، از سایه استفاده نکن
                        ob_high = ob_low + max_width
                    obs.append({'low': ob_low, 'high': ob_high, 'valid': True, 'type': 'bullish_ob'})
            
            # اوردر بلاک نزولی (آخرین کندل سبز قبل از حرکت نزولی)
            if closes[i] > opens[i]:
                move_low = min(lows[i+1:i+6])
                if move_low < lows[i] - (atr * 0.5):
                    ob_low = min(opens[i], closes[i])
                    ob_high = max(opens[i], closes[i])
                    if (ob_high - ob_low) > max_width:
                        ob_low = ob_high - max_width
                    obs.append({'low': ob_low, 'high': ob_high, 'valid': True, 'type': 'bearish_ob'})
        
        return obs

    async def check_mtf_setup(self, symbol, h2, zone, price, trend):
        zone_high, zone_low = zone['high'], zone['low']
        
        h1_data = await self.market_data.get_lower_tf_data(symbol, "60", 20)
        m15_data = await self.market_data.get_lower_tf_data(symbol, "15", 20)
        
        if not h1_data or not m15_data or len(h1_data['closes']) < 3 or len(m15_data['closes']) < 3:
            return

        h1_close = h1_data['closes'][-1]
        h1_open = h1_data['opens'][-1]
        h1_high = h1_data['highs'][-1]
        h1_low = h1_data['lows'][-1]
        
        m15_close = m15_data['closes'][-1]
        m15_open = m15_data['opens'][-1]
        m15_high = m15_data['highs'][-1]
        m15_low = m15_data['lows'][-1]

        zone_key = f"{symbol}_{zone['low']}_{zone['high']}"
        if zone_key in self._last_signal_time:
            if (datetime.now(timezone.utc).timestamp() - self._last_signal_time[zone_key]) < 86400:
                return

        signal = None

        # ۱. شکست به بالا (Breakout UP)
        if h1_close > zone_high and h1_open < zone_high:
            if (h1_close - zone_high) / (h1_high - h1_low) >= 0.20:
                # تاییدیه ۱۵ دقیقه: پولبک به زون و بسته شدن بالای آن
                if m15_low <= zone_high and m15_close > zone_high and m15_close > m15_open:
                    if trend == "UP":
                        signal = self._build_signal(symbol, "LONG", "BREAKOUT", price, zone_low, zone_high, h2['atr'])
                    else:
                        logger.log_system_event(f"⚠️ [{symbol}] شکست مقاومت رخ داد اما خلاف روند EMA است.")

        # ۲. شکست به پایین (Breakout DOWN)
        elif h1_close < zone_low and h1_open > zone_low:
            if (zone_low - h1_close) / (h1_high - h1_low) >= 0.20:
                # تاییدیه ۱۵ دقیقه: پولبک به زون و بسته شدن زیر آن
                if m15_high >= zone_low and m15_close < zone_low and m15_close < m15_open:
                    if trend == "DOWN":
                        signal = self._build_signal(symbol, "SHORT", "BREAKOUT", price, zone_high, zone_low, h2['atr'])
                    else:
                        logger.log_system_event(f"⚠️ [{symbol}] شکست حمایت رخ داد اما خلاف روند EMA است.")

        # ۳. ریجکشن مقاومت (Rejection DOWN)
        elif h1_high >= zone_low and h1_close < zone_low and h1_close < h1_open:
            if m15_high >= zone_low and m15_close < zone_low and m15_close < m15_open:
                if trend == "DOWN":
                    signal = self._build_signal(symbol, "SHORT", "REJECTION", price, zone_high, zone_low, h2['atr'])

        # ۴. ریجکشن حمایت (Rejection UP)
        elif h1_low <= zone_high and h1_close > zone_high and h1_close > h1_open:
            if m15_low <= zone_high and m15_close > zone_high and m15_close > m15_open:
                if trend == "UP":
                    signal = self._build_signal(symbol, "LONG", "REJECTION", price, zone_low, zone_high, h2['atr'])

        if signal and self.telegram_bot:
            await self.telegram_bot.send_signal(signal)
            self._last_signal_time[zone_key] = datetime.now(timezone.utc).timestamp()

    def _build_signal(self, symbol, direction, setup_type, entry, sl_zone_edge, tp_zone_edge, atr):
        sl = sl_zone_edge 
        risk = abs(entry - sl)
        if risk < (atr * 0.2) or risk > (atr * 3):
            risk = atr * 1.5 
        if direction == "LONG":
            tp = entry + risk
            sl = entry - risk
        else:
            tp = entry - risk
            sl = entry + risk
        return {
            'symbol': symbol, 'direction': direction, 'setup': setup_type,
            'entry': entry, 'sl': sl, 'tp': tp, 'rr': "1:1",
            'time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        }
