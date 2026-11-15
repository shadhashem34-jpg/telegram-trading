import asyncio
from typing import Optional
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

# ===================== تنظیمات استراتژی =====================
SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
OB_LOOKBACK = 5          # تعداد کندل‌های مورد بررسی برای تشخیص اوردر بلاک
ZONE_PROXIMITY_ATR = 1.5 # قیمت باید حداکثر ۱.۵ ATR با زون فاصله داشته باشد

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
        logger.log_system_event("🚀 MTF Strategy Started (2H Order Blocks -> 1H/15m Triggers)")

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

        # 🟢 استفاده از اوردر بلاک‌های تایم ۲ ساعته برای پیدا کردن سطوح
        order_blocks = self.find_order_blocks(h2)
        valid_zones = [z for z in order_blocks if z['valid']]
        self.zones[symbol] = valid_zones

        price = _price(h2)
        atr = h2['atr']
        trend = "UP" if price > h2['ema_200'] else "DOWN"

        # لاگ RADAR (فقط سطح، بدون تغییر در ساختار)
        if valid_zones:
            sorted_zones = sorted(valid_zones, key=lambda z: abs(price - (z['high'] + z['low']) / 2))
            nearest_zones_str = " | ".join([f"[{_fmt(z['low'])} - {_fmt(z['high'])}]" for z in sorted_zones[:3]])
            logger.log_system_event(f"📡 [OB RADAR] {symbol} | Price: {_fmt(price)} | Trend: {trend} | Nearest OBs: {nearest_zones_str}")
        else:
            logger.log_system_event(f"📡 [OB RADAR] {symbol} | Price: {_fmt(price)} | Trend: {trend} | No valid OBs found.")

        # بررسی نزدیکی قیمت به زون‌ها (این بخش بدون تغییر است)
        for zone in valid_zones:
            dist = abs(price - (zone['high'] + zone['low']) / 2)
            if dist <= (ZONE_PROXIMITY_ATR * atr):
                await self.check_mtf_setup(symbol, h2, zone, price, trend)

    # =================================================================
    # 🟢 تابع جدید: پیدا کردن اوردر بلاک‌ها (جایگزین analyze_zones قبلی)
    # =================================================================
    def find_order_blocks(self, h2):
        """
        تشخیص اوردر بلاک بر اساس آخرین کندل مخالف قبل از یک حرکت قوی.
        """
        opens = h2['opens']
        highs = h2['highs']
        lows = h2['lows']
        closes = h2['closes']
        atr = h2['atr']
        
        obs = []
        n = len(closes)
        
        for i in range(1, n - OB_LOOKBACK):
            # === تشخیص اوردر بلاک صعودی (Bullish OB) ===
            # آخرین کندل نزولی (قرمز) قبل از یک حرکت صعودی قوی
            if closes[i] < opens[i]:  # کندل قرمز
                # بررسی حرکت صعودی قوی بعد از آن
                move_high = max(highs[i+1:i+1+OB_LOOKBACK])
                if move_high > highs[i] + (atr * 0.5):  # حرکت به اندازه کافی قوی
                    obs.append({
                        'low': lows[i], 'high': highs[i], 
                        'valid': True, 'type': 'bullish'
                    })
            
            # === تشخیص اوردر بلاک نزولی (Bearish OB) ===
            # آخرین کندل صعودی (سبز) قبل از یک حرکت نزولی قوی
            if closes[i] > opens[i]:  # کندل سبز
                move_low = min(lows[i+1:i+1+OB_LOOKBACK])
                if move_low < lows[i] - (atr * 0.5):  # حرکت نزولی قوی
                    obs.append({
                        'low': lows[i], 'high': highs[i], 
                        'valid': True, 'type': 'bearish'
                    })
        
        return obs

    # =================================================================
    # بقیه توابع (check_mtf_setup و _build_signal) بدون هیچ تغییری
    # =================================================================
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

        # ستاپ شکست (Breakout) - بدون تغییر
        if h1_close > zone_high and h1_open < zone_high:
            h1_body_out = h1_close - zone_high
            h1_total_range = h1_high - h1_low
            if h1_total_range > 0 and (h1_body_out / h1_total_range) >= 0.20:
                if m15_low <= zone_high and m15_close > zone_high and m15_close > m15_open:
                    if trend == "UP":
                        signal = self._build_signal(symbol, "LONG", "BREAKOUT", price, zone_low, zone_high, h2['atr'])
                    else:
                        logger.log_system_event(f"⚠️ [{symbol}] شکست مقاومت رخ داد اما خلاف روند EMA است. سیگنال ارسال نشد.")

        elif h1_close < zone_low and h1_open > zone_low:
            h1_body_out = zone_low - h1_close
            h1_total_range = h1_high - h1_low
            if h1_total_range > 0 and (h1_body_out / h1_total_range) >= 0.20:
                if m15_high >= zone_low and m15_close < zone_low and m15_close < m15_open:
                    if trend == "DOWN":
                        signal = self._build_signal(symbol, "SHORT", "BREAKOUT", price, zone_high, zone_low, h2['atr'])
                    else:
                        logger.log_system_event(f"⚠️ [{symbol}] شکست حمایت رخ داد اما خلاف روند EMA است. سیگنال ارسال نشد.")

        # ستاپ ریجکشن (Rejection) - بدون تغییر
        elif h1_high >= zone_low and h1_close < zone_low and h1_close < h1_open:
            if m15_high >= zone_low and m15_close < zone_low and m15_close < m15_open:
                if trend == "DOWN":
                    signal = self._build_signal(symbol, "SHORT", "REJECTION", price, zone_high, zone_low, h2['atr'])
                else:
                    logger.log_system_event(f"⚠️ [{symbol}] ریجکشن مقاومت رخ داد اما روند EMA صعودی است. سیگنال ارسال نشد.")

        elif h1_low <= zone_high and h1_close > zone_high and h1_close > h1_open:
            if m15_low <= zone_high and m15_close > zone_high and m15_close > m15_open:
                if trend == "UP":
                    signal = self._build_signal(symbol, "LONG", "REJECTION", price, zone_low, zone_high, h2['atr'])
                else:
                    logger.log_system_event(f"⚠️ [{symbol}] ریجکشن حمایت رخ داد اما روند EMA نزولی است. سیگنال ارسال نشد.")

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
