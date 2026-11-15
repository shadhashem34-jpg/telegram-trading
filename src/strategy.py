import asyncio
from typing import Optional
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

# ===================== تنظیمات استراتژی =====================
SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
SWING_N = 3
CLUSTER_PCT = 0.003
CLUSTER_ATR_MULT = 0.5
MIN_TOUCHES = 2
REACTION_ATR = 0.7
REACTION_LOOKAHEAD = 6
ZONE_PROXIMITY_ATR = 1.5  # قیمت باید حداکثر ۱.۵ ATR با زون فاصله داشته باشد تا چک کنیم

def _fmt(p: float) -> str:
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.3f}"
    return f"{p:.6f}"

def _price(h4: dict) -> float:
    return h4.get('price') or h4['close']

class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.zones = {}
        self._last_signal_time = {}  # جلوگیری از ارسال سیگنال تکراری برای یک زون
        
    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 MTF Strategy Started (4H Zones -> 1H/15m Triggers)")

        while self.running:
            for symbol in SCAN_SYMBOLS:
                try:
                    data = await self.market_data.get_4h_data(symbol)
                    if data:
                        await self.process_symbol(symbol, data)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(10) # فاصله بین بررسی نمادها
            
            # هر ۵ دقیقه یکبار کل چرخه تکرار می‌شود (برای چک کردن کندل‌های ۱۵ دقیقه)
            await asyncio.sleep(300) 

    async def process_symbol(self, symbol, data):
        h4 = data['h4']
        if h4['count'] < 50: return

        # ۱. پیدا کردن زون‌های ۴ ساعته
        swings, zones, _ = self.analyze_zones(symbol, h4)
        valid_zones = [z for z in zones if z['valid']]
        self.zones[symbol] = valid_zones

        price = _price(h4)
        atr = h4['atr']
        trend = "UP" if price > h4['ema_200'] else "DOWN"

        # 🟢 لاگ جدید: نمایش قیمت و نزدیک‌ترین زون‌های معتبر
        if valid_zones:
            # مرتب‌سازی زون‌ها بر اساس فاصله از قیمت فعلی
            sorted_zones = sorted(valid_zones, key=lambda z: abs(price - (z['high'] + z['low']) / 2))
            nearest_zones_str = " | ".join([f"[{_fmt(z['low'])} - {_fmt(z['high'])}]" for z in sorted_zones[:3]])
            logger.log_system_event(f"📡 [ZONE RADAR] {symbol} | Price: {_fmt(price)} | Trend: {trend} | Nearest Zones: {nearest_zones_str}")
        else:
            logger.log_system_event(f"📡 [ZONE RADAR] {symbol} | Price: {_fmt(price)} | Trend: {trend} | No valid zones found.")

        # ۲. بررسی زون‌های نزدیک به قیمت
        for zone in valid_zones:
            dist = abs(price - (zone['high'] + zone['low']) / 2)
            if dist <= (ZONE_PROXIMITY_ATR * atr):
                # قیمت به زون رسیده است -> بررسی مولتی تایم‌فریم
                await self.check_mtf_setup(symbol, h4, zone, price, trend)

    async def check_mtf_setup(self, symbol, h4, zone, price, trend):
        """بررسی ستاپ‌های شکست و ریجکشن در تایم‌فریم‌های پایین‌تر"""
        zone_high, zone_low = zone['high'], zone['low']
        
        # دریافت دیتای ۱ ساعته و ۱۵ دقیقه
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

        # جلوگیری از ارسال سیگنال تکراری در همان زون (مثلاً هر ۲۴ ساعت یکبار)
        zone_key = f"{symbol}_{zone['low']}_{zone['high']}"
        if zone_key in self._last_signal_time:
            if (datetime.now(timezone.utc).timestamp() - self._last_signal_time[zone_key]) < 86400:
                return

        signal = None

        # ==================== ستاپ ۱: شکست (Breakout) ====================
        # مقاومت شکسته شده: کندل ۱ ساعته بالای زون بسته شده (با بدنه)
        if h1_close > zone_high and h1_open < zone_high:
            # بررسی قدرت کندل ۱ ساعته (حداقل ۲۰٪ بدنه بیرون زون باشد)
            h1_body_out = h1_close - zone_high
            h1_total_range = h1_high - h1_low
            if h1_total_range > 0 and (h1_body_out / h1_total_range) >= 0.20:
                # تاییدیه ۱۵ دقیقه: پولبک به زون و بسته شدن بالای آن
                if m15_low <= zone_high and m15_close > zone_high and m15_close > m15_open:
                    if trend == "UP":
                        signal = self._build_signal(symbol, "LONG", "BREAKOUT", price, zone_low, zone_high, h4['atr'])
                    else:
                        logger.log_system_event(f"⚠️ [{symbol}] شکست مقاومت رخ داد اما خلاف روند EMA است. سیگنال ارسال نشد.")

        # حمایت شکسته شده: کندل ۱ ساعته زیر زون بسته شده (با بدنه)
        elif h1_close < zone_low and h1_open > zone_low:
            h1_body_out = zone_low - h1_close
            h1_total_range = h1_high - h1_low
            if h1_total_range > 0 and (h1_body_out / h1_total_range) >= 0.20:
                # تاییدیه ۱۵ دقیقه: پولبک به زون و بسته شدن زیر آن
                if m15_high >= zone_low and m15_close < zone_low and m15_close < m15_open:
                    if trend == "DOWN":
                        signal = self._build_signal(symbol, "SHORT", "BREAKOUT", price, zone_high, zone_low, h4['atr'])
                    else:
                        logger.log_system_event(f"⚠️ [{symbol}] شکست حمایت رخ داد اما خلاف روند EMA است. سیگنال ارسال نشد.")

        # ==================== ستاپ ۲: ریجکشن / عدم شکست (Rejection) ====================
        # ریجکشن مقاومت: قیمت به مقاومت زده اما ۱ ساعته زیر آن بسته شده
        elif h1_high >= zone_low and h1_close < zone_low and h1_close < h1_open:
            # تاییدیه ۱۵ دقیقه: سایه داخل زون، بدنه بیرون (پایین‌تر)
            if m15_high >= zone_low and m15_close < zone_low and m15_close < m15_open:
                if trend == "DOWN": # ریجکشن مقاومت باید در روند نزولی یا خنثی باشد
                    signal = self._build_signal(symbol, "SHORT", "REJECTION", price, zone_high, zone_low, h4['atr'])
                else:
                    logger.log_system_event(f"⚠️ [{symbol}] ریجکشن مقاومت رخ داد اما روند EMA صعودی است. سیگنال ارسال نشد.")

        # ریجکشن حمایت: قیمت به حمایت زده اما ۱ ساعته بالای آن بسته شده
        elif h1_low <= zone_high and h1_close > zone_high and h1_close > h1_open:
            # تاییدیه ۱۵ دقیقه: سایه داخل زون، بدنه بیرون (بالاتر)
            if m15_low <= zone_high and m15_close > zone_high and m15_close > m15_open:
                if trend == "UP": # ریجکشن حمایت باید در روند صعودی یا خنثی باشد
                    signal = self._build_signal(symbol, "LONG", "REJECTION", price, zone_low, zone_high, h4['atr'])
                else:
                    logger.log_system_event(f"⚠️ [{symbol}] ریجکشن حمایت رخ داد اما روند EMA نزولی است. سیگنال ارسال نشد.")

        if signal and self.telegram_bot:
            await self.telegram_bot.send_signal(signal)
            self._last_signal_time[zone_key] = datetime.now(timezone.utc).timestamp()

    def _build_signal(self, symbol, direction, setup_type, entry, sl_zone_edge, tp_zone_edge, atr):
        """ساخت سیگنال با RR=1:1"""
        # حد ضرر: لبه مخالف زون ۴ ساعته (برای شکست) یا لبه بیرونی زون (برای ریجکشن)
        sl = sl_zone_edge 
        
        # محاسبه فاصله ورود تا استاپ
        risk = abs(entry - sl)
        
        # اگر ریسک خیلی کم یا زیاد بود، آن را با ATR تنظیم می‌کنیم (ایمنی)
        if risk < (atr * 0.2) or risk > (atr * 3):
            risk = atr * 1.5 

        # حد سود: RR=1:1
        if direction == "LONG":
            tp = entry + risk
            sl = entry - risk
        else:
            tp = entry - risk
            sl = entry + risk

        return {
            'symbol': symbol,
            'direction': direction,
            'setup': setup_type,
            'entry': entry,
            'sl': sl,
            'tp': tp,
            'rr': "1:1",
            'time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        }

    # --- توابع پیدا کردن زون (همان کد قبلی شما با تغییرات جزئی) ---
    def analyze_zones(self, symbol, h4):
        highs, lows = h4['highs'], h4['lows']
        price, atr = _price(h4), h4['atr']
        max_width = price * CLUSTER_PCT
        if atr > 0: max_width = min(max_width, atr * CLUSTER_ATR_MULT)
        swings = self._find_swings(highs, lows, SWING_N)
        clusters = self._cluster(swings, max_width)
        zones = [self._build_zone(c, highs, lows, atr, price) for c in clusters]
        return swings, zones, max_width

    @staticmethod
    def _find_swings(highs, lows, n):
        swings = []
        for i in range(n, len(highs) - n):
            if highs[i] > max(highs[i - n:i]) and highs[i] >= max(highs[i + 1:i + n + 1]):
                swings.append({'i': i, 'price': highs[i], 'kind': 'H'})
            if lows[i] < min(lows[i - n:i]) and lows[i] <= min(lows[i + 1:i + n + 1]):
                swings.append({'i': i, 'price': lows[i], 'kind': 'L'})
        return swings

    @staticmethod
    def _reaction(s, highs, lows):
        i = s['i']
        end = min(len(highs), i + 1 + REACTION_LOOKAHEAD)
        if end <= i + 1: return 0.0
        return s['price'] - min(lows[i + 1:end]) if s['kind'] == 'H' else max(highs[i + 1:end]) - s['price']

    @staticmethod
    def _cluster(swings, max_width):
        swings = sorted(swings, key=lambda s: s['price'])
        clusters, cur = [], []
        for s in swings:
            if cur and s['price'] - cur[0]['price'] > max_width:
                clusters.append(cur)
                cur = [s]
            else:
                cur.append(s)
        if cur: clusters.append(cur)
        return clusters

    def _build_zone(self, cluster, highs, lows, atr, price):
        prices = [s['price'] for s in cluster]
        z_low, z_high = min(prices), max(prices)
        reacted = sum(1 for s in cluster if atr > 0 and self._reaction(s, highs, lows) >= REACTION_ATR * atr)
        touches = len(cluster)
        valid = touches >= MIN_TOUCHES and reacted >= MIN_TOUCHES
        return {'low': z_low, 'high': z_high, 'valid': valid, 'touches': touches, 'reacted': reacted}
