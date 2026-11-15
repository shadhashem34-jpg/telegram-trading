import asyncio
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.active_setups = {} # ماشین وضعیت برای هر نماد
        self._last_signal_time = {}
        
        # تنظیمات
        self.MIN_REACTIONS = 2
        self.ZONE_WIDTH_PCT = 0.00225 # 0.225%
        self.SWING_N = 3

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V35 MTF Structure Started (4H/2H Zones -> 1H/15m)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    async def process_symbol(self, symbol):
        # دریافت دیتای لازم
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h2 = await self.market_data.get_tf_data(symbol, "120")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h2, h1, m15]): return

        price = h2['price']
        
        # ۱. موتور روند (۴ ساعته)
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        # ۲. زون‌های ۴ ساعته (۱۸ کندل اخیر - ۷۲ ساعت)
        h4_zones = self.find_zones(h4, lookback=18, atr=h4['atr'], price=price)
        
        # ۳. زون‌های ۲ ساعته (۳۶ کندل اخیر - ۷۲ ساعت)
        h2_zones = self.find_zones(h2, lookback=36, atr=h2['atr'], price=price)
        
        # پیدا کردن نزدیک‌ترین حمایت و مقاومت ۲ ساعته
        nearest_res = min([z for z in h2_zones if z['type'] == 'res' and z['low'] > price], key=lambda z: z['low'], default=None)
        nearest_sup = max([z for z in h2_zones if z['type'] == 'sup' and z['high'] < price], key=lambda z: z['high'], default=None)

        # ---------------- لاگ‌گیری حرفه‌ای ----------------
        log_msg = f"📊 {symbol} — 4H Structure: {trend} | Price: {price:.2f}\n"
        if h4_zones:
            h4_sup = [z for z in h4_zones if z['type'] == 'sup']
            h4_res = [z for z in h4_zones if z['type'] == 'res']
            log_msg += f"   ↳ 4H Supp: {h4_sup[0]['low']:.2f}-{h4_sup[0]['high']:.2f} " if h4_sup else "   ↳ 4H Supp: None "
            log_msg += f"| 4H Res: {h4_res[0]['low']:.2f}-{h4_res[0]['high']:.2f}\n" if h4_res else "| 4H Res: None\n"
        
        log_msg += f"⚙️ {symbol} — 2H Levels:\n"
        log_msg += f"   ↳ 2H Nearest Supp: {nearest_sup['low']:.2f}-{nearest_sup['high']:.2f}\n" if nearest_sup else "   ↳ 2H Nearest Supp: None\n"
        log_msg += f"   ↳ 2H Nearest Res: {nearest_res['low']:.2f}-{nearest_res['high']:.2f}\n" if nearest_res else "   ↳ 2H Nearest Res: None\n"
        logger.log_system_event(log_msg)
        # ------------------------------------------------

        # ماشین وضعیت (State Machine)
        setup_key = symbol
        if setup_key not in self.active_setups:
            self.active_setups[setup_key] = {'state': 'IDLE', 'zone': None, 'direction': None}

        state = self.active_setups[setup_key]

        # حالت ۱: نزدیک شدن به زون
        if state['state'] == 'IDLE':
            if nearest_res and abs(price - nearest_res['low']) <= (1.5 * h2['atr']):
                state.update({'state': 'APPROACHING_RES', 'zone': nearest_res, 'direction': 'SHORT'})
                logger.log_system_event(f"🔔 [{symbol}] Approaching 2H Resistance. Waiting for 1H Breakout DOWN...")
            elif nearest_sup and abs(price - nearest_sup['high']) <= (1.5 * h2['atr']):
                state.update({'state': 'APPROACHING_SUP', 'zone': nearest_sup, 'direction': 'LONG'})
                logger.log_system_event(f"🔔 [{symbol}] Approaching 2H Support. Waiting for 1H Breakout UP...")

        # حالت ۲: بررسی شکست در ۱ ساعته
        elif state['state'].startswith('APPROACHING'):
            zone = state['zone']
            h1_close = h1['closes'][-1]
            
            if state['direction'] == 'SHORT' and h1_close < zone['low']:
                state['state'] = 'BREAKOUT_CONFIRMED'
                logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res Zone. Waiting for 15M Pullback...")
            elif state['direction'] == 'LONG' and h1_close > zone['high']:
                state['state'] = 'BREAKOUT_CONFIRMED'
                logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup Zone. Waiting for 15M Pullback...")
            
            # اگر قیمت دور شد، ستاپ باطل می‌شود
            elif (state['direction'] == 'SHORT' and h1_close > zone['high'] + (h2['atr'] * 2)) or \
                 (state['direction'] == 'LONG' and h1_close < zone['low'] - (h2['atr'] * 2)):
                state.update({'state': 'IDLE', 'zone': None, 'direction': None})

        # حالت ۳: بررسی پولبک در ۱۵ دقیقه
        elif state['state'] == 'BREAKOUT_CONFIRMED':
            zone = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if state['direction'] == 'SHORT':
                # سایه بالا وارد زون شده اما بدنه زیر زون بسته شده (و کندل قرمز است)
                if m15_high >= zone['low'] and m15_close < zone['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry Triggered!")
                    self.send_signal(symbol, 'SHORT', m15_close, zone, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None, 'direction': None})
                # شکست ناموفق (برگشت به داخل زون)
                elif m15_close > zone['high']:
                    logger.log_system_event(f"⚠️ [{symbol}] Failed Breakout! Price returned inside zone. Setup Cancelled.")
                    state.update({'state': 'IDLE', 'zone': None, 'direction': None})

            elif state['direction'] == 'LONG':
                # سایه پایین وارد زون شده اما بدنه بالای زون بسته شده (و کندل سبز است)
                if m15_low <= zone['high'] and m15_close > zone['high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry Triggered!")
                    self.send_signal(symbol, 'LONG', m15_close, zone, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None, 'direction': None})
                # شکست ناموفق
                elif m15_close < zone['low']:
                    logger.log_system_event(f"⚠️ [{symbol}] Failed Breakout! Price returned inside zone. Setup Cancelled.")
                    state.update({'state': 'IDLE', 'zone': None, 'direction': None})

    def find_zones(self, data, lookback, atr, current_price):
        """تشخیص زون‌ها با عرض حداکثر 0.225%"""
        highs, lows, closes = data['highs'][-lookback:], data['lows'][-lookback:], data['closes'][-lookback:]
        swings = []
        
        # پیدا کردن سقف/کف‌های معتبر (Swing N)
        for i in range(self.SWING_N, len(highs) - self.SWING_N):
            if highs[i] > max(highs[i - self.SWING_N:i]) and highs[i] >= max(highs[i + 1:i + self.SWING_N + 1]):
                swings.append({'price': highs[i], 'type': 'res'})
            if lows[i] < min(lows[i - self.SWING_N:i]) and lows[i] <= min(lows[i + 1:i + self.SWING_N + 1]):
                swings.append({'price': lows[i], 'type': 'sup'})

        if not swings: return []

        # محدودیت عرض زون (حداکثر 0.225% قیمت)
        max_width = current_price * self.ZONE_WIDTH_PCT

        # خوشه‌بندی سطوح نزدیک
        swings.sort(key=lambda x: x['price'])
        clusters = []
        current_cluster = []
        for s in swings:
            if current_cluster and s['price'] - current_cluster[0]['price'] > max_width:
                clusters.append(current_cluster)
                current_cluster = [s]
            else:
                current_cluster.append(s)
        if current_cluster: clusters.append(current_cluster)

        valid_zones = []
        for c in clusters:
            prices = [s['price'] for s in c]
            if len(prices) >= self.MIN_REACTIONS: # حداقل ۲ واکنش
                z_low, z_high = min(prices), max(prices)
                
                # اعمال سقف عرض زون
                if (z_high - z_low) > max_width:
                    z_high = z_low + max_width
                
                z_type = 'res' if c[0]['type'] == 'res' else 'sup'
                valid_zones.append({'low': z_low, 'high': z_high, 'type': z_type})
        
        return valid_zones

    def send_signal(self, symbol, direction, entry, zone, atr):
        if not self.telegram_bot: return
        sl = zone['high'] if direction == 'SHORT' else zone['low']
        risk = abs(entry - sl)
        if risk < atr * 0.2: risk = atr * 1.0 # حداقل فاصله مجاز
        
        tp = entry - (risk * 1.5) if direction == 'SHORT' else entry + (risk * 1.5)
        
        signal = {
            'symbol': symbol, 'type': direction, 'entry_price': entry,
            'stop_loss': sl, 'take_profit_1': tp, 'take_profit_2': 0,
            'strategy': 'V35_Structure'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
