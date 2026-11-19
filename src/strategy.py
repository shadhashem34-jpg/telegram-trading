import asyncio
from logger import get_trading_logger

logger = get_trading_logger()

class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.active_setups = {}

        # تنظیمات ثابت استراتژی
        self.LOOKBACK_CANDLES = 12
        self.MAX_LOOKBACK = 30
        self.ZONE_WIDTH_PCT = 0.0025
        self.DROP_FORMING_CANDLE = 'auto'

        self.last_signal_h1 = {}
        self.last_signal_15m = {}

        # 🧠 حافظه سطوح ثبت‌شده (شکست‌خورده)
        self.recorded_levels = {}

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V67 Started (Pure 4H Levels + Instant 1H Confirmation)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            await asyncio.sleep(180)

    # ==========================================================
    #  منطق سطح‌یابی (کاملاً دست‌نخورده و اصلی - فقط ۴ ساعته)
    # ==========================================================
    def _collect_candidates(self, o, h, l, c, price, lookback, trend):
        O, H, L, C = o[-lookback:], h[-lookback:], l[-lookback:], c[-lookback:]
        n = len(O)
        res_c, sup_c = [], []
        later_top = float('-inf')
        later_bot = float('inf')
        tops = [max(O[i], C[i]) for i in range(n)]
        bots = [min(O[i], C[i]) for i in range(n)]
        eps = price * 0.00001

        check_left_res = (trend == "UPTREND")
        check_left_sup = (trend == "DOWNTREND")

        def inside_left_body(level, idx):
            for j in range(idx):
                if bots[j] + eps < level < tops[j] - eps:
                    return True
            return False

        for i in range(n - 1, -1, -1):
            top = tops[i]
            bot = bots[i]

            if top > price and top >= later_top:
                if not (check_left_res and inside_left_body(top, i)):
                    res_c.append((top, H[i]))
            if bot < price and bot <= later_bot:
                if not (check_left_sup and inside_left_body(bot, i)):
                    sup_c.append((bot, L[i]))

            later_top = max(later_top, top)
            later_bot = min(later_bot, bot)

        return res_c, sup_c

    def _levels_from_candles(self, data, price, trend, drop_forming):
        o, h, l, c = data['opens'], data['highs'], data['lows'], data['closes']
        if drop_forming == 'auto':
            if abs(c[-1] - price) <= price * 0.0002:
                o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]
        elif drop_forming:
            o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]

        res_zone, sup_zone = None, None
        res_c, sup_c = [], []

        for lookback in (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK):
            r, s = self._collect_candidates(o, h, l, c, price, lookback, trend)

            if res_zone is None and r:
                res_c = r
                body = max(x[0] for x in r)
                wick = max(x[1] for x in r if x[0] == body)
                top = min(max(wick, body), body * (1 + self.ZONE_WIDTH_PCT))
                res_zone = {'low': body, 'high': top, 'type': 'res'}

            if sup_zone is None and s:
                sup_c = s
                body = max(x[0] for x in s)
                wick = min(x[1] for x in s if x[0] == body)
                bottom = max(min(wick, body), body * (1 - self.ZONE_WIDTH_PCT))
                sup_zone = {'low': bottom, 'high': body, 'type': 'sup'}

            if res_zone and sup_zone:
                break

        return res_zone, sup_zone, res_c, sup_c

    def _find_nearest_levels(self, h4, price, trend):
        # فقط ۴ ساعته
        return self._levels_from_candles(h4, price, trend, self.DROP_FORMING_CANDLE)

    # ==========================================================
    #  ثبت آنی شکست
    # ==========================================================
    def _register_breaks(self, symbol, res_zone, sup_zone, price):
        if res_zone and price > res_zone['high']:
            self.recorded_levels[symbol] = {'zone': res_zone, 'type': 'LONG'}
            logger.log_system_event(f"🧠 [{symbol}] Breakout recorded above {res_zone['high']:.2f}")
        elif sup_zone and price < sup_zone['low']:
            self.recorded_levels[symbol] = {'zone': sup_zone, 'type': 'SHORT'}
            logger.log_system_event(f"🧠 [{symbol}] Breakdown recorded below {sup_zone['low']:.2f}")

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]):
            logger.log_system_event(f"❌ {symbol}: داده‌ی تایم‌فریم ناقص است")
            return

        price = h4['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        # ۱. پیدا کردن سطوح ۴ ساعته
        res_zone, sup_zone, res_c, sup_c = self._find_nearest_levels(h4, price, trend)
        
        # ۲. ثبت آنی شکست‌ها
        self._register_breaks(symbol, res_zone, sup_zone, price)

        if not res_zone or not sup_zone:
            logger.log_system_event(f"⚠️ {symbol}: level not found (res={bool(res_zone)}, sup={bool(sup_zone)}) | price={price:.4f}")
            return

        # 📊 لاگ سطوح
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += (f"   ↳ Res Zone: {res_zone['low']:.2f} - {res_zone['high']:.2f}\n")
        log_msg += (f"   ↳ Sup Zone: {sup_zone['low']:.2f} - {sup_zone['high']:.2f}\n")
        logger.log_system_event(log_msg)

        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None, 'direction': None}

        state = self.active_setups[symbol]
        
        h1_close = h1['closes'][-1]
        h1_open = h1['opens'][-1]
        h1_high = h1['highs'][-1]
        h1_low = h1['lows'][-1]
        h1_time = h1['times'][-1]
        m15_time = m15['times'][-1]

        # اگر در حافظه سطح ثبت‌شده داریم و ربات در حالت IDLE است
        if state['state'] == 'IDLE' and symbol in self.recorded_levels:
            mem = self.recorded_levels[symbol]
            z = mem['zone']
            
            # گیت روند: جهت فقط از ۴ ساعته
            if mem['type'] == 'SHORT' and trend == "DOWNTREND":
                state.update({'state': 'WAIT_1H_BREAK', 'zone': z, 'direction': 'SHORT'})
                logger.log_system_event(f"🔔 [{symbol}] Waiting for 1H close below {z['low']:.2f}...")
            elif mem['type'] == 'LONG' and trend == "UPTREND":
                state.update({'state': 'WAIT_1H_BREAK', 'zone': z, 'direction': 'LONG'})
                logger.log_system_event(f"🔔 [{symbol}] Waiting for 1H close above {z['high']:.2f}...")

        # منتظر تایید کندل ۱ ساعته
        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']
            if state['direction'] == 'SHORT':
                if h1_close < z['low']: 
                    state.update({'state': 'WAIT_15M_RETEST'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below level. Waiting for 15M Retest...")
            elif state['direction'] == 'LONG':
                if h1_close > z['high']: 
                    state.update({'state': 'WAIT_15M_RETEST'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above level. Waiting for 15M Retest...")

        # منتظر پولبک ۱۵ دقیقه
        elif state['state'] == 'WAIT_15M_RETEST':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            triggered = False
            if state['direction'] == 'SHORT':
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    triggered = True
            elif state['direction'] == 'LONG':
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    triggered = True

            if triggered:
                if (self.last_signal_h1.get(symbol) == h1_time or
                        self.last_signal_15m.get(symbol) == m15_time):
                    return

                logger.log_system_event(f"🏆 [{symbol}] {state['direction']} Entry Triggered!")
                self.send_signal(symbol, state['direction'], m15_close, z, h4['atr'], m15_high, m15_low)
                self.last_signal_h1[symbol] = h1_time
                self.last_signal_15m[symbol] = m15_time
                
                state.update({'state': 'IDLE', 'zone': None})
                if symbol in self.recorded_levels:
                    del self.recorded_levels[symbol]

    def send_signal(self, symbol, direction, entry, zone, atr, m15_high, m15_low):
        if not self.telegram_bot:
            return
            
        if direction == 'SHORT':
            sl = max(zone['high'], m15_high)
        else:
            sl = min(zone['low'], m15_low)
            
        risk = abs(entry - sl)
        if risk < atr * 0.2:
            risk = atr * 1.0
        tp = entry - (risk * 1.5) if direction == 'SHORT' else entry + (risk * 1.5)

        signal = {
            'symbol': symbol, 'type': direction, 'entry_price': entry,
            'stop_loss': sl, 'take_profit_1': tp, 'take_profit_2': 0,
            'strategy': 'V67_Instant_Record'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
