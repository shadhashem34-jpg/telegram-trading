import asyncio
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()


class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.active_setups = {}

        # ---------- تنظیمات سطوح ----------
        self.LOOKBACK_CANDLES = 12       # ۲ روز = ۱۲ کندل ۴ ساعته
        self.MAX_LOOKBACK = 30           # اگر سطحی پیدا نشد تا اینجا گسترش بده
        self.ZONE_WIDTH_PCT = 0.0025     # حداکثر عرض زون 0.25%
        self.DROP_FORMING_CANDLE = 'auto'  # 'auto' | True | False

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V54 Started (Nearest Body Levels, closed candles)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)

            await asyncio.sleep(180)

    # ------------------------------------------------------------------
    # پیدا کردن نزدیک‌ترین مقاومت و حمایت (بدنه → سایه)
    # ------------------------------------------------------------------
    def _collect_candidates(self, o, h, l, c, price, lookback):
        O, H, L, C = o[-lookback:], h[-lookback:], l[-lookback:], c[-lookback:]
        n = len(O)
        res_c, sup_c = [], []

        # از کندل آخر به عقب می‌رویم. یک بدنه فقط وقتی سطح معتبر است که
        # هیچ کندل بعد از آن، بدنه‌ای بالاتر (برای مقاومت) یا پایین‌تر
        # (برای حمایت) نساخته باشد؛ یعنی زیر بدنهٔ کندل‌های بعدی گم نشده باشد.
        later_top = float('-inf')
        later_bot = float('inf')
        for i in range(n - 1, -1, -1):
            top = max(O[i], C[i])
            bot = min(O[i], C[i])

            if top > price and top >= later_top:
                res_c.append((top, H[i]))
            if bot < price and bot <= later_bot:
                sup_c.append((bot, L[i]))

            later_top = max(later_top, top)
            later_bot = min(later_bot, bot)

        return res_c, sup_c

    def _find_nearest_levels(self, h4, price):
        o, h, l, c = h4['opens'], h4['highs'], h4['lows'], h4['closes']
        # حذف کندل در حال تشکیل فقط وقتی که واقعاً ناقص است:
        # کندل ناقص Close آن تقریباً برابر قیمت جاری است.
        if self.DROP_FORMING_CANDLE == 'auto':
            if abs(c[-1] - price) <= price * 0.0002:
                o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]
        elif self.DROP_FORMING_CANDLE:
            o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]

        res_zone, sup_zone = None, None
        res_c, sup_c = [], []

        for lookback in (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK):
            r, s = self._collect_candidates(o, h, l, c, price, lookback)

            if res_zone is None and r:
                res_c = r
                body = min(x[0] for x in r)  # نزدیک‌ترین بالای قیمت
                wick = max(x[1] for x in r if x[0] == body)
                top = min(max(wick, body), body * (1 + self.ZONE_WIDTH_PCT))
                res_zone = {'low': body, 'high': top, 'type': 'res'}

            if sup_zone is None and s:
                sup_c = s
                body = max(x[0] for x in s)  # نزدیک‌ترین پایین قیمت
                wick = min(x[1] for x in s if x[0] == body)
                bottom = max(min(wick, body), body * (1 - self.ZONE_WIDTH_PCT))
                sup_zone = {'low': bottom, 'high': body, 'type': 'sup'}

            if res_zone and sup_zone:
                break

        return res_zone, sup_zone, res_c, sup_c

    # ------------------------------------------------------------------
    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]):
            return

        price = h4['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        res_zone, sup_zone, res_c, sup_c = self._find_nearest_levels(h4, price)
        if not res_zone or not sup_zone:
            logger.log_system_event(
                f"⚠️ {symbol}: level not found (res={bool(res_zone)}, sup={bool(sup_zone)})"
            )
            return

        last3 = [(round(h4['opens'][i], 2), round(h4['closes'][i], 2)) for i in (-3, -2, -1)]
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += f"   ↳ Last 3 candles (open,close): {last3}\n"
        log_msg += (f"   ↳ Res Zone: {res_zone['low']:.2f} - {res_zone['high']:.2f} "
                    f"| candidates(body): {sorted(round(x[0], 2) for x in res_c)}\n")
        log_msg += (f"   ↳ Sup Zone: {sup_zone['low']:.2f} - {sup_zone['high']:.2f} "
                    f"| candidates(body): {sorted((round(x[0], 2) for x in sup_c), reverse=True)}\n")
        logger.log_system_event(log_msg)

        # --- ماشین وضعیت (State Machine) ---
        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None, 'direction': None}

        state = self.active_setups[symbol]
        all_zones = [res_zone, sup_zone]

        if state['state'] == 'IDLE':
            for z in all_zones:
                if z['low'] <= price <= z['high']:
                    direction = 'SHORT' if z['type'] == 'res' else 'LONG'
                    state.update({'state': 'WAIT_1H_BREAK', 'zone': z, 'direction': direction})
                    logger.log_system_event(
                        f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone "
                        f"({z['low']:.2f}-{z['high']:.2f}). Waiting for 1H Confirmation..."
                    )
                    break

        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']
            h1_close = h1['closes'][-1]
            h1_open = h1['opens'][-1]
            h1_high = h1['highs'][-1]
            h1_low = h1['lows'][-1]

            if state['direction'] == 'SHORT':
                if h1_close < z['low']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res. Waiting for 15M Retest...")
                elif h1_high >= z['low'] and h1_close < z['low'] and h1_close < h1_open:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Res. Waiting for 15M Retest...")
                elif h1_close > z['high'] + (h4['atr'] * 0.5):
                    state.update({'state': 'IDLE', 'zone': None})

            elif state['direction'] == 'LONG':
                if h1_close > z['high']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup. Waiting for 15M Retest...")
                elif h1_low <= z['high'] and h1_close > z['high'] and h1_close > h1_open:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Sup. Waiting for 15M Retest...")
                elif h1_close < z['low'] - (h4['atr'] * 0.5):
                    state.update({'state': 'IDLE', 'zone': None})

        elif state['state'] == 'WAIT_15M_RETEST':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if state['direction'] == 'SHORT':
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry Triggered!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h4['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
            elif state['direction'] == 'LONG':
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry Triggered!")
                    self.send_signal(symbol, 'LONG', m15_close, z, h4['atr'])
                    state.update({'state': 'IDLE', 'zone': None})

    def send_signal(self, symbol, direction, entry, zone, atr):
        if not self.telegram_bot:
            return
        sl = zone['high'] if direction == 'SHORT' else zone['low']
        risk = abs(entry - sl)
        if risk < atr * 0.2:
            risk = atr * 1.0

        tp = entry - (risk * 1.5) if direction == 'SHORT' else entry + (risk * 1.5)

        signal = {
            'symbol': symbol, 'type': direction, 'entry_price': entry,
            'stop_loss': sl, 'take_profit_1': tp, 'take_profit_2': 0,
            'strategy': 'V54_Nearest_Body_Levels'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
