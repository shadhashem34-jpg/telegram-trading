import asyncio
from logger import get_trading_logger

logger = get_trading_logger()


# ======================= توابع تشخیص سطح =======================
ZONE_WIDTH = 0.0025
TOUCH_TOL = 0.0015


def calc_atr(h, l, c, period=14):
    n = len(c)
    trs = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, n)]
    trs = trs[-period:]
    return sum(trs) / len(trs) if trs else (h[-1] - l[-1])


def _touch_count(side, L, O, H, Lo, C, tol=TOUCH_TOL, min_gap=2):
    cnt, last = 0, -10**9
    for i in range(len(O)):
        top, bot = max(O[i], C[i]), min(O[i], C[i])
        if side == 'res':
            hit = H[i] >= L * (1 - tol) and top <= L * (1 + tol)
        else:
            hit = Lo[i] <= L * (1 + tol) and bot >= L * (1 - tol)
        if hit and i - last >= min_gap:
            cnt += 1
            last = i
    return cnt


def collect_candidates(o, h, l, c, price, trend, lookback=30):
    """لیست کاندیدها با متادیتا: level, side, idx, age, body_atr, wick, touches"""
    n = min(len(o), len(h), len(l), len(c), lookback)
    O, H, Lo, C = list(o)[-n:], list(h)[-n:], list(l)[-n:], list(c)[-n:]
    atr = calc_atr(H, Lo, C)
    tops = [max(O[i], C[i]) for i in range(n)]
    bots = [min(O[i], C[i]) for i in range(n)]
    eps = price * 0.00001

    def inside_left(level, idx):
        return any(bots[j] + eps < level < tops[j] - eps for j in range(idx))

    raw = []
    later_top, later_bot = float('-inf'), float('inf')
    for i in range(n - 1, -1, -1):
        body_atr = abs(C[i] - O[i]) / atr if atr else 0.0
        if tops[i] > price and tops[i] >= later_top:
            if not (trend == "UPTREND" and inside_left(tops[i], i)):
                raw.append(dict(level=tops[i], side='res', idx=i, age=n - 1 - i,
                                body_atr=body_atr, wick=H[i]))
        if bots[i] < price and bots[i] <= later_bot:
            if not (trend == "DOWNTREND" and inside_left(bots[i], i)):
                raw.append(dict(level=bots[i], side='sup', idx=i, age=n - 1 - i,
                                body_atr=body_atr, wick=Lo[i]))
        later_top = max(later_top, tops[i])
        later_bot = min(later_bot, bots[i])

    # ادغام سطوح یکسان (close کندل i == open کندل i+1): بزرگ‌ترین بدنه و قدیمی‌ترین سن
    merged = {}
    for r in raw:
        key = (r['side'], round(r['level'], 8))
        m = merged.get(key)
        if m is None:
            merged[key] = dict(r)
        else:
            m['body_atr'] = max(m['body_atr'], r['body_atr'])
            m['age'] = max(m['age'], r['age'])
            m['idx'] = min(m['idx'], r['idx'])
            m['wick'] = max(m['wick'], r['wick']) if r['side'] == 'res' else min(m['wick'], r['wick'])
    cands = list(merged.values())
    for m in cands:
        m['touches'] = _touch_count(m['side'], m['level'], O, H, Lo, C)
    return cands, atr


def select_level(cands, side, price, rule='impulse', param=0.8):
    pool = [x for x in cands if x['side'] == side]
    if rule == 'impulse':
        pool = [x for x in pool if x['body_atr'] >= param]
    elif rule == 'aged':
        pool = [x for x in pool if x['age'] >= param]
    elif rule == 'touches':
        pool = [x for x in pool if x['touches'] >= param]
    elif rule == 'mindist':
        pool = [x for x in pool if abs(x['level'] - price) / price * 100 >= param]
    elif rule != 'nearest':
        raise ValueError(rule)
    if not pool:
        return None
    return min(pool, key=lambda x: abs(x['level'] - price))


def to_zone(x, width=ZONE_WIDTH):
    L = x['level']
    if x['side'] == 'res':
        return {'low': L, 'high': min(max(x['wick'], L), L * (1 + width)), 'type': 'res',
                'body_atr': x['body_atr'], 'age': x['age'], 'touches': x['touches']}
    return {'low': max(min(x['wick'], L), L * (1 - width)), 'high': L, 'type': 'sup',
            'body_atr': x['body_atr'], 'age': x['age'], 'touches': x['touches']}


def find_levels_manual(o, h, l, c, price, trend, rule='impulse', param=0.8, lookback=30):
    """جایگزین _find_nearest_levels: برمی‌گرداند (res_zone, sup_zone)"""
    cands, _ = collect_candidates(o, h, l, c, price, trend, lookback)
    r = select_level(cands, 'res', price, rule, param)
    s = select_level(cands, 'sup', price, rule, param)
    return (to_zone(r) if r else None), (to_zone(s) if s else None)


# ======================= کلاس استراتژی =======================
class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.active_setups = {}

        # ---------- تنظیمات سطوح ----------
        self.LOOKBACK = 30                # تعداد کندل ۴ساعته برای اسکن
        self.ZONE_WIDTH_PCT = 0.0025      # عرض زون ۰.۲۵٪
        # قانون انتخاب مقاومت/حمایت (باید با verify_levels.py تأیید بشه):
        #   'nearest' | 'mindist' | 'impulse' | 'aged' | 'touches'
        # mindist: نزدیک‌ترین سطح که حداقل ٪ فاصله از قیمت دارد (روی ۸ سطح تو ۸/۸ جواب داد، ولی فقط ۴ نمونه‌ست)
        self.LEVEL_RULE = 'mindist'
        self.LEVEL_RULE_PARAM = 0.9       # mindist: درصد | impulse: ضریب ATR | aged: کندل | touches: لمس
        # وقتی قیمت به زونِ انتخاب‌شده نزدیک‌تر از این درصد شد، زون قفل می‌شه تا ماشین حالت بتونه کار کنه
        self.FREEZE_NEAR_PCT = 0.9
        self.armed = {}
        self.FALLBACK_TO_NEAREST = True   # اگه هیچ سطحی قانون رو پاس نکرد، نزدیک‌ترین
        self.SYMBOLS = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

    def set_market_data_handler(self, handler):
        self.market_data = handler

    # ------------------------------------------------------------------
    async def start_monitoring(self):
        self.running = True
        logger.log_system_event(
            f"🚀 V60 Started (rule={self.LEVEL_RULE}:{self.LEVEL_RULE_PARAM}, {self.LOOKBACK} candles)"
        )
        while self.running:
            for symbol in self.SYMBOLS:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            await asyncio.sleep(180)

    # ------------------------------------------------------------------
    @staticmethod
    def _fmt_cands(cands, side, price):
        xs = sorted((x for x in cands if x['side'] == side), key=lambda x: abs(x['level'] - price))
        return " ; ".join(
            f"{x['level']:.2f}|{x['age']}|{x['body_atr']:.2f}|{x['touches']}" for x in xs
        ) or "-"

    @staticmethod
    def _dist_pct(z, price):
        if z['low'] <= price <= z['high']:
            return 0.0
        edge = z['low'] if price < z['low'] else z['high']
        return abs(edge - price) / price * 100

    def _still_valid(self, z, price, atr):
        if z['type'] == 'res':
            return price <= z['high'] + 0.5 * atr
        return price >= z['low'] - 0.5 * atr

    def _find_levels(self, symbol, h4, price, trend):
        """فرض: h4 فقط کندل‌های بسته‌شده رو می‌ده (کندل در حال تشکیل داخلش نیست)."""
        o, h, l, c = (list(h4[k]) for k in ('opens', 'highs', 'lows', 'closes'))
        n = min(len(o), len(h), len(l), len(c))
        if n < 5:
            return None, None, []
        o, h, l, c = o[-n:], h[-n:], l[-n:], c[-n:]

        cands, _ = collect_candidates(o, h, l, c, price, trend, self.LOOKBACK)

        picked = {}
        for side in ('res', 'sup'):
            x = select_level(cands, side, price, self.LEVEL_RULE, self.LEVEL_RULE_PARAM)
            if x is None and self.FALLBACK_TO_NEAREST:
                x = select_level(cands, side, price, 'nearest')
                if x:
                    x = dict(x, fallback=True)
            picked[side] = x

        res_zone = to_zone(picked['res'], self.ZONE_WIDTH_PCT) if picked['res'] else None
        sup_zone = to_zone(picked['sup'], self.ZONE_WIDTH_PCT) if picked['sup'] else None
        for z, x in ((res_zone, picked['res']), (sup_zone, picked['sup'])):
            if z:
                z['fallback'] = bool(x.get('fallback'))

        # قفل زون: وقتی قیمت به زونِ قبلی نزدیک شد (یا واردش شد) همون رو نگه دار،
        # وگرنه قانون mindist زونِ نزدیک رو کنار می‌ذاره و قیمت هیچ‌وقت «داخل زون» نمی‌شه
        armed = self.armed.setdefault(symbol, {'res': None, 'sup': None})
        out = {}
        for side, fresh in (('res', res_zone), ('sup', sup_zone)):
            old = armed[side]
            if (old and self._still_valid(old, price, h4['atr'])
                    and self._dist_pct(old, price) < self.FREEZE_NEAR_PCT):
                out[side] = dict(old, frozen=True)
            else:
                out[side] = fresh
            armed[side] = out[side]
        return out['res'], out['sup'], cands

    # ------------------------------------------------------------------
    @staticmethod
    def _fmt_zone(z):
        tag = (" (fallback)" if z.get('fallback') else "") + (" (frozen)" if z.get('frozen') else "")
        return (f"{z['low']:.2f} - {z['high']:.2f} | body/ATR={z['body_atr']:.2f} "
                f"age={z['age']} touches={z['touches']}{tag}")

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")
        if not all([h4, h1, m15]):
            return

        price = h4['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        res_zone, sup_zone, cands = self._find_levels(symbol, h4, price, trend)
        if not res_zone or not sup_zone:
            logger.log_system_event(
                f"⚠️ {symbol}: level not found (res={bool(res_zone)}, sup={bool(sup_zone)})"
            )
            return

        logger.log_system_event(
            f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
            f"   ↳ Res Zone: {self._fmt_zone(res_zone)}\n"
            f"   ↳ Sup Zone: {self._fmt_zone(sup_zone)}\n"
            f"   ↳ RES cands (level|age|body/ATR|touches): {self._fmt_cands(cands, 'res', price)}\n"
            f"   ↳ SUP cands (level|age|body/ATR|touches): {self._fmt_cands(cands, 'sup', price)}"
        )

        state = self.active_setups.setdefault(
            symbol, {'state': 'IDLE', 'zone': None, 'direction': None}
        )

        h1_open, h1_high = h1['opens'][-1], h1['highs'][-1]
        h1_low, h1_close = h1['lows'][-1], h1['closes'][-1]

        if state['state'] == 'IDLE':
            self._handle_idle(symbol, state, price, res_zone, sup_zone, h1)
        elif state['state'] == 'WAIT_1H_BREAK':
            self._handle_wait_1h(symbol, state, h4, h1_open, h1_high, h1_low, h1_close)
        elif state['state'] == 'WAIT_15M_RETEST':
            self._handle_wait_15m(symbol, state, h4, m15)

    # ---------------------------- IDLE ---------------------------------
    def _handle_idle(self, symbol, state, price, res_zone, sup_zone, h1):
        h1_close, h1_open = h1['closes'][-1], h1['opens'][-1]

        # شکست سریع مقاومت به پایین
        if h1_close < res_zone['low'] and h1_close < h1_open and h1['highs'][-2] >= res_zone['low']:
            state.update(state='WAIT_15M_RETEST', zone=res_zone, direction='SHORT')
            logger.log_system_event(f"⚡ [{symbol}] Fast 1H Breakdown below Res! Waiting for 15M Retest...")
            return

        # شکست سریع حمایت به بالا
        if h1_close > sup_zone['high'] and h1_close > h1_open and h1['lows'][-2] <= sup_zone['high']:
            state.update(state='WAIT_15M_RETEST', zone=sup_zone, direction='LONG')
            logger.log_system_event(f"⚡ [{symbol}] Fast 1H Breakout above Sup! Waiting for 15M Retest...")
            return

        # ورود قیمت به زون
        for z in (res_zone, sup_zone):
            if z['low'] <= price <= z['high']:
                direction = 'SHORT' if z['type'] == 'res' else 'LONG'
                state.update(state='WAIT_1H_BREAK', zone=z, direction=direction)
                logger.log_system_event(
                    f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone. Waiting for 1H Confirmation..."
                )
                return

    # ------------------------- WAIT_1H_BREAK ----------------------------
    def _handle_wait_1h(self, symbol, state, h4, h1_open, h1_high, h1_low, h1_close):
        z, atr = state['zone'], h4['atr']

        if state['direction'] == 'SHORT':
            if h1_close < z['low']:
                state['state'] = 'WAIT_15M_RETEST'
                logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res. Waiting for 15M Retest...")
            elif h1_high >= z['low'] and h1_close < z['low'] and h1_close < h1_open:
                state['state'] = 'WAIT_15M_RETEST'
                logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Res. Waiting for 15M Retest...")
            elif h1_close > z['high'] + atr * 0.5:
                state.update(state='IDLE', zone=None, direction=None)

        elif state['direction'] == 'LONG':
            if h1_close > z['high']:
                state['state'] = 'WAIT_15M_RETEST'
                logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup. Waiting for 15M Retest...")
            elif h1_low <= z['high'] and h1_close > z['high'] and h1_close > h1_open:
                state['state'] = 'WAIT_15M_RETEST'
                logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Sup. Waiting for 15M Retest...")
            elif h1_close < z['low'] - atr * 0.5:
                state.update(state='IDLE', zone=None, direction=None)

    # ------------------------ WAIT_15M_RETEST ---------------------------
    def _handle_wait_15m(self, symbol, state, h4, m15):
        z = state['zone']
        c, h, l, o = m15['closes'][-1], m15['highs'][-1], m15['lows'][-1], m15['opens'][-1]

        if state['direction'] == 'SHORT' and h >= z['low'] and c < z['low'] and c < o:
            logger.log_system_event(f"🏆 [{symbol}] SHORT Entry Triggered!")
            self.send_signal(symbol, 'SHORT', c, z, h4['atr'])
            state.update(state='IDLE', zone=None, direction=None)

        elif state['direction'] == 'LONG' and l <= z['high'] and c > z['high'] and c > o:
            logger.log_system_event(f"🏆 [{symbol}] LONG Entry Triggered!")
            self.send_signal(symbol, 'LONG', c, z, h4['atr'])
            state.update(state='IDLE', zone=None, direction=None)

    # --------------------------- SIGNAL ---------------------------------
    def send_signal(self, symbol, direction, entry, zone, atr):
        if not self.telegram_bot:
            return

        sl = zone['high'] if direction == 'SHORT' else zone['low']
        risk = abs(entry - sl)
        if risk < atr * 0.2:
            risk = atr * 1.0

        tp = entry - risk * 1.5 if direction == 'SHORT' else entry + risk * 1.5

        signal = {
            'symbol': symbol,
            'type': direction,
            'entry_price': entry,
            'stop_loss': sl,
            'take_profit_1': tp,
            'take_profit_2': 0,
            'strategy': f'V60_{self.LEVEL_RULE}',
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
