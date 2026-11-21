import asyncio
import time
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

TAG = "V72"


def _log(msg):
    logger.log_system_event(msg)


def _fmt(p):
    if p is None:
        return "—"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.3f}"
    return f"{p:.6f}"


def _ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%m-%d %H:%M')


def _tail_candles(tf, now_ms, tail=40):
    """کندل‌های بسته‌شده: (open_ms, close_ms, o, h, l, c) — فقط tail تای آخر."""
    ms = tf.get('ms') or 60_000
    n = len(tf['times'])
    out = []
    for i in range(max(0, n - tail), n):
        cm = tf['times'][i] + ms
        if cm <= now_ms:
            out.append((tf['times'][i], cm, tf['opens'][i], tf['highs'][i], tf['lows'][i], tf['closes'][i]))
    return out


class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None

        # ---------- تنظیمات سطح‌یابی (دست‌نخورده) ----------
        self.LOOKBACK_CANDLES = 12
        self.MAX_LOOKBACK = 30
        self.WEEKLY_LOOKBACK = 60          # اگر ۴ساعته و روزانه سطح پیدا نکردند، ۶۰ کندل هفتگی
        self.ZONE_WIDTH_PCT = 0.0025
        self.DROP_FORMING_CANDLE = 'auto'

        # ---------- تنظیمات ستاپ و سیگنال ----------
        self.RETEST_EXPIRE_H = 12          # مهلت پولبک بعد از «شکست» سطح (ساعت)
        self.REJECT_EXPIRE_H = 8           # مهلت سیگنال بعد از «رد شدن» از سطح (ساعت)
        self.ZONE_COOLDOWN_H = 3           # بعد از سیگنال/انقضا همان زون تا این‌قدر دوباره فعال نشود
        self.MAX_DRIFT_ATR15 = 1.5         # اگر قیمت زنده از ورود بیشتر از این‌قدر ATR(15m) دور شد، رد شود
        self.RR = 1.5                      # نسبت ریوارد به ریسک

        self.setups = {}                   # symbol -> ستاپ فعال (فقط یکی برای هر نماد)
        self.active_setups = self.setups   # نام قدیمی (سازگاری)
        self.last_zones = {}               # symbol -> زون‌های اسکن قبلی (برای تشخیص رویداد)
        self.blocked = {}                  # symbol -> [(low, high, until_ms)]
        self.last_signal_15m = {}

    def set_market_data_handler(self, handler):
        self.market_data = handler

    def _now_ms(self):
        return int(time.time() * 1000)

    async def start_monitoring(self):
        self.running = True
        _log(f"🚀 {TAG} Started (Trend > Levels > Touch/Break > 1H > 15M)")
        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    _log(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            await asyncio.sleep(180)

    async def stop(self):
        self.running = False
        _log("StrategyManager stopped")

    # ==========================================================
    # منطق سطح‌یابی (دست‌نخورده؛ فقط فال‌بک هفتگی اضافه شده)
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

    def _levels_from_candles(self, data, price, trend, drop_forming, lookbacks=None):
        o, h, l, c = data['opens'], data['highs'], data['lows'], data['closes']

        if drop_forming == 'auto':
            if abs(c[-1] - price) <= price * 0.0002:
                o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]
        elif drop_forming:
            o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]

        res_zone, sup_zone = None, None
        res_c, sup_c = [], []

        for lookback in (lookbacks or (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK)):
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

    def _find_levels(self, h4, h1d, h1w, price, trend):
        # ۱. اول ۴ ساعته
        res_zone, sup_zone, res_c, sup_c = self._levels_from_candles(h4, price, trend, self.DROP_FORMING_CANDLE)
        source_res = "4H" if res_zone else None
        source_sup = "4H" if sup_zone else None

        # ۲. اگر ۴ ساعته کور بود، روزانه
        if h1d is not None and (res_zone is None or sup_zone is None):
            r1d, s1d, r1dc, s1dc = self._levels_from_candles(h1d, price, trend, drop_forming=False)
            if res_zone is None and r1d:
                res_zone, res_c, source_res = r1d, r1dc, "1D"
            if sup_zone is None and s1d:
                sup_zone, sup_c, source_sup = s1d, s1dc, "1D"

        # ۳. اگر باز هم کور بود (مثلاً قیمت در پایین‌ترین سطوح است)، ۶۰ کندل هفتگی
        if h1w is not None and (res_zone is None or sup_zone is None):
            lbs = (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK, self.WEEKLY_LOOKBACK)
            rw, sw, rwc, swc = self._levels_from_candles(h1w, price, trend, False, lbs)
            if res_zone is None and rw:
                res_zone, res_c, source_res = rw, rwc, "1W"
            if sup_zone is None and sw:
                sup_zone, sup_c, source_sup = sw, swc, "1W"

        return res_zone, sup_zone, res_c, sup_c, source_res, source_sup

    # ==========================================================
    # رویدادها: لمس / شکست سطح
    # ==========================================================
    @staticmethod
    def _overlap(a, b, tol):
        return not (a['high'] < b['low'] - tol or a['low'] > b['high'] + tol)

    def _is_blocked(self, symbol, z, now_ms):
        lst = [b for b in self.blocked.get(symbol, []) if b[2] > now_ms]
        self.blocked[symbol] = lst
        tol = z['high'] * 0.001
        return any(not (z['high'] < b[0] - tol or z['low'] > b[1] + tol) for b in lst)

    def _block(self, symbol, z, now_ms):
        self.blocked.setdefault(symbol, []).append((z['low'], z['high'], now_ms + self.ZONE_COOLDOWN_H * 3_600_000))

    def _detect_event(self, symbol, prev, m15, price, now_ms):
        """اولین لمس/شکست زون‌های اسکن قبلی: با کندل‌های ۱۵دقیقه‌ی بسته‌شده از آن زمان + قیمت زنده."""
        zones = [('RES', prev.get('res')), ('SUP', prev.get('sup'))]
        zones = [(side, z) for side, z in zones if z and not self._is_blocked(symbol, z, now_ms)]
        if not zones:
            return None

        for (t, cm, o, h, l, c) in _tail_candles(m15, now_ms, 30):
            if cm <= prev['ms']:
                continue
            for side, z in zones:
                if side == 'RES' and h >= z['low']:
                    return side, z, t
                if side == 'SUP' and l <= z['high']:
                    return side, z, t

        for side, z in zones:
            if side == 'RES' and price >= z['low']:
                return side, z, now_ms
            if side == 'SUP' and price <= z['high']:
                return side, z, now_ms
        return None

    @staticmethod
    def _beyond(role, z, close):
        """کلوز ۱ساعته بیرون زون (بدنه رد شده)؟"""
        return (role == 'RES' and close > z['high']) or (role == 'SUP' and close < z['low'])

    @staticmethod
    def _zs(z):
        return f"{_fmt(z['low'])}–{_fmt(z['high'])}"

    # ==========================================================
    # ماشین حالت: WAIT_1H  ->  ARMED  ->  سیگنال
    # ==========================================================
    def _advance(self, symbol, st, trend, price, h1c, m15c, atr15, atr4h, now_ms):
        z = st['zone']

        # ---- تایید ۱ساعته: شکست با بدنه یا فقط لمس با سایه؟ ----
        if st['state'] == 'WAIT_1H':
            after = [c for c in h1c if c[1] > st['event_ms']]
            if not after:
                return None
            broke = next((c for c in after if self._beyond(st['role'], z, c[5])), None)
            if broke:
                old = st['role']
                st['role'] = 'SUP' if old == 'RES' else 'RES'
                st.update(state='ARMED', kind='RETEST', since_ms=broke[1], checked_ms=broke[1])
                _log(f"💥 [{symbol}] 1H BODY BREAK ({_fmt(broke[5])}) | زون {self._zs(z)}: {old}→{st['role']} | "
                     f"منتظر پولبک ۱۵دقیقه تا {self.RETEST_EXPIRE_H} ساعت")
            else:
                st.update(state='ARMED', kind='REJECT', since_ms=st['event_ms'], checked_ms=st['event_ms'])
                _log(f"↩️ [{symbol}] فقط لمس با سایه (کلوز ۱ساعته بیرون زون {self._zs(z)} نرفت) | "
                     f"منتظر کندل ۱۵دقیقه‌ی رد شدن تا {self.REJECT_EXPIRE_H} ساعت")

        if st['state'] != 'ARMED':
            return None

        # ---- شکست بعدی ۱ساعته علیه ستاپ: نقش زون عوض می‌شود ----
        for c in h1c:
            if c[1] > st['since_ms'] and self._beyond(st['role'], z, c[5]):
                old = st['role']
                st['role'] = 'SUP' if old == 'RES' else 'RES'
                st.update(kind='RETEST', since_ms=c[1], checked_ms=c[1])
                _log(f"💥 [{symbol}] شکست ۱ساعته ({_fmt(c[5])}) | زون {self._zs(z)}: {old}→{st['role']} | منتظر پولبک")

        # ---- انقضا ----
        hours = self.RETEST_EXPIRE_H if st['kind'] == 'RETEST' else self.REJECT_EXPIRE_H
        if now_ms - st['since_ms'] > hours * 3_600_000:
            _log(f"⌛ [{symbol}] ستاپ {st['kind']} روی زون {self._zs(z)} بدون سیگنال منقضی شد")
            self._block(symbol, z, now_ms)
            st['state'] = 'DEAD'
            return None

        # ---- تریگر ۱۵دقیقه: سایه به زون + کلوز بیرون زون + جهت کندل ----
        trig = None
        last_cm = st['checked_ms']
        for c in m15c:
            if c[1] <= max(st['since_ms'], st['checked_ms']):
                continue
            last_cm = max(last_cm, c[1])
            _, _, o, h, l, cl = c
            if st['role'] == 'RES' and h >= z['low'] and cl < z['low'] and cl < o:
                trig = c
            if st['role'] == 'SUP' and l <= z['high'] and cl > z['high'] and cl > o:
                trig = c
        st['checked_ms'] = last_cm
        if not trig:
            return None

        direction = 'SHORT' if st['role'] == 'RES' else 'LONG'
        if (direction == 'SHORT' and trend != 'DOWNTREND') or (direction == 'LONG' and trend != 'UPTREND'):
            _log(f"🚫 [{symbol}] تریگر {direction} روی زون {self._zs(z)} دیده شد ولی خلاف روند ({trend}) است؛ رد شد")
            return None

        entry = trig[5]
        if atr15 and abs(price - entry) > self.MAX_DRIFT_ATR15 * atr15:
            _log(f"⏭️ [{symbol}] تریگر {direction} دیده شد ولی قیمت از ورود ({_fmt(entry)}) دور شده ({_fmt(price)})؛ فرصت از دست رفت")
            return None

        if self.last_signal_15m.get(symbol) == trig[0]:
            return None
        self.last_signal_15m[symbol] = trig[0]

        _log(f"🏆 [{symbol}] {direction} Entry Triggered! ({'پولبک بعد از شکست' if st['kind'] == 'RETEST' else 'رد شدن از سطح'}) "
             f"| کندل ۱۵دقیقه {_ts(trig[0])} | زون {self._zs(z)}")
        self.send_signal(symbol, direction, entry, z, atr4h, trig[3], trig[4])
        self._block(symbol, z, now_ms)
        st['state'] = 'DEAD'
        return True

    # ==========================================================
    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")
        h1d = await self.market_data.get_tf_data(symbol, "D")
        h1w = await self.market_data.get_tf_data(symbol, "W")

        if not all([h4, h1, m15]):
            _log(f"⚠️ {symbol}: data missing (4H={bool(h4)}, 1H={bool(h1)}, 15m={bool(m15)})")
            return

        now_ms = self._now_ms()
        price = h4['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        res_zone, sup_zone, _, _, source_res, source_sup = self._find_levels(h4, h1d, h1w, price, trend)

        # --- رویداد جدید (فقط اگر ستاپ فعالی نیست) ---
        st = self.setups.get(symbol)
        if st is None:
            prev = self.last_zones.get(symbol)
            if prev:
                ev = self._detect_event(symbol, prev, m15, price, now_ms)
                if ev:
                    side, zone, ev_ms = ev
                    st = {'state': 'WAIT_1H', 'role': side, 'zone': dict(zone), 'event_ms': ev_ms,
                          'kind': None, 'since_ms': ev_ms, 'checked_ms': ev_ms}
                    self.setups[symbol] = st
                    _log(f"🎯 [{symbol}] TOUCH/BREAK سطح {'مقاومت' if side == 'RES' else 'حمایت'} {self._zs(zone)} "
                         f"در {_ts(ev_ms)} | قیمت {_fmt(price)} | منتظر کلوز ۱ساعته")

        # --- پیشبرد ستاپ ---
        if st is not None:
            h1c = _tail_candles(h1, now_ms, 40)
            m15c = _tail_candles(m15, now_ms, 40)
            self._advance(symbol, st, trend, price, h1c, m15c, m15['atr'], h4['atr'], now_ms)
            if st['state'] == 'DEAD':
                self.setups.pop(symbol, None)

        # --- ذخیره‌ی زون‌ها برای اسکن بعد (فقط وقتی ستاپی فعال نیست) ---
        if symbol not in self.setups:
            self.last_zones[symbol] = {
                'res': dict(res_zone) if res_zone else None,
                'sup': dict(sup_zone) if sup_zone else None,
                'ms': now_ms,
            }

        # --- لاگ وضعیت ---
        st = self.setups.get(symbol)
        res_t = f"{self._zs(res_zone)} [{source_res}]" if res_zone else "پیدا نشد"
        sup_t = f"{self._zs(sup_zone)} [{source_sup}]" if sup_zone else "پیدا نشد"
        if st:
            z = st['zone']
            tail15 = _tail_candles(m15, now_ms, 1)
            last15 = tail15[-1] if tail15 else None
            need = ""
            if st['state'] == 'ARMED' and last15:
                if st['role'] == 'RES':
                    need = f" | نیاز: high≥{_fmt(z['low'])} و close<{_fmt(z['low'])} و کندل نزولی | آخرین ۱۵م: H{_fmt(last15[3])} C{_fmt(last15[5])}"
                else:
                    need = f" | نیاز: low≤{_fmt(z['high'])} و close>{_fmt(z['high'])} و کندل صعودی | آخرین ۱۵م: L{_fmt(last15[4])} C{_fmt(last15[5])}"
            setup_t = f"ستاپ: {st['state']}/{st['kind'] or 'EVENT'} نقش‌زون={st['role']} {self._zs(z)}{need}"
        else:
            setup_t = "ستاپ فعالی نیست"
        _log(f"📊 {symbol} — {trend} | قیمت {_fmt(price)} | مقاومت {res_t} | حمایت {sup_t} | {setup_t}")

    # ==========================================================
    def send_signal(self, symbol, direction, entry, zone, atr, m15_high, m15_low):
        if direction == 'SHORT':
            sl = max(zone['high'], m15_high)
        else:
            sl = min(zone['low'], m15_low)

        risk = abs(entry - sl)
        if risk < atr * 0.2:
            risk = atr * 1.0
            sl = entry + risk if direction == 'SHORT' else entry - risk   # حد ضرر هم‌اندازه‌ی ریسک

        tp = entry - risk * self.RR if direction == 'SHORT' else entry + risk * self.RR

        signal = {
            'symbol': symbol,
            'type': 'SELL' if direction == 'SHORT' else 'BUY',
            'direction': direction,
            'entry_price': entry,
            'stop_loss': round(sl, 4),
            'take_profit_1': round(tp, 4),
            'take_profit_2': 0,
            'take_profit_3': 0,
            'strategy': f'{TAG}_4H_Levels_1H_15M',
        }
        _log(f"📤 [{symbol}] SIGNAL {signal['type']} | Entry {_fmt(entry)} | SL {_fmt(sl)} | TP {_fmt(tp)}")

        if not self.telegram_bot:
            _log(f"⚠️ [{symbol}] telegram_bot تنظیم نشده؛ سیگنال ارسال نشد")
            return signal

        task = asyncio.create_task(self.telegram_bot.send_signal(signal))
        task.add_done_callback(self._on_send_done)
        return signal

    @staticmethod
    def _on_send_done(task):
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc:
            _log(f"❌ ارسال سیگنال به تلگرام شکست خورد: {exc!r}")
        else:
            _log("✅ سیگنال به تلگرام فرستاده شد")
