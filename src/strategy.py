

import os

import time

from datetime import datetime, timezone

from logger import get_trading_logger



logger = get_trading_logger()



TAG = "V90"     # سطح دستی ۱ساعته → تأیید ۱۵دقیقه → کندل تأیید/ورود ۵دقیقه





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





def _tsi(ms):

    """ساعت ایران (UTC+3:30)."""

    return datetime.fromtimestamp(ms / 1000 + 3.5 * 3600, tz=timezone.utc).strftime('%H:%M')





def _parse_updated(s):

    """LEVELS_UPDATED به وقت ایران: 'YYYY-MM-DD HH:MM' یا 'YYYY-MM-DD' → epoch UTC."""

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):

        try:

            dt = datetime.strptime(str(s), fmt)

            return dt.replace(tzinfo=timezone.utc).timestamp() - 3.5 * 3600

        except Exception:

            continue

    return None





def _tail(tf, now_ms, tail=60):

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

    """V90: سطوح دستی ۱ساعته؛ لمس/شکست → تأیید ۱۵دقیقه (شکست = بدنه‌ی قاطع با فاصله)

    → کندل تأیید ۵دقیقه (پولبک یا رد شدن، با امتیازدهی) → سیگنال."""



    def __init__(self, telegram_bot=None):

        self.running = False

        self.telegram_bot = telegram_bot

        self.market_data = None



        # ================= سطوح دستی =================

        self.SINGLE_LEVEL_HALF_WIDTH_PCT = 0.125   # اگر فقط یک عدد دادی: ±این‌قدر درصد

        self.STALE_LEVELS_HOURS = 30               # سطوح قدیمی‌تر از این: هشدار در لاگ

        self.WARN_EVERY_MIN = 60



        # ================= شکست (تأیید ۱۵دقیقه) =================

        self.BREAK_MARGIN_ATR15 = 0.2       # کلوز حداقل این‌قدر ATR(15m) بیرون لبه‌ی دور زون

        self.BREAK_MIN_BODY_RATIO = 0.5     # بدنه ≥ این نسبت از کل کندل (کندل قاطع، نه دوجی)

        self.RETEST_EXPIRE_H = 6            # مهلت پولبک بعد از شکست

        self.REJECT_EXPIRE_H = 3            # مهلت ورود بعد از «فقط لمس»

        self.APPROACH_FILTER = True         # False = فیلتر «قیمت از دور آمده باشد» خاموش

        self.APPROACH_LOOKBACK_C15 = 16

        self.APPROACH_CLEAR_C15 = 8

        self.APPROACH_MIN_DIST_ATR15 = 1.0

        self.SKIP_BLOCK_H = 1

        self.HISTORY_TRIGGER_MAX_AGE_MIN = 10

        self.START_INSIDE_TOL_PCT = 0.03

        self.LOG_ONLY_COOLDOWN_H = 2



        # ================= کندل تأیید ۵دقیقه =================

        self.TOUCH_TOL_ATR5 = 0.15          # رسیدن سایه تا این‌قدر نزدیک زون هم «لمس» است

        self.MIN_RANGE_ATR5 = 0.4           # کندل خیلی ریز (رنج < این‌قدر ATR(5m)) اصلاً بررسی نمی‌شود

        self.MIN_CONFIRM_SCORE = 3          # حداقل امتیاز (از ۸) برای سیگنال

        self.PIN_WICK_RATIO = 0.5           # سایه‌ی رد کننده ≥ ۵۰٪ کل کندل

        self.STRONG_BODY_RATIO = 0.5

        self.CLOSE_EXTREME_RATIO = 0.30     # کلوز در ۳۰٪ انتهایی کندل

        self.SHALLOW_ATR5 = 0.3             # سایه بیشتر از این‌قدر ATR(5m) از لبه‌ی دور زون عبور نکرده

        self.VOL_LOOKBACK = 20              # میانگین حجم ۲۰ کندل قبل

        self.VOL_SPIKE_RATIO = 1.3          # حجم کندل تأیید ≥ ۱.۳ برابر میانگین → امتیاز



        # ================= ستاپ و سیگنال =================

        self.MAX_SETUPS = 2

        self.ZONE_COOLDOWN_H = 2

        self.SIGNAL_COOLDOWN_MIN = 15

        self.MAX_DRIFT_ATR5 = 2.0           # قیمت زنده بیشتر از این‌قدر ATR(5m) از ورود دور شد → رد

        self.COUNTER_TREND_NOTE_IN_TYPE = True   # اگر ربات تلگرام ایراد گرفت False کن



        # ================= حد ضرر / هدف =================

        self.SL_ATR5_BUFFER = 0.5

        self.MIN_RISK_ATR15 = 0.75

        self.MAX_RISK_ATR15 = 4.0

        self.RR1 = 1.5

        self.MIN_ROOM_R = 1.0               # سطح مخالف نزدیک‌تر از این‌قدر R بود، سیگنال نده (۰ = خاموش)

        self.TRACK_MAX_H = 24



        # ================= اسکن =================

        self.SCAN_SLEEP_SYMBOL = 3

        self.SCAN_SLEEP_CYCLE = 10

        self.STATUS_LOG_EVERY_SEC = 180



        self.setups = {}

        self.prev_scan = {}

        self.blocked = {}

        self.log_only_until = {}

        self.warn_until = {}

        self.last_signal_ms = {}

        self.last_status = {}

        self.tracked = []

        self.seq = 0

        self.stats = {k: {'n': 0, 'tp': 0, 'sl': 0, 'r': 0.0} for k in ('کل', 'هم‌جهت', 'ضدروند')}

        self.flag_stats = {}



        # ---- سطوح (با بازخوانی خودکار) ----

        self._lv_mod = None

        self._lv_mtime = None

        self._lv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'levels.py')

        self.levels, self.levels_updated, self.cfg_symbols, self.levels_error = {}, None, [], None

        self._load_levels(first=True)

        if self.levels_error:

            _log(f"❌ [{TAG}] فایل levels.py خوانده نشد: {self.levels_error} — بدون سطح، سیگنالی صادر نمی‌شود")

        else:

            _log(f"✅ [{TAG}] سطوح ۱ساعته خوانده شد (به‌روزرسانی: {self.levels_updated}) | "

                 + " | ".join(f"{s}: {len(v)} زون" for s, v in self.levels.items()))



    # ==========================================================

    def set_market_data_handler(self, handler):

        self.market_data = handler



    def _now_ms(self):

        return int(time.time() * 1000)



    @property

    def symbols(self):

        return [s for s in self.cfg_symbols if self.levels.get(s)]



    async def start_monitoring(self):

        self.running = True

        _log(f"🚀 {TAG} Started (سطح ۱ساعته > تأیید ۱۵دقیقه > ورود ۵دقیقه)")

        while self.running:

            self._check_levels_reload()

            syms = self.symbols

            if not syms:

                self._warn('ALL', 'empty', "هیچ نمادی سطح ندارد؛ levels.py را پر کن", self._now_ms())

            for symbol in syms:

                try:

                    await self.process_symbol(symbol)

                except Exception as e:

                    _log(f"❌ [{TAG}] Error in {symbol}: {e!r}")

                await asyncio.sleep(self.SCAN_SLEEP_SYMBOL)

            await asyncio.sleep(self.SCAN_SLEEP_CYCLE)



    async def stop(self):

        self.running = False

        _log("StrategyManager stopped")



    # ==========================================================

    # سطوح دستی (با بازخوانی خودکار وقتی فایل تغییر کرد)

    # ==========================================================

    def _mtime(self):

        try:

            return os.path.getmtime(self._lv_path)

        except Exception:

            return None



    def _load_levels(self, first=False):

        try:

            if self._lv_mod is None:

                self._lv_mod = importlib.import_module('levels')

            elif not first:

                importlib.reload(self._lv_mod)

            mod = self._lv_mod

            self.levels = dict(mod.LEVELS)

            self.levels_updated = getattr(mod, 'LEVELS_UPDATED', None)

            self.cfg_symbols = list(getattr(mod, 'SYMBOLS', None) or self.levels.keys())

            self.levels_error = None

        except Exception as e:

            self.levels_error = repr(e)

        self._lv_mtime = self._mtime()

        return self.levels_error is None



    def _check_levels_reload(self):

        try:

            m = self._mtime()

            if m == self._lv_mtime:

                return

            if self._load_levels():

                self.prev_scan.clear()      # اسکن بعدی با سطوح جدید شروع شود

                _log(f"🔄 [{TAG}] levels.py تغییر کرد و دوباره خوانده شد (به‌روزرسانی: {self.levels_updated}) | "

                     + " | ".join(f"{s}: {len(v)} زون" for s, v in self.levels.items()))

            else:

                _log(f"❌ [{TAG}] levels.py تغییر کرد ولی خطا دارد؛ سطوح قبلی نگه داشته شد: {self.levels_error}")

        except Exception as e:

            _log(f"❌ [{TAG}] خطا در بازخوانی سطوح: {e!r}")



    def _get_levels(self, symbol):

        out = []

        for item in self.levels.get(symbol, []):

            try:

                if isinstance(item, (int, float)):

                    c = float(item)

                    w = c * self.SINGLE_LEVEL_HALF_WIDTH_PCT / 100

                    lo, hi = c - w, c + w

                else:

                    lo, hi = float(item[0]), float(item[1])

            except Exception:

                continue

            if lo > hi:

                lo, hi = hi, lo

            out.append({'low': lo, 'high': hi, 'mid': (lo + hi) / 2})

        out.sort(key=lambda b: b['low'])

        return out



    @staticmethod

    def _pick_active(book, price):

        """نزدیک‌ترین زون بالای قیمت (مقاومت) و نزدیک‌ترین زیر قیمت (حمایت) فعال‌اند.

        نقش‌ها از موقعیت نسبت به قیمت می‌آید؛ بعد از شکست، سطح شکسته خودکار نقش مقابل را می‌گیرد."""

        for b in book:

            if b['mid'] > price:

                b['role'], b['dist'] = 'RES', max(0.0, b['low'] - price)

            else:

                b['role'], b['dist'] = 'SUP', max(0.0, price - b['high'])

            b['active'] = False

        for role in ('RES', 'SUP'):

            side = [b for b in book if b['role'] == role]

            if side:

                min(side, key=lambda b: b['dist'])['active'] = True



    def _warn(self, symbol, key, msg, now_ms):

        k = (symbol, key)

        if self.warn_until.get(k, 0) <= now_ms:

            self.warn_until[k] = now_ms + self.WARN_EVERY_MIN * 60_000

            _log(f"⚠️ [{TAG}][{symbol}] {msg}")



    def _check_warnings(self, symbol, book, price, now_ms):

        if not any(b['role'] == 'RES' for b in book):

            self._warn(symbol, 'nores', f"قیمت ({_fmt(price)}) بالاتر از همه‌ی سطوح است؛ مقاومتی نیست. سطوح را به‌روز کن", now_ms)

        if not any(b['role'] == 'SUP' for b in book):

            self._warn(symbol, 'nosup', f"قیمت ({_fmt(price)}) پایین‌تر از همه‌ی سطوح است؛ حمایتی نیست. سطوح را به‌روز کن", now_ms)

        if self.levels_updated:

            ts = _parse_updated(self.levels_updated)

            if ts is not None:

                age_h = (now_ms / 1000 - ts) / 3600

                if age_h > self.STALE_LEVELS_HOURS:

                    self._warn(symbol, 'stale', f"سطوح {age_h:.0f} ساعت پیش به‌روز شده‌اند. لطفاً به‌روز کن", now_ms)



    # ==========================================================

    # رویدادها: لمس / شکست

    # ==========================================================

    def _is_blocked(self, symbol, z, now_ms):

        lst = [b for b in self.blocked.get(symbol, []) if b[2] > now_ms]

        self.blocked[symbol] = lst

        tol = z['high'] * 0.001

        return any(not (z['high'] < b[0] - tol or z['low'] > b[1] + tol) for b in lst)



    def _block(self, symbol, z, now_ms, hours=None):

        h = self.ZONE_COOLDOWN_H if hours is None else hours

        self.blocked.setdefault(symbol, []).append((z['low'], z['high'], now_ms + h * 3_600_000))



    def _detect_events(self, symbol, prev, c5, price, now_ms):

        """هر سطحی از اسکن قبلی که تا الان لمس/رد شده (قبلش از سمت درست آمده بوده)."""

        pp = prev['price']

        candles = [c for c in c5 if c[1] > prev['ms']]

        active = self.setups.get(symbol, [])

        found = []

        for lv in prev['levels']:

            if self._is_blocked(symbol, lv, now_ms):

                continue

            tol = lv['high'] * 0.001

            if any(not (lv['high'] < s['zone']['low'] - tol or lv['low'] > s['zone']['high'] + tol) for s in active):

                continue

            if pp < lv['low']:

                role = 'RES'

            elif pp > lv['high']:

                role = 'SUP'

            else:

                continue

            ev = None

            for (t, cm, o, h, l, c) in candles:

                if (role == 'RES' and h >= lv['low']) or (role == 'SUP' and l <= lv['high']):

                    ev = t

                    break

            if ev is None and ((role == 'RES' and price >= lv['low']) or (role == 'SUP' and price <= lv['high'])):

                ev = now_ms

            if ev is not None:

                found.append((lv, role, ev))

        found.sort(key=lambda x: x[2])

        return found



    @staticmethod

    def _zs(z):

        return f"{_fmt(z['low'])}–{_fmt(z['high'])}"



    @staticmethod

    def _beyond(role, z, close):

        return (role == 'RES' and close > z['high']) or (role == 'SUP' and close < z['low'])



    def _is_break(self, role, z, c, atr15):

        """شکست واقعی در ۱۵دقیقه: کلوز بیرون لبه‌ی دور زون با فاصله + بدنه‌ی قاطع در جهت شکست."""

        _, _, o, h, l, cl = c

        rng = max(h - l, 1e-12)

        if abs(cl - o) < self.BREAK_MIN_BODY_RATIO * rng:

            return False

        margin = self.BREAK_MARGIN_ATR15 * atr15

        if role == 'RES':

            return cl > z['high'] + margin and cl > o

        return cl < z['low'] - margin and cl < o



    def _history_break(self, role, z, c15, atr15, now_ms):

        """قیمت الان بیرون زون است؛ آیا کندل‌های ۱۵دقیقه‌ی بسته‌شده هم شکست قاطع را تأیید کرده‌اند؟"""

        lim = now_ms - self.RETEST_EXPIRE_H * 3_600_000

        cs = [c for c in c15 if c[1] > lim]

        if not cs or not self._beyond(role, z, cs[-1][5]):

            return None

        i = len(cs) - 1

        while i > 0 and self._beyond(role, z, cs[i - 1][5]):

            i -= 1

        for c in cs[i:]:

            if self._is_break(role, z, c, atr15):

                return c

        return None



    def _side_from_history(self, z, price, c5, c15):

        """قیمت داخل زون است: از کدام سمت آمده؟ آخرین کلوزِ بیرون زون."""

        for series in (c5, c15):

            for c in reversed(series):

                if c[5] < z['low']:

                    return 'RES'

                if c[5] > z['high']:

                    return 'SUP'

        return 'RES' if price < (z['low'] + z['high']) / 2 else 'SUP'



    def _fresh_approach(self, lv, role, c15, atr15, ev_ms, price):

        """لمس معتبر = قیمت از دور آمده؛ نه اینکه ساعت‌ها کنار سطح چرخیده باشد."""

        if not self.APPROACH_FILTER:

            return True, "فیلتر خاموش است"

        prior = [c for c in c15 if c[1] <= ev_ms][-self.APPROACH_LOOKBACK_C15:]

        if len(prior) < self.APPROACH_CLEAR_C15:

            return True, "دیتا کافی نیست؛ بدون فیلتر"

        tol = price * 0.0003

        clear = prior[-self.APPROACH_CLEAR_C15:]

        if role == 'RES':

            touched = any(c[3] >= lv['low'] - tol for c in clear)

            far = lv['low'] - min(c[4] for c in prior)

        else:

            touched = any(c[4] <= lv['high'] + tol for c in clear)

            far = max(c[3] for c in prior) - lv['high']

        if touched:

            return False, f"قیمت در {self.APPROACH_CLEAR_C15 * 15} دقیقه‌ی اخیر کنار سطح بوده"

        if atr15 and far < self.APPROACH_MIN_DIST_ATR15 * atr15:

            return False, f"دورترین فاصله‌ی قیمت از سطح فقط {far / atr15:.2f} ATR(15m) بوده"

        return True, "ok"



    def _new_setup(self, symbol, lv, role, ev_ms, price, now_ms, c15, atr15, fresh_ok, fresh_why, origin):

        z = {'low': lv['low'], 'high': lv['high']}

        st = {'state': 'WAIT_1H', 'role': role, 'zone': z, 'event_ms': ev_ms, 'kind': None,
              'since_ms': ev_ms, 'checked_ms': ev_ms, 'fresh_ok': fresh_ok, 'fresh_why': fresh_why,
              'c15_txt': 'منتظر بسته‌شدن کندل ۱ساعته'}

        fa = 'مقاومت' if role == 'RES' else 'حمایت'
        _log(f"⏳ [{TAG}][{symbol}] {origin}: {fa} {self._zs(z)} لمس/ثبت شد؛ سیگنال ممنوع تا بسته‌شدن کندل ۱ساعته")
        return st


    # ==========================================================

    # کندل تأیید ۵دقیقه

    # ==========================================================

    def _eval_confirm(self, role, z, c, prev, atr5, relvol):

        """None اگر دروازه رد شد؛ وگرنه {'score','flags',...}.

        دروازه: رنج کندل کافی، سایه به زون برسد، کلوز بیرون زون (در جهت ستاپ)، جهت کندل درست.

        امتیاز (حداکثر ۸): پین‌بار ۲، انگالف ۲، بدنه‌ی قوی ۱، کلوز در انتهای کندل ۱، پولبک کم‌عمق ۱، حجم بالا ۱."""

        _, _, o, h, l, cl = c

        rng = max(h - l, 1e-12)

        if rng < self.MIN_RANGE_ATR5 * atr5:

            return None

        body = abs(cl - o)

        tol = self.TOUCH_TOL_ATR5 * atr5

        if role == 'RES':

            if not (h >= z['low'] - tol and cl < z['low'] and cl < o):

                return None

            wick = h - max(o, cl)

            close_ext = (cl - l) / rng

            engulf = bool(prev and prev[5] > prev[2] and o >= prev[5] and cl <= prev[2] and body >= abs(prev[5] - prev[2]))

            shallow = h <= z['high'] + self.SHALLOW_ATR5 * atr5

            pen = h - z['low']

        else:

            if not (l <= z['high'] + tol and cl > z['high'] and cl > o):

                return None

            wick = min(o, cl) - l

            close_ext = (h - cl) / rng

            engulf = bool(prev and prev[5] < prev[2] and o <= prev[5] and cl >= prev[2] and body >= abs(prev[5] - prev[2]))

            shallow = l >= z['low'] - self.SHALLOW_ATR5 * atr5

            pen = z['high'] - l

        flags = {

            'pin': wick / rng >= self.PIN_WICK_RATIO,

            'engulf': engulf,

            'body': body / rng >= self.STRONG_BODY_RATIO,

            'close': close_ext <= self.CLOSE_EXTREME_RATIO,

            'shallow': shallow,

            'vol': relvol is not None and relvol >= self.VOL_SPIKE_RATIO,

        }

        score = 2 * flags['pin'] + 2 * flags['engulf'] + flags['body'] + flags['close'] + flags['shallow'] + flags['vol']

        return {'score': int(score), 'flags': flags, 'pen': pen, 'wick': wick / rng, 'body': body / rng, 'relvol': relvol}



    def _relvol(self, c5, i, v5):

        cur = v5.get(c5[i][0])

        prior = [v5.get(p[0]) for p in c5[max(0, i - self.VOL_LOOKBACK):i]]

        prior = [v for v in prior if v is not None and v > 0]

        if cur is None or len(prior) < 5:

            return None

        return cur / (sum(prior) / len(prior))



    # ==========================================================

    # ماشین حالت: WAIT_1H -> WAIT_15M / ARMED -> سیگنال

    # ==========================================================

    def _advance(self, symbol, st, trend, price, c1h, c15, c5, v5, atr5, atr15, targets, now_ms):

        z = st['zone']



        # ---- دروازه‌ی ۱ساعته: تا بسته‌شدن کندلِ شامل رویداد هیچ سیگنالی صادر نشود ----

        if st['state'] == 'WAIT_1H':
            event_ms = st['event_ms']
            event_hour = next((c for c in c1h if c[0] <= event_ms < c[1]), None)
            if event_hour is None:
                return None  # کندل مربوطه هنوز بسته نشده یا در داده‌ی دریافتی نیست

            _, hour_close_ms, hour_o, hour_h, hour_l, hour_cl = event_hour
            old_role = st['role']
            z = st['zone']

            if self._beyond(old_role, z, hour_cl):
                st['role'] = 'SUP' if old_role == 'RES' else 'RES'
                st.update(state='ARMED', kind='RETEST', since_ms=hour_close_ms,
                          checked_ms=hour_close_ms,
                          c15_txt=f"کلوز ۱ساعته {_fmt(hour_cl)} بیرون زون؛ شکست تأیید شد")
                _log(f"💥 [{TAG}][{symbol}] کلوز ۱ساعته {_fmt(hour_cl)} بیرون زون {self._zs(z)}؛ {old_role}→{st['role']} | پس از این لحظه بررسی پولبک/تأیید ۵دقیقه مجاز است")
            else:
                st.update(state='WAIT_15M', event_ms=hour_close_ms, since_ms=hour_close_ms,
                          checked_ms=hour_close_ms,
                          c15_txt=f"کلوز ۱ساعته {_fmt(hour_cl)} شکست را تأیید نکرد؛ بررسی ۱۵دقیقه از بعدِ کلوز")
                _log(f"🕐 [{TAG}][{symbol}] کندل ۱ساعته بسته شد ({_fmt(hour_cl)}) و شکست زون {self._zs(z)} تأیید نشد؛ اکنون بررسی ۱۵دقیقه شروع می‌شود")

        # ---- تأیید ۱۵دقیقه: شکست قاطع یا فقط لمس؟ ----

        if st['state'] == 'WAIT_15M':

            after = [c for c in c15 if c[1] > st['event_ms']]

            if not after:

                return None

            broke = next((c for c in after if self._is_break(st['role'], z, c, atr15)), None)

            if broke:

                old = st['role']

                st['role'] = 'SUP' if old == 'RES' else 'RES'

                st.update(state='ARMED', kind='RETEST', since_ms=broke[1], checked_ms=broke[1],

                          c15_txt=f"کندل ۱۵دقیقه {_ts(broke[0])} UTC ({_tsi(broke[0])} ایران) با بدنه‌ی قاطع و کلوز {_fmt(broke[5])} بیرون زون بسته شد")

                _log(f"💥 [{TAG}][{symbol}] ۱۵دقیقه: شکست واقعی ({_fmt(broke[5])}) | زون {self._zs(z)} از {'مقاومت' if old == 'RES' else 'حمایت'} "

                     f"به {'مقاومت' if st['role'] == 'RES' else 'حمایت'} تبدیل شد | منتظر کندل تأیید ۵دقیقه تا {self.RETEST_EXPIRE_H} ساعت")

            else:

                if not st.get('fresh_ok', True):

                    _log(f"🚫 [{TAG}][{symbol}] ۱۵دقیقه: نشکست (فقط لمس)، ولی «رد شدن» از زون {self._zs(z)} نادیده گرفته شد: {st['fresh_why']}")

                    self._block(symbol, z, now_ms, self.SKIP_BLOCK_H)

                    st['state'] = 'DEAD'

                    return None

                st.update(state='ARMED', kind='REJECT', since_ms=st['event_ms'], checked_ms=st['event_ms'],

                          c15_txt="کندل ۱۵دقیقه‌ی بعد از لمس با بدنه‌ی قاطع بیرون زون نبست")

                _log(f"↩️ [{TAG}][{symbol}] ۱۵دقیقه: شکست واقعی نبود، فقط لمس زون {self._zs(z)} | منتظر کندل تأیید ۵دقیقه تا {self.REJECT_EXPIRE_H} ساعت")



        if st['state'] != 'ARMED':

            return None



        # ---- شکست بعدی ۱۵دقیقه علیه ستاپ: نقش زون عوض می‌شود ----

        for c in c15:

            if c[1] > st['since_ms'] and self._is_break(st['role'], z, c, atr15):

                old = st['role']

                st['role'] = 'SUP' if old == 'RES' else 'RES'

                st.update(kind='RETEST', since_ms=c[1], checked_ms=c[1],

                          c15_txt=f"کندل ۱۵دقیقه {_ts(c[0])} UTC ({_tsi(c[0])} ایران) با بدنه‌ی قاطع و کلوز {_fmt(c[5])} بیرون زون بسته شد")

                _log(f"💥 [{TAG}][{symbol}] شکست ۱۵دقیقه ({_fmt(c[5])}) | زون {self._zs(z)}: {old}→{st['role']} | منتظر کندل تأیید ۵دقیقه")



        # ---- انقضا ----

        hours = self.RETEST_EXPIRE_H if st['kind'] == 'RETEST' else self.REJECT_EXPIRE_H

        if now_ms - st['since_ms'] > hours * 3_600_000:

            _log(f"⌛ [{TAG}][{symbol}] ستاپ {st['kind']} روی زون {self._zs(z)} بدون سیگنال منقضی شد")

            self._block(symbol, z, now_ms)

            st['state'] = 'DEAD'

            return None



        # ---- کندل تأیید ۵دقیقه ----

        best = None

        last_cm = st['checked_ms']

        for i, c in enumerate(c5):

            if c[1] <= max(st['since_ms'], st['checked_ms']):

                continue

            last_cm = max(last_cm, c[1])

            ev = self._eval_confirm(st['role'], z, c, c5[i - 1] if i > 0 else None, atr5, self._relvol(c5, i, v5))

            if ev is None:

                continue

            if ev['score'] >= self.MIN_CONFIRM_SCORE:

                best = (c, ev)

            else:

                _log(f"➖ [{TAG}][{symbol}] کندل ۵دقیقه {_ts(c[0])} UTC ({_tsi(c[0])} ایران) به زون {self._zs(z)} رسید ولی تأیید ضعیف بود "

                     f"(امتیاز {ev['score']} از ۸؛ حداقل {self.MIN_CONFIRM_SCORE})")

        st['checked_ms'] = last_cm

        if not best:

            return None

        trig, ev = best



        direction = 'SHORT' if st['role'] == 'RES' else 'LONG'

        counter = (direction == 'SHORT' and trend != 'DOWNTREND') or (direction == 'LONG' and trend != 'UPTREND')

        entry = trig[5]

        if atr5 and abs(price - entry) > self.MAX_DRIFT_ATR5 * atr5:

            _log(f"⏭️ [{TAG}][{symbol}] تأیید {direction} دیده شد ولی قیمت از ورود ({_fmt(entry)}) دور شده ({_fmt(price)})؛ فرصت از دست رفت")

            return None

        if now_ms - self.last_signal_ms.get(symbol, 0) < self.SIGNAL_COOLDOWN_MIN * 60_000:

            _log(f"⏸️ [{TAG}][{symbol}] تأیید {direction} دیده شد ولی کمتر از {self.SIGNAL_COOLDOWN_MIN} دقیقه از سیگنال قبلی گذشته")

            return None



        trade = self._build_trade(symbol, direction, entry, z, trig, atr5, atr15, targets)

        if trade is None:

            return None



        self.last_signal_ms[symbol] = now_ms

        self.seq += 1

        sid = self.seq

        flags_txt = "+".join(k for k, v in ev['flags'].items() if v) or "—"

        ctag = ' ⚠️ برخلاف روند ۴ساعته' if counter else ''

        _, _, to, th, tl, tc = trig

        kind_t = 'پولبک بعد از شکست' if st['kind'] == 'RETEST' else 'رد شدن از سطح'

        rv = f"{ev['relvol']:.2f}x" if ev['relvol'] is not None else "—"

        _log(f"🏆 [{TAG}][{symbol}] {direction} Entry Triggered! #{sid} ({kind_t}){ctag} | تأیید ۱۵دقیقه: {st.get('c15_txt', '—')} | "

             f"کندل ۵دقیقه {_ts(trig[0])} UTC ({_tsi(trig[0])} ایران): O{_fmt(to)} H{_fmt(th)} L{_fmt(tl)} C{_fmt(tc)} | "

             f"امتیاز {ev['score']}/۸ [{flags_txt}] | حجم {rv} | زون {self._zs(z)}")

        _log(f"📒 JOURNAL | #{sid} | {symbol} | {direction} | {st['kind']} | امتیاز {ev['score']}/۸ [{flags_txt}] | حجم {rv} | "

             f"سایه {ev['wick'] * 100:.0f}% بدنه {ev['body'] * 100:.0f}% | زون {self._zs(z)} | ورود {_fmt(entry)} SL {_fmt(trade['sl'])} "

             f"TP1 {_fmt(trade['tp1'])} TP2 {_fmt(trade['tp2']) if trade['tp2'] else '—'} | "

             f"ریسک {trade['risk'] / entry * 100:.2f}% = {trade['risk'] / atr15:.2f} ATR(15m) | {trade['room_txt']} | "

             f"روند {trend}{' (ضدروند)' if counter else ''} | {_ts(now_ms)} UTC")

        self.send_signal(symbol, direction, entry, trade, counter, trend, ev)

        self._track_add(sid, symbol, direction, entry, trade, trig[1], now_ms, counter, ev)

        self._block(symbol, z, now_ms)

        st['state'] = 'DEAD'

        return True



    # ==========================================================

    # حد ضرر / هدف

    # ==========================================================

    def _build_trade(self, symbol, direction, entry, z, trig, atr5, atr15, targets):

        buf = self.SL_ATR5_BUFFER * atr5

        sl = (max(z['high'], trig[3]) + buf) if direction == 'SHORT' else (min(z['low'], trig[4]) - buf)

        risk = abs(entry - sl)

        if atr15:

            if risk > self.MAX_RISK_ATR15 * atr15:

                _log(f"🚫 [{TAG}][{symbol}] {direction} روی {self._zs(z)} رد شد: ریسک {risk / atr15:.2f} ATR(15m) از سقف {self.MAX_RISK_ATR15} بیشتر است")

                return None

            if risk < self.MIN_RISK_ATR15 * atr15:

                risk = self.MIN_RISK_ATR15 * atr15

                sl = entry + risk if direction == 'SHORT' else entry - risk

        if risk <= 0:

            return None



        # نزدیک‌ترین سطوح در جهت معامله (لبه‌ی نزدیک به قیمت)

        if direction == 'SHORT':

            edges = sorted({b['high'] for b in targets if b['high'] < entry - 1e-9 and b['mid'] < z['low']}, reverse=True)

            dists = [entry - e for e in edges]

        else:

            edges = sorted({b['low'] for b in targets if b['low'] > entry + 1e-9 and b['mid'] > z['high']})

            dists = [e - entry for e in edges]



        d1 = self.RR1 * risk

        tp1_dist, tp2_edge = d1, 0

        room_txt = "سطحی در مسیر نیست"

        if dists:

            room_txt = f"نزدیک‌ترین سطح مسیر {dists[0] / risk:.2f}R ({_fmt(edges[0])})"

            if self.MIN_ROOM_R and dists[0] < self.MIN_ROOM_R * risk:

                _log(f"🚫 [{TAG}][{symbol}] {direction} روی {self._zs(z)} رد شد: سطح مسیر فقط {dists[0] / risk:.2f}R آن‌طرف‌تر است "

                     f"({_fmt(edges[0])}؛ حداقل {self.MIN_ROOM_R}R)")

                return None

            if dists[0] < d1:

                tp1_dist = dists[0]                      # TP1 قبل از مانع

                tp2_edge = edges[1] if len(edges) > 1 else 0

            else:

                tp2_edge = edges[0]

        tp1 = entry - tp1_dist if direction == 'SHORT' else entry + tp1_dist

        return {'sl': sl, 'tp1': tp1, 'tp2': tp2_edge, 'risk': risk, 'r_tp1': tp1_dist / risk, 'room_txt': room_txt}



    def send_signal(self, symbol, direction, entry, trade, counter=False, trend='', ev=None):

        base = 'SELL' if direction == 'SHORT' else 'BUY'

        typ = base + (' ⚠️ برخلاف روند ۴ساعته' if (counter and self.COUNTER_TREND_NOTE_IN_TYPE) else '')

        note = (f"⚠️ برخلاف روند ۴ساعته ({'نزولی' if trend == 'DOWNTREND' else 'صعودی'}، EMA200) — تصمیم با خودت" if counter else '')

        signal = {

            'symbol': symbol, 'type': typ, 'direction': direction, 'entry_price': entry,

            'stop_loss': round(trade['sl'], 4), 'take_profit_1': round(trade['tp1'], 4),

            'take_profit_2': round(trade['tp2'], 4) if trade['tp2'] else 0, 'take_profit_3': 0,

            'strategy': f'{TAG}_1H_levels_15m_5m' + (' | ⚠️ برخلاف روند' if counter else ''),

            'note': note, 'counter_trend': counter, 'confirm_score': ev['score'] if ev else 0,

        }

        _log(f"📤 [{TAG}][{symbol}] SIGNAL {typ} | Entry {_fmt(entry)} | SL {_fmt(trade['sl'])} | TP1 {_fmt(trade['tp1'])} ({trade['r_tp1']:.2f}R) | "

             f"TP2 {_fmt(trade['tp2']) if trade['tp2'] else '—'}")

        if not self.telegram_bot:

            _log(f"⚠️ [{TAG}][{symbol}] telegram_bot تنظیم نشده؛ سیگنال ارسال نشد")

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

        _log(f"❌ ارسال سیگنال به تلگرام شکست خورد: {exc!r}" if exc else "✅ سیگنال به تلگرام فرستاده شد")



    # ==========================================================

    # ردیابی نتیجه‌ی سیگنال‌ها

    # ==========================================================

    def _track_add(self, sid, symbol, direction, entry, trade, start_ms, now_ms, counter, ev):

        self.tracked.append({'id': sid, 'symbol': symbol, 'direction': direction, 'entry': entry, 'sl': trade['sl'],

                             'tp': trade['tp1'], 'risk': trade['risk'], 'r_tp': trade['r_tp1'], 'start_ms': start_ms,

                             'created_ms': now_ms, 'last_cm': start_ms, 'mfe': 0.0, 'mae': 0.0, 'status': 'OPEN',

                             'counter': counter, 'flags': [k for k, v in ev['flags'].items() if v], 'score': ev['score']})



    def _track_update(self, symbol, c5, now_ms):

        for t in self.tracked:

            if t['symbol'] != symbol or t['status'] != 'OPEN':

                continue

            for (ot, cm, o, h, l, c) in c5:

                if ot < t['start_ms'] or cm <= t['last_cm']:

                    continue

                t['last_cm'] = cm

                if t['direction'] == 'SHORT':

                    t['mfe'] = max(t['mfe'], (t['entry'] - l) / t['risk'])

                    t['mae'] = max(t['mae'], (h - t['entry']) / t['risk'])

                    hit_sl, hit_tp = h >= t['sl'], l <= t['tp']

                else:

                    t['mfe'] = max(t['mfe'], (h - t['entry']) / t['risk'])

                    t['mae'] = max(t['mae'], (t['entry'] - l) / t['risk'])

                    hit_sl, hit_tp = l <= t['sl'], h >= t['tp']

                if hit_sl or hit_tp:

                    self._track_close(t, 'SL' if hit_sl else 'TP1', cm, hit_sl and hit_tp)

                    break

            if t['status'] == 'OPEN' and now_ms - t['created_ms'] > self.TRACK_MAX_H * 3_600_000:

                self._track_close(t, 'OPEN-EXPIRED', now_ms, False)



    def _track_close(self, t, result, ms, ambiguous):

        t['status'] = result

        r = t['r_tp'] if result == 'TP1' else (-1.0 if result == 'SL' else 0.0)

        group = 'ضدروند' if t['counter'] else 'هم‌جهت'

        if result in ('TP1', 'SL'):

            for k in ('کل', group):

                s = self.stats[k]

                s['n'] += 1

                s['tp' if result == 'TP1' else 'sl'] += 1

                s['r'] += r

            for f in (t['flags'] or ['بدون‌الگو']):

                fs = self.flag_stats.setdefault(f, [0, 0])

                fs[0] += 1

                fs[1] += 1 if result == 'TP1' else 0

        _log(f"📒 RESULT | #{t['id']} | {t['symbol']} | {t['direction']}{' (ضدروند)' if t['counter'] else ''} | {result}"

             f"{' (هر دو در یک کندل؛ SL فرض شد)' if ambiguous else ''} | R={r:+.2f} | امتیاز {t['score']} [{'+'.join(t['flags']) or '—'}] | "

             f"بیشترین پیشروی {t['mfe']:.2f}R / برگشت {t['mae']:.2f}R | مدت {(ms - t['created_ms']) / 3_600_000:.1f}h | "

             f"ورود {_fmt(t['entry'])} SL {_fmt(t['sl'])} TP {_fmt(t['tp'])}")

        parts = [f"{k}: {s['n']} معامله، وین‌ریت {s['tp'] / s['n'] * 100:.0f}%، جمع R {s['r']:+.2f}" for k, s in self.stats.items() if s['n']]

        if parts:

            _log(f"📈 آمار | " + " | ".join(parts))

        if self.flag_stats:

            _log("📈 الگوها | " + " ، ".join(f"{f}: {v[1]}/{v[0]} TP" for f, v in self.flag_stats.items()))

        self.tracked = [x for x in self.tracked if x['status'] == 'OPEN']



    # ==========================================================

    # حلقه‌ی اصلی هر نماد

    # ==========================================================

    async def process_symbol(self, symbol):

        md = self.market_data

        h4 = await md.get_tf_data(symbol, "240")      # فقط EMA200 برای جهت روند

        h1 = await md.get_tf_data(symbol, "60")       # دروازه‌ی تأیید: فقط پس از کلوز کندل لمس

        m15 = await md.get_tf_data(symbol, "15")

        m5 = await md.get_tf_data(symbol, "5")

        if not all([h4, h1, m15, m5]):

            _log(f"⚠️ [{TAG}] {symbol}: data missing (4H={bool(h4)}, 1H={bool(h1)}, 15m={bool(m15)}, 5m={bool(m5)})")

            return



        now_ms = self._now_ms()

        price = m5['price']

        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        atr5, atr15 = m5['atr'], m15['atr']



        book = self._get_levels(symbol)

        self._pick_active(book, price)

        self._check_warnings(symbol, book, price, now_ms)



        c1h = _tail(h1, now_ms, 48)

        c15 = _tail(m15, now_ms, 60)

        c5 = _tail(m5, now_ms, 80)

        v5 = dict(zip(m5['times'], m5.get('volumes', [])))

        lst = self.setups.setdefault(symbol, [])

        open_n = lambda: len([x for x in lst if x['state'] != 'DEAD'])



        # --- رویدادهای جدید ---

        prev = self.prev_scan.get(symbol)

        if not prev:

            # دیپلوی/ری‌استارت/بازخوانی سطوح: اگر قیمت همین الان داخل زون فعال است، ثبتش کن

            tol = price * self.START_INSIDE_TOL_PCT / 100

            for lv in book:

                if lv.get('active') and lv['low'] - tol <= price <= lv['high'] + tol and open_n() < self.MAX_SETUPS:

                    role = self._side_from_history(lv, price, c5, c15)

                    lst.append(self._new_setup(symbol, lv, role, now_ms, price, now_ms, c15, atr15, True, 'ok',

                                               f"شروع/بازخوانی: قیمت داخل زون ۱ساعته است ({'از پایین' if role == 'RES' else 'از بالا'} آمده)"))

        else:

            logged = 0

            for lv, role, ev_ms in self._detect_events(symbol, prev, c5, price, now_ms):

                if not lv.get('active'):

                    key = (symbol, round(lv['low'], 4))

                    if logged < 5 and self.log_only_until.get(key, 0) <= now_ms:

                        self.log_only_until[key] = now_ms + self.LOG_ONLY_COOLDOWN_H * 3_600_000

                        logged += 1

                        _log(f"📝 [{TAG}][{symbol}] سطح غیرفعال {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} در {_ts(ev_ms)} لمس/شکسته شد | "

                             f"قیمت {_fmt(price)} (فقط ثبت)")

                    continue

                if open_n() >= self.MAX_SETUPS:

                    continue

                ok, why = self._fresh_approach(lv, role, c15, atr15, ev_ms, price)

                passed = self._beyond(role, lv, price)

                origin = (f"عبور از سطح بین دو اسکن (قبلی {_fmt(prev['price'])} → الان {_fmt(price)})" if passed

                          else f"لمس سطح در {_ts(ev_ms)} UTC")

                lst.append(self._new_setup(symbol, lv, role, ev_ms, price, now_ms, c15, atr15, ok, why, origin))



        # --- پیشبرد ستاپ‌ها ---

        for st in lst:

            if st['state'] != 'DEAD':

                self._advance(symbol, st, trend, price, c1h, c15, c5, v5, atr5, atr15, book, now_ms)

        self.setups[symbol] = lst = [x for x in lst if x['state'] != 'DEAD']



        # --- نتیجه‌ی سیگنال‌های قبلی ---

        self._track_update(symbol, c5, now_ms)

        self.prev_scan[symbol] = {'levels': book, 'price': price, 'ms': now_ms}



        # --- لاگ وضعیت (کم‌تکرار) ---

        sig = tuple((s['state'], s['kind'], s['role']) for s in lst)

        ls = self.last_status.get(symbol)

        if not ls or ls[0] != sig or now_ms - ls[1] >= self.STATUS_LOG_EVERY_SEC * 1000:

            self.last_status[symbol] = (sig, now_ms)



            def _lv(b):

                mark = ('▲مقاومت' if b['role'] == 'RES' else '▼حمایت') if b['active'] else ('ر' if b['role'] == 'RES' else 'ح')

                return f"{mark} {self._zs(b)}"

            parts = [f"{s['state']}/{s['kind'] or 'EVENT'} {s['role']} {self._zs(s['zone'])}" for s in lst]

            _log(f"📊 [{TAG}] {symbol} — {trend} | قیمت {_fmt(price)} | سطوح ۱ساعته: "

                 + (" ، ".join(_lv(b) for b in sorted(book, key=lambda b: -b['low'])) or "—")

                 + " | ستاپ‌ها: " + (" || ".join(parts) if parts else "—"))
