import asyncio
import importlib
import os
import time
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

TAG = "V90"     # سطح دستی ۱ساعته → تأیید ۱ساعته → تأیید ۱۵دقیقه → کندل تأیید/ورود ۵دقیقه


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
    """V90: سطوح دستی ۱ساعته؛ لمس/عبور → کندل ۱ساعته تأیید (شکست قاطع یا رد شدن)
    → کندل ۱۵دقیقه تأیید → کندل تأیید ۵دقیقه (پولبک یا رد شدن، با امتیازدهی) → سیگنال."""

    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None

        # ================= سطوح دستی =================
        self.SINGLE_LEVEL_HALF_WIDTH_PCT = 0.125
        self.STALE_LEVELS_HOURS = 30
        self.WARN_EVERY_MIN = 60

        # ================= شکست =================
        self.BREAK_MARGIN_ATR15 = 0.2
        self.BREAK_MIN_BODY_RATIO = 0.5
        self.RETEST_EXPIRE_H = 6
        self.REJECT_EXPIRE_H = 3
        self.WAIT_1H_EXPIRE_H = 2            # مهلت کلوز کندل ۱ساعته بعد از لمس
        self.WAIT_15M_EXPIRE_H = 1           # مهلت کلوز کندل ۱۵دقیقه بعد از تأیید ۱ساعته
        self.APPROACH_FILTER = True
        self.APPROACH_LOOKBACK_C15 = 16
        self.APPROACH_CLEAR_C15 = 8
        self.APPROACH_MIN_DIST_ATR15 = 1.0
        self.SKIP_BLOCK_H = 1
        self.HISTORY_TRIGGER_MAX_AGE_MIN = 10
        self.START_INSIDE_TOL_PCT = 0.03
        self.LOG_ONLY_COOLDOWN_H = 2

        # ================= کندل تأیید ۵دقیقه =================
        self.TOUCH_TOL_ATR5 = 0.15
        self.MIN_RANGE_ATR5 = 0.4
        self.MIN_CONFIRM_SCORE = 3
        self.PIN_WICK_RATIO = 0.5
        self.STRONG_BODY_RATIO = 0.5
        self.CLOSE_EXTREME_RATIO = 0.30
        self.SHALLOW_ATR5 = 0.3
        self.VOL_LOOKBACK = 20
        self.VOL_SPIKE_RATIO = 1.3

        # ================= ستاپ و سیگنال =================
        self.MAX_SETUPS = 2
        self.ZONE_COOLDOWN_H = 2
        self.SIGNAL_COOLDOWN_MIN = 15
        self.MAX_DRIFT_ATR5 = 2.0
        self.COUNTER_TREND_NOTE_IN_TYPE = True

        # ================= حد ضرر / هدف =================
        self.SL_ATR5_BUFFER = 0.5
        self.MIN_RISK_ATR15 = 0.75
        self.MAX_RISK_ATR15 = 4.0
        self.RR1 = 1.5
        self.MIN_ROOM_R = 1.0
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
        _log(f"🚀 {TAG} Started (سطح ۱ساعته > تأیید ۱ساعته > تأیید ۱۵دقیقه > ورود ۵دقیقه)")
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
                self.prev_scan.clear()
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

    def _is_break(self, role, z, c, atr):
        """شکست واقعی: کلوز بیرون لبه‌ی دور زون با فاصله + بدنه‌ی قاطع در جهت شکست."""
        _, _, o, h, l, cl = c
        rng = max(h - l, 1e-12)
        if abs(cl - o) < self.BREAK_MIN_BODY_RATIO * rng:
            return False
        margin = self.BREAK_MARGIN_ATR15 * atr
        if role == 'RES':
            return cl > z['high'] + margin and cl > o
        return cl < z['low'] - margin and cl < o

    def _side_from_history(self, z, price, c5, c15):
        for series in (c5, c15):
            for c in reversed(series):
                if c[5] < z['low']:
                    return 'RES'
                if c[5] > z['high']:
                    return 'SUP'
        return 'RES' if price < (z['low'] + z['high']) / 2 else 'SUP'

    def _fresh_approach(self, lv, role, c15, atr15, ev_ms, price):
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

    def _new_setup(self, symbol, lv, role, ev_ms, price, now_ms, fresh_ok, fresh_why, origin):
        """همه‌ی ستاپ‌ها از WAIT_1H شروع می‌کنند؛ صبر برای کلوز کندل ۱ساعته."""
        z = {'low': lv['low'], 'high': lv['high']}
        st = {'state': 'WAIT_1H', 'role': role, 'zone': z, 'event_ms': ev_ms, 'kind': None,
              'since_ms': ev_ms, 'checked_ms': ev_ms, 'fresh_ok': fresh_ok, 'fresh_why': fresh_why,
              'c1h_txt': '—', 'c15_txt': '—'}
        fa = 'مقاومت' if role == 'RES' else 'حمایت'
        _log(f"🎯 [{TAG}][{symbol}] {origin}: {fa} {self._zs(z)} | قیمت {_fmt(price)} | ثبت شد؛ "
             f"منتظر کلوز کندل ۱ساعته (حداکثر {self.WAIT_1H_EXPIRE_H} ساعت)")
        return st

    # ==========================================================
    # کندل تأیید ۵دقیقه
    # ==========================================================
    def _eval_confirm(self, role, z, c, prev, atr5, relvol):
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

    # =========================================================
