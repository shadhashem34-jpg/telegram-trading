import asyncio
import time
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

TAG = "V74"


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

        # ================= سطح‌یابی (دست‌نخورده) =================
        self.LOOKBACK_CANDLES = 12
        self.MAX_LOOKBACK = 30
        self.WEEKLY_LOOKBACK = 60          # اگر ۴ساعته و روزانه سطح پیدا نکردند، ۶۰ کندل هفتگی
        self.ZONE_WIDTH_PCT = 0.0025
        self.DROP_FORMING_CANDLE = 'auto'
        self.BOOK_LOOKBACKS = (12, 30)      # سطوح فقط از ۴ساعته؛ حافظه = اجتماع پنجره‌های ۱۲ و ۳۰ کندل (بدون تکرار)
        self.DAILY_LOOKBACKS = (12, 30)     # فقط اگر یک طرف در ۴ساعته سطحی نداشت (مثل V71)

        # ================= امتیازدهی سطوح =================
        self.SCORE_WINDOW = 120             # امتیاز از روی ۱۲۰ کندل ۴ساعته‌ی اخیر
        self.REACTION_ATR = 1.0             # «برگشت واقعی» = بعد از لمس، حداقل این‌قدر ATR(4H) دور شدن
        self.REACTION_LOOKAHEAD = 6         # برگشت را در این‌قدر کندل بعد از لمس بسنج
        self.RECENT_REACTION_CANDLES = 30   # برگشت «تازه» = در این‌قدر کندل اخیر
        self.TF_WEIGHT = {'4H': 1.0, '1D': 2.0, '1W': 3.0}
        self.NEAR_MAX_ATR = 4.0             # سطح «نزدیک»: تا این‌قدر ATR(4H) از قیمت ...
        self.NEAR_MAX_PCT = 6.0             # ... و حداکثر این‌قدر درصد (کمتر از این دو)
        self.DIST_PENALTY_PER_ATR = 0.75    # هر ATR فاصله این‌قدر از امتیاز انتخاب کم می‌کند

        # ================= رویداد (لمس/شکست) =================
        self.APPROACH_LOOKBACK_H1 = 8       # قیمت باید در این‌قدر کندل ۱ساعته‌ی قبل ...
        self.APPROACH_CLEAR_H1 = 5          # ... و در ۵ کندل آخرشان اصلاً به سطح نخورده باشد
        self.APPROACH_MIN_DIST_ATR1H = 1.0  # ... و حداقل این‌قدر ATR(1H) از سطح دور شده باشد
        self.SKIP_BLOCK_H = 1               # لمس بی‌اعتبار: همان سطح این‌قدر ساعت دوباره بررسی نشود
        self.LOG_ONLY_COOLDOWN_H = 2        # لاگ لمس سطوح غیرفعال: هر سطح هر X ساعت یک بار

        # ================= ستاپ =================
        self.MAX_SETUPS = 2                 # هر طرف (مقاومت/حمایت) یک ستاپ
        self.RETEST_EXPIRE_H = 12           # مهلت پولبک بعد از «شکست» سطح (ساعت)
        self.REJECT_EXPIRE_H = 8            # مهلت سیگنال بعد از «رد شدن» از سطح (ساعت)
        self.ZONE_COOLDOWN_H = 3            # بعد از سیگنال/انقضا همان زون تا این‌قدر فعال نشود
        self.SIGNAL_COOLDOWN_MIN = 30       # فاصله‌ی حداقل بین دو سیگنال یک نماد
        self.MAX_DRIFT_ATR15 = 1.5          # اگر قیمت زنده بیشتر از این‌قدر ATR(15m) از ورود دور شد، رد شود

        # ================= حد ضرر / هدف =================
        self.SL_ATR15_BUFFER = 0.5          # SL = سایه‌ی کندل یا لبه‌ی زون (هرکدام دورتر) ± این‌قدر ATR(15m)
        self.MIN_RISK_ATR1H = 0.75          # حداقل ریسک (برحسب ATR ۱ساعته)
        self.MAX_RISK_ATR1H = 2.0           # بیشتر از این ریسک → سیگنال نمی‌دهد
        self.RR = 1.5                       # TP1 = این‌قدر برابر ریسک
        self.MIN_ROOM_R = 0                 # فیلتر فضا (فعلاً خاموش): اگر سطح مخالف نزدیک‌تر از این‌قدر برابر ریسک بود، سیگنال نده (مثلاً 1.5)

        # ================= ردیابی نتیجه =================
        self.TRACK_MAX_H = 48

        # ================= لاگ =================
        self.BOOK_LOG_MIN = 60
        self.BOOK_LOG_MAX_DIST_PCT = 15.0

        self.setups = {}                   # symbol -> لیست ستاپ‌ها
        self.active_setups = self.setups   # نام قدیمی (سازگاری)
        self.prev_scan = {}                # symbol -> {'levels','price','ms'}
        self.blocked = {}                  # symbol -> [(low, high, until_ms)]
        self.log_only_until = {}           # (symbol, round(low,2)) -> until_ms
        self.last_signal_ms = {}
        self.last_book_log = {}
        self.tracked = []                  # سیگنال‌های در حال ردیابی
        self.signal_seq = 0
        self.stats = {'n': 0, 'tp': 0, 'sl': 0, 'r': 0.0}

    def set_market_data_handler(self, handler):
        self.market_data = handler

    def _now_ms(self):
        return int(time.time() * 1000)

    async def start_monitoring(self):
        self.running = True
        _log(f"🚀 {TAG} Started (Trend > Scored Levels > Touch/Break > 1H > 15M)")
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
    # منطق سطح‌یابی (دست‌نخورده)
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

    # ---------- حافظه‌ی همه‌ی سطوح ----------
    @staticmethod
    def _prep(data, price, drop_forming):
        o, h, l, c = data['opens'], data['highs'], data['lows'], data['closes']
        if drop_forming == 'auto' and abs(c[-1] - price) <= price * 0.0002:
            o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]
        return o, h, l, c

    def _zones_from_cands(self, cands, side):
        w = self.ZONE_WIDTH_PCT
        out = []
        for body, wick in cands:
            if side == 'RES':
                out.append({'low': body, 'high': min(max(wick, body), body * (1 + w))})
            else:
                out.append({'low': max(min(wick, body), body * (1 - w)), 'high': body})
        return out

    @staticmethod
    def _add_levels(book, zones, tf, price):
        tol = price * 0.001
        for z in zones:
            if any(not (z['high'] < b['low'] - tol or z['low'] > b['high'] + tol) for b in book):
                continue
            book.append({'low': z['low'], 'high': z['high'], 'tf': tf})

    def _collect_tf(self, book, data, tf, lookbacks, price, trend, drop_forming):
        o, h, l, c = self._prep(data, price, drop_forming)
        for lb in lookbacks:
            r, s = self._collect_candidates(o, h, l, c, price, lb, trend)
            self._add_levels(book, self._zones_from_cands(r, 'RES'), tf, price)
            self._add_levels(book, self._zones_from_cands(s, 'SUP'), tf, price)

    def _build_book(self, h4, h1d, h1w, price, trend):
        """سطوح فقط از ۴ساعته. روزانه و بعد هفتگی (۶۰ کندل) فقط برای طرفی که ۴ساعته سطحی نداشت."""
        book = []
        self._collect_tf(book, h4, '4H', self.BOOK_LOOKBACKS, price, trend, self.DROP_FORMING_CANDLE)

        for data, tf, lbs in ((h1d, '1D', self.DAILY_LOOKBACKS),
                              (h1w, '1W', (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK, self.WEEKLY_LOOKBACK))):
            if data is None:
                continue
            has_res = any((b['low'] + b['high']) / 2 > price for b in book)
            has_sup = any((b['low'] + b['high']) / 2 <= price for b in book)
            if has_res and has_sup:
                break
            extra = []
            self._collect_tf(extra, data, tf, lbs, price, trend, False)
            for b in extra:
                mid = (b['low'] + b['high']) / 2
                if (mid > price and not has_res) or (mid <= price and not has_sup):
                    self._add_levels(book, [b], tf, price)

        for b in book:
            b['mid'] = (b['low'] + b['high']) / 2
        return book

    # ---------- امتیازدهی ----------
    def _score_book(self, book, h4):
        """امتیاز = وزن تایم‌فریم + برگشت‌های واقعی (حداکثر ۵) + تازگی + نقش‌عوض‌کردگی."""
        H, L, C = h4['highs'], h4['lows'], h4['closes']
        n = len(C)
        start = max(1, n - self.SCORE_WINDOW)
        atr = h4['atr']
        for b in book:
            lo, hi = b['low'], b['high']

            # برگشت واقعی: قیمت از یک سمت آمده، زون را لمس کرده و دوباره از همان سمت ≥ ۱ ATR دور شده
            reactions, last_react = 0, None
            i = start
            while i < n:
                if H[i] >= lo and L[i] <= hi:
                    j = i
                    while j + 1 < n and H[j + 1] >= lo and L[j + 1] <= hi:
                        j += 1
                    if j + 1 < n:
                        end = min(n, j + 1 + self.REACTION_LOOKAHEAD)
                        came_from_below = C[i - 1] < lo
                        move = (lo - min(L[j + 1:end])) if came_from_below else (max(H[j + 1:end]) - hi)
                        if atr > 0 and move >= self.REACTION_ATR * atr:
                            reactions += 1
                            last_react = j
                    i = j + 1
                else:
                    i += 1

            # نقش‌عوض‌کردگی: زون قبلاً نقش مخالف داشته (حمایتی که مقاومت شده یا برعکس)
            flipped = False
            above = below = False
            for k in range(start, n):
                if C[k] > hi:
                    above = True
                    if below:
                        flipped = True
                elif C[k] < lo:
                    below = True
                    if above:
                        flipped = True

            recent = last_react is not None and (n - 1 - last_react) <= self.RECENT_REACTION_CANDLES
            b['reactions'] = reactions
            b['recent'] = recent
            b['flipped'] = flipped
            b['score'] = (self.TF_WEIGHT.get(b['tf'], 1.0) + min(reactions, 5) * 1.0
                          + (1.0 if recent else 0.0) + (1.5 if flipped else 0.0))

    def _pick_active(self, book, price, atr4h):
        """هر طرف فقط یک سطح فعال: بهترین (امتیاز − جریمه‌ی فاصله) در محدوده‌ی نزدیک؛ وگرنه نزدیک‌ترین."""
        for b in book:
            if b['mid'] > price:
                b['role'], b['dist'] = 'RES', max(0.0, b['low'] - price)
            else:
                b['role'], b['dist'] = 'SUP', max(0.0, price - b['high'])
            b['dist_atr'] = b['dist'] / atr4h if atr4h else 0.0
            b['active'] = False
            b['why'] = ''
        limit = self.NEAR_MAX_PCT / 100 * price
        if atr4h:
            limit = min(limit, self.NEAR_MAX_ATR * atr4h)
        for role in ('RES', 'SUP'):
            side = [b for b in book if b['role'] == role]
            if not side:
                continue
            near = [b for b in side if b['dist'] <= limit]
            if near:
                pick = max(near, key=lambda b: b['score'] - self.DIST_PENALTY_PER_ATR * b['dist_atr'])
                pick['why'] = 'near'
            else:
                pick = min(side, key=lambda b: b['dist'])
                pick['why'] = 'fallback'
            pick['active'] = True

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

    def _detect_events(self, symbol, prev, m15c, price, now_ms):
        """هر سطحی از اسکن قبلی که تا الان لمس/رد شده (از سمت درست آمده بوده)."""
        pp = prev['price']
        candles = [c for c in m15c if c[1] > prev['ms']]
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
        found.sort(key=lambda x: (x[2], -x[0]['score']))
        return found

    def _fresh_approach(self, lv, role, h1c, atr1h, ev_ms, price):
        """لمس معتبر = قیمت از دور آمده؛ نه اینکه ساعت‌ها کنار سطح چرخیده باشد."""
        prior = [c for c in h1c if c[1] <= ev_ms][-self.APPROACH_LOOKBACK_H1:]
        if len(prior) < self.APPROACH_CLEAR_H1:
            return True, "دیتای ۱ساعته کافی نیست؛ بدون فیلتر"
        tol = price * 0.0005
        clear = prior[-self.APPROACH_CLEAR_H1:]
        if role == 'RES':
            touched = any(c[3] >= lv['low'] - tol for c in clear)
            far = lv['low'] - min(c[4] for c in prior)
        else:
            touched = any(c[4] <= lv['high'] + tol for c in clear)
            far = max(c[3] for c in prior) - lv['high']
        if touched:
            return False, f"قیمت در {self.APPROACH_CLEAR_H1} ساعت اخیر کنار سطح بوده"
        if atr1h and far < self.APPROACH_MIN_DIST_ATR1H * atr1h:
            return False, f"دورترین فاصله‌ی قیمت از سطح فقط {far / atr1h:.2f} ATR(1H) بوده"
        return True, "ok"

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
    def _advance(self, symbol, st, trend, price, h1c, m15c, atr15, atr1h, atr4h, book, now_ms):
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

        if now_ms - self.last_signal_ms.get(symbol, 0) < self.SIGNAL_COOLDOWN_MIN * 60_000:
            _log(f"⏸️ [{symbol}] تریگر {direction} روی {self._zs(z)} دیده شد ولی کمتر از {self.SIGNAL_COOLDOWN_MIN} دقیقه از سیگنال قبلی گذشته")
            return None

        trade = self._build_trade(symbol, direction, entry, z, trig, atr15, atr1h, atr4h, book)
        if trade is None:
            return None

        self.last_signal_ms[symbol] = now_ms
        self.signal_seq += 1
        sid = self.signal_seq
        kind_t = 'پولبک بعد از شکست' if st['kind'] == 'RETEST' else 'رد شدن از سطح'
        lv = st.get('level') or {}
        _log(f"🏆 [{symbol}] {direction} Entry Triggered! #{sid} ({kind_t}) | کندل ۱۵دقیقه {_ts(trig[0])} | زون {self._zs(z)}")
        _log(f"📒 JOURNAL | #{sid} | {symbol} | {direction} | {st['kind']} | سطح {lv.get('tf', '?')} امتیاز={lv.get('score', 0):.1f} "
             f"برگشت‌ها={lv.get('reactions', '?')} نقش‌عوض={lv.get('flipped', '?')} | زون {self._zs(z)} | ورود {_fmt(entry)} "
             f"SL {_fmt(trade['sl'])} TP1 {_fmt(trade['tp1'])} TP2 {_fmt(trade['tp2']) if trade['tp2'] else '—'} | "
             f"ریسک {trade['risk'] / entry * 100:.2f}% = {trade['risk'] / atr1h:.2f} ATR(1H) | فضا تا سطح مخالف {trade['room_txt']} | "
             f"روند {trend} | {_ts(now_ms)} UTC")
        self.send_signal(symbol, direction, entry, trade)
        self._track_add(sid, symbol, direction, entry, trade, trig[1], now_ms)
        self._block(symbol, z, now_ms)
        st['state'] = 'DEAD'
        return True

    # ==========================================================
    # حد ضرر / هدف
    # ==========================================================
    def _build_trade(self, symbol, direction, entry, z, trig, atr15, atr1h, atr4h, book):
        buf = self.SL_ATR15_BUFFER * atr15
        if direction == 'SHORT':
            sl = max(z['high'], trig[3]) + buf
        else:
            sl = min(z['low'], trig[4]) - buf
        risk = abs(entry - sl)

        if atr1h:
            min_risk, max_risk = self.MIN_RISK_ATR1H * atr1h, self.MAX_RISK_ATR1H * atr1h
            if risk > max_risk:
                _log(f"🚫 [{symbol}] {direction} روی {self._zs(z)} رد شد: ریسک {risk / atr1h:.2f} ATR(1H) از سقف {self.MAX_RISK_ATR1H} بیشتر است "
                     f"(ورود {_fmt(entry)} SL {_fmt(sl)})")
                return None
            if risk < min_risk:
                risk = min_risk
                sl = entry + risk if direction == 'SHORT' else entry - risk
        if risk <= 0:
            return None

        # سطح مخالف بعدی (برای فضای حرکت و TP2)
        if direction == 'SHORT':
            opp = [b for b in book if b['high'] < entry - 1e-9 and b['mid'] < z['low']]
            nxt = max(opp, key=lambda b: b['high'])['high'] if opp else None
            room = (entry - nxt) if nxt is not None else None
        else:
            opp = [b for b in book if b['low'] > entry + 1e-9 and b['mid'] > z['high']]
            nxt = min(opp, key=lambda b: b['low'])['low'] if opp else None
            room = (nxt - entry) if nxt is not None else None

        if self.MIN_ROOM_R and room is not None and room < self.MIN_ROOM_R * risk:
            _log(f"🚫 [{symbol}] {direction} روی {self._zs(z)} رد شد: سطح مخالف فقط {room / risk:.2f}R آن‌طرف‌تر است "
                 f"({_fmt(nxt)}؛ حداقل {self.MIN_ROOM_R}R لازم است)")
            return None

        tp1 = entry - risk * self.RR if direction == 'SHORT' else entry + risk * self.RR
        tp2 = nxt if nxt is not None else 0
        return {'sl': sl, 'tp1': tp1, 'tp2': tp2, 'risk': risk,
                'room_txt': (f"{room / risk:.2f}R ({_fmt(nxt)})" if room is not None else "سطحی نیست")}

    def send_signal(self, symbol, direction, entry, trade):
        signal = {
            'symbol': symbol,
            'type': 'SELL' if direction == 'SHORT' else 'BUY',
            'direction': direction,
            'entry_price': entry,
            'stop_loss': round(trade['sl'], 4),
            'take_profit_1': round(trade['tp1'], 4),
            'take_profit_2': round(trade['tp2'], 4) if trade['tp2'] else 0,
            'take_profit_3': 0,
            'strategy': f'{TAG}_4H_Levels_1H_15M',
        }
        _log(f"📤 [{symbol}] SIGNAL {signal['type']} | Entry {_fmt(entry)} | SL {_fmt(trade['sl'])} | "
             f"TP1 {_fmt(trade['tp1'])} | TP2 {_fmt(trade['tp2']) if trade['tp2'] else '—'}")

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

    # ==========================================================
    # ردیابی نتیجه‌ی سیگنال‌ها (برای بک‌تست زنده)
    # ==========================================================
    def _track_add(self, sid, symbol, direction, entry, trade, start_ms, now_ms):
        self.tracked.append({
            'id': sid, 'symbol': symbol, 'direction': direction, 'entry': entry,
            'sl': trade['sl'], 'tp': trade['tp1'], 'risk': trade['risk'],
            'start_ms': start_ms, 'created_ms': now_ms, 'last_cm': start_ms,
            'mfe': 0.0, 'mae': 0.0, 'status': 'OPEN',
        })

    def _track_update(self, symbol, m15c, now_ms):
        for t in self.tracked:
            if t['symbol'] != symbol or t['status'] != 'OPEN':
                continue
            for (ot, cm, o, h, l, c) in m15c:
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
                    amb = hit_sl and hit_tp   # هر دو در یک کندل: محتاطانه SL حساب می‌شود
                    self._track_close(t, 'SL' if hit_sl else 'TP1', cm, amb)
                    break
            if t['status'] == 'OPEN' and now_ms - t['created_ms'] > self.TRACK_MAX_H * 3_600_000:
                self._track_close(t, 'OPEN-EXPIRED', now_ms, False)

    def _track_close(self, t, result, ms, ambiguous):
        t['status'] = result
        r = self.RR if result == 'TP1' else (-1.0 if result == 'SL' else 0.0)
        if result in ('TP1', 'SL'):
            self.stats['n'] += 1
            self.stats['tp' if result == 'TP1' else 'sl'] += 1
            self.stats['r'] += r
        hours = (ms - t['created_ms']) / 3_600_000
        _log(f"📒 RESULT | #{t['id']} | {t['symbol']} | {t['direction']} | {result}{' (هر دو در یک کندل؛ SL فرض شد)' if ambiguous else ''} | "
             f"R={r:+.2f} | بیشترین پیشروی {t['mfe']:.2f}R / بیشترین برگشت {t['mae']:.2f}R | مدت {hours:.1f}h | "
             f"ورود {_fmt(t['entry'])} SL {_fmt(t['sl'])} TP {_fmt(t['tp'])}")
        s = self.stats
        if s['n']:
            _log(f"📈 آمار کل: {s['n']} معامله | TP {s['tp']} | SL {s['sl']} | وین‌ریت {s['tp'] / s['n'] * 100:.0f}% | جمع R = {s['r']:+.2f}")
        self.tracked = [x for x in self.tracked if x['status'] == 'OPEN']

    # ==========================================================
    # لاگ
    # ==========================================================
    def _log_book(self, symbol, book, price):
        near = [b for b in book if abs(b['mid'] - price) / price * 100 <= self.BOOK_LOG_MAX_DIST_PCT]
        res = sorted([b for b in near if b['role'] == 'RES'], key=lambda b: b['low'])
        sup = sorted([b for b in near if b['role'] == 'SUP'], key=lambda b: -b['high'])
        _log(f"🗂️ [{symbol}] حافظه‌ی سطوح (تا {self.BOOK_LOG_MAX_DIST_PCT:.0f}%): مقاومت {len(res)} | حمایت {len(sup)} | قیمت {_fmt(price)}")
        for tag, lst in (("RES", res), ("SUP", sup)):
            for b in lst:
                mark = "⭐فعال" if b['active'] else "     "
                _log(f"   {mark} {tag} {_fmt(b['low'])}–{_fmt(b['high'])} [{b['tf']}] امتیاز={b['score']:.1f} "
                     f"برگشت‌ها={b['reactions']} {'نقش‌عوض ' if b['flipped'] else ''}{'تازه ' if b['recent'] else ''}"
                     f"فاصله={b['dist'] / price * 100:.2f}% ({b['dist_atr']:.1f} ATR)")

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
        atr4h, atr1h, atr15 = h4['atr'], h1['atr'], m15['atr']

        book = self._build_book(h4, h1d, h1w, price, trend)
        self._score_book(book, h4)
        self._pick_active(book, price, atr4h)

        h1c = _tail_candles(h1, now_ms, 60)
        m15c = _tail_candles(m15, now_ms, 60)
        lst = self.setups.setdefault(symbol, [])

        # --- رویدادهای جدید ---
        prev = self.prev_scan.get(symbol)
        if prev:
            logged = 0
            for lv, role, ev_ms in self._detect_events(symbol, prev, m15c, price, now_ms):
                if not lv.get('active'):
                    key = (symbol, round(lv['low'], 2))
                    if logged < 5 and self.log_only_until.get(key, 0) <= now_ms:
                        self.log_only_until[key] = now_ms + self.LOG_ONLY_COOLDOWN_H * 3_600_000
                        logged += 1
                        _log(f"📝 [{symbol}] سطح ثبت‌شده (غیرفعال) {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} "
                             f"[{lv['tf']} امتیاز {lv['score']:.1f}] در {_ts(ev_ms)} لمس/شکسته شد | قیمت {_fmt(price)} (فقط ثبت، بدون ستاپ)")
                    continue

                if len([x for x in lst if x['state'] != 'DEAD']) >= self.MAX_SETUPS:
                    continue
                ok, why = self._fresh_approach(lv, role, h1c, atr1h, ev_ms, price)
                if not ok:
                    self._block(symbol, lv, now_ms, self.SKIP_BLOCK_H)
                    _log(f"🚫 [{symbol}] لمس {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} نادیده گرفته شد: {why}")
                    continue
                st = {'state': 'WAIT_1H', 'role': role, 'zone': {'low': lv['low'], 'high': lv['high']},
                      'level': dict(lv), 'event_ms': ev_ms, 'kind': None, 'since_ms': ev_ms, 'checked_ms': ev_ms}
                lst.append(st)
                _log(f"🎯 [{symbol}] TOUCH/BREAK سطح فعال {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} "
                     f"[{lv['tf']} امتیاز {lv['score']:.1f} برگشت‌ها {lv['reactions']}] در {_ts(ev_ms)} | قیمت {_fmt(price)} | منتظر کلوز ۱ساعته")

        # --- پیشبرد ستاپ‌ها ---
        for st in lst:
            if st['state'] != 'DEAD':
                self._advance(symbol, st, trend, price, h1c, m15c, atr15, atr1h, atr4h, book, now_ms)
        self.setups[symbol] = lst = [x for x in lst if x['state'] != 'DEAD']

        # --- نتیجه‌ی سیگنال‌های قبلی ---
        self._track_update(symbol, m15c, now_ms)

        # --- ذخیره برای اسکن بعد ---
        self.prev_scan[symbol] = {'levels': book, 'price': price, 'ms': now_ms}

        # --- لاگ ---
        if now_ms - self.last_book_log.get(symbol, 0) >= self.BOOK_LOG_MIN * 60_000:
            self._log_book(symbol, book, price)
            self.last_book_log[symbol] = now_ms

        act = {b['role']: b for b in book if b['active']}
        def _a(role):
            b = act.get(role)
            return f"{self._zs(b)} [{b['tf']} s={b['score']:.1f}{' ⚠دور' if b['why'] == 'fallback' else ''}]" if b else "—"
        tail15 = _tail_candles(m15, now_ms, 1)
        last15 = tail15[-1] if tail15 else None
        parts = []
        for st in lst:
            z = st['zone']
            p = f"{st['state']}/{st['kind'] or 'EVENT'} {st['role']} {self._zs(z)}"
            if st['state'] == 'ARMED' and last15:
                if st['role'] == 'RES':
                    p += f" (نیاز: high≥{_fmt(z['low'])} و close<{_fmt(z['low'])} نزولی | آخرین ۱۵م H{_fmt(last15[3])} C{_fmt(last15[5])})"
                else:
                    p += f" (نیاز: low≤{_fmt(z['high'])} و close>{_fmt(z['high'])} صعودی | آخرین ۱۵م L{_fmt(last15[4])} C{_fmt(last15[5])})"
            parts.append(p)
        _log(f"📊 {symbol} — {trend} | قیمت {_fmt(price)} | سطوح در حافظه {len(book)} | فعال: مقاومت {_a('RES')} / حمایت {_a('SUP')} | "
             f"ستاپ‌ها: " + (" || ".join(parts) if parts else "—"))
