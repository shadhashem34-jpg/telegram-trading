import asyncio
import time
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

TAG = "V73"


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

        # ---------- حافظه‌ی همه‌ی سطوح ----------
        self.BOOK_LOOKBACKS = (12, 30, 60)  # سطوح ۴ساعته از ۱۲ و ۳۰ و ۶۰ کندل اخیر (اجتماع، بدون تکرار)
        self.DAILY_LOOKBACK = 30            # سطوح روزانه همیشه به حافظه اضافه می‌شوند
        self.MAX_SETUPS = 4                 # حداکثر ستاپ هم‌زمان برای هر نماد
        self.SIGNAL_COOLDOWN_MIN = 30       # فاصله‌ی حداقل بین دو سیگنال یک نماد
        self.BOOK_LOG_MIN = 60              # هر چند دقیقه یک بار کل حافظه‌ی سطوح در لاگ نوشته شود
        self.BOOK_LOG_MAX_DIST_PCT = 15.0   # در لاگ فقط سطوح تا این فاصله (درصد)

        self.setups = {}                   # symbol -> لیست ستاپ‌های فعال (هر سطح یک ستاپ)
        self.active_setups = self.setups   # نام قدیمی (سازگاری)
        self.prev_scan = {}                # symbol -> {'levels','price','ms'} اسکن قبلی (برای تشخیص رویداد)
        self.blocked = {}                  # symbol -> [(low, high, until_ms)]
        self.last_signal_ms = {}
        self.last_book_log = {}

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

    # ---------- ساخت «حافظه‌ی سطوح» از همه‌ی کاندیداها ----------
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
        """همه‌ی سطوح معتبر: ۴ساعته (۳ پنجره) + روزانه + (اگر یک طرف خالی بود) هفتگی."""
        book = []
        self._collect_tf(book, h4, '4H', self.BOOK_LOOKBACKS, price, trend, self.DROP_FORMING_CANDLE)
        if h1d is not None:
            self._collect_tf(book, h1d, '1D', (self.DAILY_LOOKBACK,), price, trend, False)

        has_res = any((b['low'] + b['high']) / 2 > price for b in book)
        has_sup = any((b['low'] + b['high']) / 2 <= price for b in book)
        if h1w is not None and not (has_res and has_sup):
            self._collect_tf(book, h1w, '1W', (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK, self.WEEKLY_LOOKBACK),
                             price, trend, False)

        # امتیاز: تایم‌فریم + تعداد کندل‌های ۴ساعته‌ی اخیر که از سطح گذشته‌اند
        n = len(h4['highs'])
        start = max(0, n - 120)
        weight = {'4H': 2.0, '1D': 4.0, '1W': 6.0}
        for b in book:
            t = sum(1 for i in range(start, n) if h4['highs'][i] >= b['low'] and h4['lows'][i] <= b['high'])
            b['touches'] = t
            b['score'] = weight[b['tf']] + min(t, 8) * 0.5
            b['mid'] = (b['low'] + b['high']) / 2
        return book

    # ==========================================================
    # رویدادها: لمس / شکست هر سطح در حافظه
    # ==========================================================
    def _is_blocked(self, symbol, z, now_ms):
        lst = [b for b in self.blocked.get(symbol, []) if b[2] > now_ms]
        self.blocked[symbol] = lst
        tol = z['high'] * 0.001
        return any(not (z['high'] < b[0] - tol or z['low'] > b[1] + tol) for b in lst)

    def _block(self, symbol, z, now_ms):
        self.blocked.setdefault(symbol, []).append((z['low'], z['high'], now_ms + self.ZONE_COOLDOWN_H * 3_600_000))

    def _detect_events(self, symbol, prev, m15, price, now_ms):
        """هر سطحی که از اسکن قبلی تا الان لمس/رد شده (از سمت درست آمده بوده)."""
        pp = prev['price']
        candles = [c for c in _tail_candles(m15, now_ms, 30) if c[1] > prev['ms']]
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

        if now_ms - self.last_signal_ms.get(symbol, 0) < self.SIGNAL_COOLDOWN_MIN * 60_000:
            _log(f"⏸️ [{symbol}] تریگر {direction} روی {self._zs(z)} دیده شد ولی کمتر از {self.SIGNAL_COOLDOWN_MIN} دقیقه از سیگنال قبلی گذشته")
            return None
        self.last_signal_ms[symbol] = now_ms

        kind_t = 'پولبک بعد از شکست' if st['kind'] == 'RETEST' else 'رد شدن از سطح'
        lv = st.get('level') or {}
        _log(f"🏆 [{symbol}] {direction} Entry Triggered! ({kind_t}) | کندل ۱۵دقیقه {_ts(trig[0])} | زون {self._zs(z)}")
        _log(f"📒 JOURNAL | {symbol} | {direction} | {st['kind']} | سطح {lv.get('tf', '?')} امتیاز={lv.get('score', '?')} "
             f"لمس={lv.get('touches', '?')} | زون {self._zs(z)} | ورود {_fmt(entry)} | روند {trend} | {_ts(now_ms)} UTC")
        self.send_signal(symbol, direction, entry, z, atr4h, trig[3], trig[4])
        self._block(symbol, z, now_ms)
        st['state'] = 'DEAD'
        return True

    # ==========================================================
    def _log_book(self, symbol, book, price):
        near = [b for b in book if abs(b['mid'] - price) / price * 100 <= self.BOOK_LOG_MAX_DIST_PCT]
        res = sorted([b for b in near if b['mid'] > price], key=lambda b: b['low'])
        sup = sorted([b for b in near if b['mid'] <= price], key=lambda b: -b['high'])
        _log(f"🗂️ [{symbol}] حافظه‌ی سطوح (تا {self.BOOK_LOG_MAX_DIST_PCT:.0f}%): مقاومت {len(res)} | حمایت {len(sup)} | قیمت {_fmt(price)}")
        for tag, lst in (("RES", res), ("SUP", sup)):
            for b in lst:
                dist = abs((b['low'] if tag == 'RES' else b['high']) - price) / price * 100
                _log(f"     {tag} {_fmt(b['low'])}–{_fmt(b['high'])} [{b['tf']}] امتیاز={b['score']:.1f} لمس‌های ۴ساعته={b['touches']} فاصله={dist:.2f}%")

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

        book = self._build_book(h4, h1d, h1w, price, trend)
        lst = self.setups.setdefault(symbol, [])

        # --- رویدادهای جدید روی هر سطح در حافظه ---
        prev = self.prev_scan.get(symbol)
        if prev:
            for lv, role, ev_ms in self._detect_events(symbol, prev, m15, price, now_ms):
                if len([x for x in lst if x['state'] != 'DEAD']) >= self.MAX_SETUPS:
                    _log(f"⚠️ [{symbol}] سقف {self.MAX_SETUPS} ستاپ هم‌زمان پر است؛ سطح {self._zs(lv)} نادیده ماند")
                    break
                st = {'state': 'WAIT_1H', 'role': role, 'zone': {'low': lv['low'], 'high': lv['high']},
                      'level': dict(lv), 'event_ms': ev_ms, 'kind': None, 'since_ms': ev_ms, 'checked_ms': ev_ms}
                lst.append(st)
                _log(f"🎯 [{symbol}] TOUCH/BREAK سطح {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} "
                     f"[{lv['tf']} امتیاز {lv['score']:.1f}] در {_ts(ev_ms)} | قیمت {_fmt(price)} | منتظر کلوز ۱ساعته")

        # --- پیشبرد همه‌ی ستاپ‌ها ---
        if lst:
            h1c = _tail_candles(h1, now_ms, 40)
            m15c = _tail_candles(m15, now_ms, 40)
            for st in lst:
                if st['state'] != 'DEAD':
                    self._advance(symbol, st, trend, price, h1c, m15c, m15['atr'], h4['atr'], now_ms)
            self.setups[symbol] = lst = [x for x in lst if x['state'] != 'DEAD']

        # --- ذخیره‌ی حافظه‌ی سطوح برای اسکن بعد ---
        self.prev_scan[symbol] = {'levels': book, 'price': price, 'ms': now_ms}

        # --- لاگ ---
        if now_ms - self.last_book_log.get(symbol, 0) >= self.BOOK_LOG_MIN * 60_000:
            self._log_book(symbol, book, price)
            self.last_book_log[symbol] = now_ms

        res = sorted([b for b in book if b['mid'] > price], key=lambda b: b['low'])
        sup = sorted([b for b in book if b['mid'] <= price], key=lambda b: -b['high'])
        nr = f"{self._zs(res[0])} [{res[0]['tf']}]" if res else "—"
        ns = f"{self._zs(sup[0])} [{sup[0]['tf']}]" if sup else "—"
        parts = []
        tail15 = _tail_candles(m15, now_ms, 1)
        last15 = tail15[-1] if tail15 else None
        for st in lst:
            z = st['zone']
            p = f"{st['state']}/{st['kind'] or 'EVENT'} {st['role']} {self._zs(z)}"
            if st['state'] == 'ARMED' and last15:
                if st['role'] == 'RES':
                    p += f" (نیاز: high≥{_fmt(z['low'])} و close<{_fmt(z['low'])} نزولی | آخرین ۱۵م H{_fmt(last15[3])} C{_fmt(last15[5])})"
                else:
                    p += f" (نیاز: low≤{_fmt(z['high'])} و close>{_fmt(z['high'])} صعودی | آخرین ۱۵م L{_fmt(last15[4])} C{_fmt(last15[5])})"
            parts.append(p)
        _log(f"📊 {symbol} — {trend} | قیمت {_fmt(price)} | سطوح در حافظه: مقاومت {len(res)} / حمایت {len(sup)} | "
             f"نزدیک‌ترین مقاومت {nr} | نزدیک‌ترین حمایت {ns} | ستاپ‌ها: " + (" || ".join(parts) if parts else "—"))

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
