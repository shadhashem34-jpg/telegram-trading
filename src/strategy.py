import asyncio
import time
from datetime import datetime, timezone
from logger import get_trading_logger

try:
    from levels import LEVELS, LEVELS_UPDATED
except Exception as _e:
    LEVELS, LEVELS_UPDATED = {}, None
    _LEVELS_ERROR = repr(_e)
else:
    _LEVELS_ERROR = None

logger = get_trading_logger()

TAG = "V81"


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

        self.SINGLE_LEVEL_HALF_WIDTH_PCT = 0.125
        self.STALE_LEVELS_HOURS = 36
        self.WARN_EVERY_MIN = 60

        self.APPROACH_FILTER = True
        self.APPROACH_LOOKBACK_H1 = 8
        self.APPROACH_CLEAR_H1 = 5
        self.APPROACH_MIN_DIST_ATR1H = 1.0
        self.SKIP_BLOCK_H = 1
        self.LOG_ONLY_COOLDOWN_H = 2

        self.MAX_SETUPS = 2
        self.RETEST_EXPIRE_H = 12
        self.REJECT_EXPIRE_H = 8
        self.ZONE_COOLDOWN_H = 3
        self.SIGNAL_COOLDOWN_MIN = 30
        self.MAX_DRIFT_ATR15 = 1.5

        self.SL_ATR15_BUFFER = 0.5
        self.MIN_RISK_ATR1H = 0.75
        self.MAX_RISK_ATR1H = 4.0
        self.RR = 1.5
        self.MIN_ROOM_R = 0

        self.TRACK_MAX_H = 48

        self.setups = {}
        self.active_setups = self.setups
        self.prev_scan = {}
        self.blocked = {}
        self.log_only_until = {}
        self.warn_until = {}
        self.last_signal_ms = {}
        self.tracked = []
        self.signal_seq = 0
        self.stats = {'n': 0, 'tp': 0, 'sl': 0, 'r': 0.0}

        if _LEVELS_ERROR:
            _log(f"❌ [{TAG}] فایل levels.py خوانده نشد: {_LEVELS_ERROR} — بدون سطح، سیگنالی صادر نمی‌شود")
        else:
            _log(f"✅ [{TAG}] سطوح دستی خوانده شد (به‌روزرسانی: {LEVELS_UPDATED}) | "
                 + " | ".join(f"{s}: {len(v)} زون" for s, v in LEVELS.items()))

    def set_market_data_handler(self, handler):
        self.market_data = handler

    def _now_ms(self):
        return int(time.time() * 1000)

    async def start_monitoring(self):
        self.running = True
        _log(f"🚀 {TAG} Started (Inside-Zone Event Detection)")

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

    def _get_levels(self, symbol):
        out = []
        for item in LEVELS.get(symbol, []):
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
            out.append({'low': lo, 'high': hi, 'mid': (lo + hi) / 2, 'tf': 'دستی', 'score': 0.0})
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
            _log(f"⚠️ [{symbol}] {msg}")

    def _check_warnings(self, symbol, book, price, now_ms):
        if not book:
            self._warn(symbol, 'none', "هیچ سطحی در levels.py برای این نماد نیست؛ سیگنال صادر نمی‌شود", now_ms)
            return
        if not any(b['role'] == 'RES' for b in book):
            self._warn(symbol, 'nores', f"قیمت ({_fmt(price)}) بالاتر از همه‌ی سطوح است؛ مقاومتی نیست. سطوح را به‌روز کن", now_ms)
        if not any(b['role'] == 'SUP' for b in book):
            self._warn(symbol, 'nosup', f"قیمت ({_fmt(price)}) پایین‌تر از همه‌ی سطوح است؛ حمایتی نیست. سطوح را به‌روز کن", now_ms)
        if LEVELS_UPDATED:
            try:
                age_h = (now_ms / 1000 - datetime.strptime(LEVELS_UPDATED, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) / 3600
                if age_h > self.STALE_LEVELS_HOURS:
                    self._warn(symbol, 'stale', f"سطوح {age_h:.0f} ساعت پیش به‌روز شده‌اند. لطفاً به‌روز کن", now_ms)
            except Exception:
                pass

    def _is_blocked(self, symbol, z, now_ms):
        lst = [b for b in self.blocked.get(symbol, []) if b[2] > now_ms]
        self.blocked[symbol] = lst
        tol = z['high'] * 0.001
        return any(not (z['high'] < b[0] - tol or z['low'] > b[1] + tol) for b in lst)

    def _block(self, symbol, z, now_ms, hours=None):
        h = self.ZONE_COOLDOWN_H if hours is None else hours
        self.blocked.setdefault(symbol, []).append((z['low'], z['high'], now_ms + h * 3_600_000))

    def _detect_events(self, symbol, prev, m15c, price, now_ms):
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
            
            # 🛠️ تغییر اصلی: تشخیص قیمت داخل زون
            if pp < lv['low']:
                role = 'RES'
            elif pp > lv['high']:
                role = 'SUP'
            elif lv['low'] <= pp <= lv['high']:
                # قیمت داخل زون است؛ نقش را بر اساس اینکه به کدام سمت نزدیک‌تر است تعیین می‌کنیم (یا به عنوان RES/SUP گذر موقت)
                role = 'RES' if (price > lv['mid']) else 'SUP' # اینجا فرض می‌کنیم اگر بالاتر از وسط است، شاید مقاومت باشد
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

    def _fresh_approach(self, lv, role, h1c, atr1h, ev_ms, price):
        if not self.APPROACH_FILTER:
            return True, "فیلتر خاموش است"
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
        return (role == 'RES' and close > z['high']) or (role == 'SUP' and close < z['low'])

    @staticmethod
    def _zs(z):
        return f"{_fmt(z['low'])}–{_fmt(z['high'])}"

    def _advance(self, symbol, st, trend, price, h1c, m15c, atr15, atr1h, book, now_ms):
        z = st['zone']

        if st['state'] == 'WAIT_1H':
            after = [c for c in h1c if c[1] > st['event_ms']]
            if not after:
                return None
            broke = next((c for c in after if self._beyond(st['role'], z, c[5])), None)
            if broke:
                old = st['role']
                st['role'] = 'SUP' if old == 'RES' else 'RES'
                st.update(state='ARMED', kind='RETEST', since_ms=broke[1], checked_ms=broke[1])
                _log(f"💥 [{symbol}] ۱ساعته: شکست با بدنه ({_fmt(broke[5])}) | زون {self._zs(z)} از {'مقاومت' if old == 'RES' else 'حمایت'} "
                     f"به {'مقاومت' if st['role'] == 'RES' else 'حمایت'} تبدیل شد | منتظر پولبک ۱۵دقیقه تا {self.RETEST_EXPIRE_H} ساعت")
            else:
                st.update(state='ARMED', kind='REJECT', since_ms=st['event_ms'], checked_ms=st['event_ms'])
                _log(f"↩️ [{symbol}] ۱ساعته: نشکست، فقط لمس با سایه | منتظر کندل ۱۵دقیقه‌ی رد شدن تا {self.REJECT_EXPIRE_H} ساعت")

        if st['state'] != 'ARMED':
            return None

        for c in h1c:
            if c[1] > st['since_ms'] and self._beyond(st['role'], z, c[5]):
                old = st['role']
                st['role'] = 'SUP' if old == 'RES' else 'RES'
                st.update(kind='RETEST', since_ms=c[1], checked_ms=c[1])
                _log(f"💥 [{symbol}] شکست ۱ساعته ({_fmt(c[5])}) | زون {self._zs(z)}: {old}→{st['role']} | منتظر پولبک")

        hours = self.RETEST_EXPIRE_H if st['kind'] == 'RETEST' else self.REJECT_EXPIRE_H
        if now_ms - st['since_ms'] > hours * 3_600_000:
            _log(f"⌛ [{symbol}] ستاپ {st['kind']} روی زون {self._zs(z)} بدون سیگنال منقضی شد")
            self._block(symbol, z, now_ms)
            st['state'] = 'DEAD'
            return None

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

        trade = self._build_trade(symbol, direction, entry, z, trig, atr15, atr1h, book)
        if trade is None:
            return None

        self.last_signal_ms[symbol] = now_ms
        self.signal_seq += 1
        sid = self.signal_seq
        kind_t = 'پولبک بعد از شکست' if st['kind'] == 'RETEST' else 'رد شدن از سطح'
        _, _, to, th, tl, tc = trig
        pen = (th - z['low']) if direction == 'SHORT' else (z['high'] - tl)
        _log(f"🏆 [{symbol}] {direction} Entry Triggered! #{sid} ({kind_t}) | کندل ۱۵دقیقه {_ts(trig[0])} UTC: "
             f"O{_fmt(to)} H{_fmt(th)} L{_fmt(tl)} C{_fmt(tc)} | نفوذ داخل زون {pen:.4g} | زون {self._zs(z)}")
        _log(f"📒 JOURNAL | #{sid} | {symbol} | {direction} | {st['kind']} | زون {self._zs(z)} | ورود {_fmt(entry)} "
             f"SL {_fmt(trade['sl'])} TP1 {_fmt(trade['tp1'])} TP2 {_fmt(trade['tp2']) if trade['tp2'] else '—'} | "
             f"ریسک {trade['risk'] / entry * 100:.2f}% = {trade['risk'] / atr1h:.2f} ATR(1H) | فضا تا سطح مخالف {trade['room_txt']} | "
             f"روند {trend} | {_ts(now_ms)} UTC")
        self.send_signal(symbol, direction, entry, trade)
        self._track_add(sid, symbol, direction, entry, trade, trig[1], now_ms)
        self._block(symbol, z, now_ms)
        st['state'] = 'DEAD'
        return True

    def _build_trade(self, symbol, direction, entry, z, trig, atr15, atr1h, book):
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
            'strategy': f'{TAG}_Manual_Levels_1H_15M',
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
                    self._track_close(t, 'SL' if hit_sl else 'TP1', cm, hit_sl and hit_tp)
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

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]):
            _log(f"⚠️ {symbol}: data missing (4H={bool(h4)}, 1H={bool(h1)}, 15m={bool(m15)})")
            return

        now_ms = self._now_ms()
        price = h4['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        atr1h, atr15 = h1['atr'], m15['atr']

        book = self._get_levels(symbol)
        self._pick_active(book, price)
        self._check_warnings(symbol, book, price, now_ms)

        h1c = _tail_candles(h1, now_ms, 60)
        m15c = _tail_candles(m15, now_ms, 60)
        lst = self.setups.setdefault(symbol, [])

        prev = self.prev_scan.get(symbol)
        if prev:
            logged = 0
            for lv, role, ev_ms in self._detect_events(symbol, prev, m15c, price, now_ms):
                if not lv.get('active'):
                    key = (symbol, round(lv['low'], 4))
                    if logged < 5 and self.log_only_until.get(key, 0) <= now_ms:
                        self.log_only_until[key] = now_ms + self.LOG_ONLY_COOLDOWN_H * 3_600_000
                        logged += 1
                        _log(f"📝 [{symbol}] سطح ثبت‌شده (غیرفعال) {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} "
                             f"در {_ts(ev_ms)} لمس/شکسته شد | قیمت {_fmt(price)} (فقط ثبت، بدون ستاپ)")
                    continue

                if len([x for x in lst if x['state'] != 'DEAD']) >= self.MAX_SETUPS:
                    continue
                ok, why = self._fresh_approach(lv, role, h1c, atr1h, ev_ms, price)
                if not ok:
                    self._block(symbol, lv, now_ms, self.SKIP_BLOCK_H)
                    _log(f"🚫 [{symbol}] لمس {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} نادیده گرفته شد: {why}")
                    continue
                st = {'state': 'WAIT_1H', 'role': role, 'zone': {'low': lv['low'], 'high': lv['high']},
                      'event_ms': ev_ms, 'kind': None, 'since_ms': ev_ms, 'checked_ms': ev_ms}
                lst.append(st)
                _log(f"🎯 [{symbol}] TOUCH/BREAK {'مقاومت' if role == 'RES' else 'حمایت'} {self._zs(lv)} "
                     f"در {_ts(ev_ms)} UTC | قیمت {_fmt(price)} | منتظر کلوز ۱ساعته")

        for st in lst:
            if st['state'] != 'DEAD':
                self._advance(symbol, st, trend, price, h1c, m15c, atr15, atr1h, book, now_ms)
        self.setups[symbol] = lst = [x for x in lst if x['state'] != 'DEAD']

        self._track_update(symbol, m15c, now_ms)
        self.prev_scan[symbol] = {'levels': book, 'price': price, 'ms': now_ms}

        def _lv(b):
            mark = {'RES': '▲مقاومت', 'SUP': '▼حمایت'}[b['role']] if b['active'] else ('ر' if b['role'] == 'RES' else 'ح')
            return f"{mark} {self._zs(b)}"
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
        _log(f"📊 {symbol} — {trend} | قیمت {_fmt(price)} | سطوح: " + (" ، ".join(_lv(b) for b in sorted(book, key=lambda b: -b['low'])) or "—")
             + " | ستاپ‌ها: " + (" || ".join(parts) if parts else "—"))
