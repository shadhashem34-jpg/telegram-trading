import asyncio
from typing import Optional
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

# ============================================================
#                      تنظیمات (قابل تغییر)
# ============================================================
SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

# --- پیدا کردن سوینگ ---
SWING_N = 3                  # هر سقف/کف باید از N کندل قبل و بعدش بالاتر/پایین‌تر باشد

# --- خوشه‌بندی (ساخت زون) ---
# حداکثر عرض زون = min(CLUSTER_PCT * قیمت ، CLUSTER_ATR_MULT * ATR)
CLUSTER_PCT = 0.003          # ۰.۳٪ (برای ETH=2000 می‌شود ۶ دلار)
CLUSTER_ATR_MULT = 0.5
SYMBOL_CLUSTER_PCT = {       # اگر برای نماد خاصی درصد جدا می‌خواهی، مثلاً {"BTCUSDT": 0.002}
}

# --- اعتبارسنجی ---
MIN_TOUCHES = 2              # حداقل تعداد سوینگ (لمس) داخل زون
REACTION_ATR = 0.7           # واکنش بعد از لمس حداقل چند ATR باشد
REACTION_LOOKAHEAD = 6       # واکنش را در چند کندل بعد از لمس بسنج

# --- لاگ ---
LOG_MAX_DISTANCE_PCT = 10.0  # زون‌های دورتر از این (درصد) فقط شمارش می‌شوند
LOG_MAX_REJECTED = 12        # حداکثر تعداد زون ردشده‌ی نزدیک که با جزئیات لاگ شود
LOG_PIVOT_DETAILS = True     # تاریخ و قیمت هر سوینگ داخل زون لاگ شود
REPORT_INTERVAL_MIN = 60     # گزارش کامل: با هر کندل ۴ساعته‌ی جدید، یا هر X دقیقه

# --- اسکن ---
DELAY_BETWEEN_SYMBOLS = 5
DELAY_BETWEEN_CYCLES = 120

# --- سیگنال‌دهی (فعلاً خاموش؛ فقط نقشه‌ی سطوح) ---
SIGNALS_ENABLED = False


def _fmt(p: float) -> str:
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.3f}"
    return f"{p:.6f}"


def _ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%m-%d %H:%M')


def _price(h4: dict) -> float:
    """قیمت زنده؛ اگر نبود، کلوز آخرین کندل ۴ساعته."""
    return h4.get('price') or h4['close']


class StrategyManager:
    def __init__(self):
        self.running = False
        self.signals_today = 0
        self.max_daily_signals = 2
        self.telegram_bot = None
        self.market_data = None
        self.last_crypto_signal_time = None
        self.watchlist = {}
        self.zones = {}            # آخرین زون‌های معتبر هر نماد (برای مرحله‌ی بعد)
        self._last_report = {}     # symbol -> (last_candle_time, datetime)
        logger.log_system_event("Strategy Initialized (V31.1 - 4H Zone Finder, signals OFF)")

    # ------------------------------------------------------------
    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("Strategy Monitoring Started (4H zone mapping mode)...")

        while self.running:
            for symbol in SCAN_SYMBOLS:
                try:
                    if self.market_data:
                        data = await self.market_data.get_market_data(symbol)
                        if data:
                            signal = await self.generate_signal(symbol, data)
                            if signal and self.telegram_bot:
                                await self.telegram_bot.send_signal(signal)
                        else:
                            logger.log_system_event(f"❌ [V31.1] {symbol} | Data is None (API issue).")
                except Exception as e:
                    logger.log_system_event(f"Error scanning {symbol}: {e}")
                await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)
            await asyncio.sleep(DELAY_BETWEEN_CYCLES)

    async def stop(self):
        self.running = False
        logger.log_system_event("StrategyManager stopped")

    # ------------------------------------------------------------
    #                      هسته‌ی پیدا کردن زون
    # ------------------------------------------------------------
    @staticmethod
    def _find_swings(highs, lows, n):
        """سقف/کف محلی با high و low واقعی. فقط کندل‌های بسته‌شده."""
        swings = []
        for i in range(n, len(highs) - n):
            left_h = max(highs[i - n:i])
            right_h = max(highs[i + 1:i + n + 1])
            if highs[i] > left_h and highs[i] >= right_h:
                swings.append({'i': i, 'price': highs[i], 'kind': 'H'})

            left_l = min(lows[i - n:i])
            right_l = min(lows[i + 1:i + n + 1])
            if lows[i] < left_l and lows[i] <= right_l:
                swings.append({'i': i, 'price': lows[i], 'kind': 'L'})
        return swings

    @staticmethod
    def _reaction(s, highs, lows):
        """میزان حرکت قیمت بعد از لمس (در جهت برگشت)."""
        i = s['i']
        end = min(len(highs), i + 1 + REACTION_LOOKAHEAD)
        if end <= i + 1:
            return 0.0
        if s['kind'] == 'H':
            return s['price'] - min(lows[i + 1:end])
        return max(highs[i + 1:end]) - s['price']

    @staticmethod
    def _cluster(swings, max_width):
        """سوینگ‌ها را بر اساس قیمت مرتب و در خوشه‌هایی با عرض حداکثر max_width جمع می‌کند."""
        swings = sorted(swings, key=lambda s: s['price'])
        clusters, cur = [], []
        for s in swings:
            if cur and s['price'] - cur[0]['price'] > max_width:
                clusters.append(cur)
                cur = [s]
            else:
                cur.append(s)
        if cur:
            clusters.append(cur)
        return clusters

    def _build_zone(self, cluster, highs, lows, atr, price):
        prices = [s['price'] for s in cluster]
        z_low, z_high = min(prices), max(prices)
        reacted = 0
        for s in cluster:
            s['reaction'] = self._reaction(s, highs, lows)
            if atr > 0 and s['reaction'] >= REACTION_ATR * atr:
                reacted += 1

        touches = len(cluster)
        valid, reason = True, ""
        if touches < MIN_TOUCHES:
            valid, reason = False, f"فقط {touches} لمس (حداقل {MIN_TOUCHES})"
        elif reacted < MIN_TOUCHES:
            valid, reason = False, f"واکنش ضعیف: فقط {reacted} از {touches} لمس حرکت ≥ {REACTION_ATR} ATR داشت"

        if z_high < price:
            role, dist = "SUPPORT", price - z_high
        elif z_low > price:
            role, dist = "RESISTANCE", z_low - price
        else:
            role, dist = "INSIDE", 0.0

        return {
            'low': z_low, 'high': z_high,
            'touches': touches, 'reacted': reacted,
            'n_high': sum(1 for s in cluster if s['kind'] == 'H'),
            'n_low': sum(1 for s in cluster if s['kind'] == 'L'),
            'first_i': min(s['i'] for s in cluster),
            'last_i': max(s['i'] for s in cluster),
            'pivots': sorted(cluster, key=lambda s: s['i']),
            'valid': valid, 'reason': reason,
            'role': role, 'dist': dist,
            'dist_pct': dist / price * 100 if price else 0.0,
            'dist_atr': dist / atr if atr else 0.0,
        }

    def analyze_zones(self, symbol, h4):
        highs, lows = h4['highs'], h4['lows']
        price, atr = _price(h4), h4['atr']

        pct = SYMBOL_CLUSTER_PCT.get(symbol, CLUSTER_PCT)
        max_width = price * pct
        if atr > 0:
            max_width = min(max_width, atr * CLUSTER_ATR_MULT)

        swings = self._find_swings(highs, lows, SWING_N)
        clusters = self._cluster(swings, max_width)
        zones = [self._build_zone(c, highs, lows, atr, price) for c in clusters]
        return swings, zones, max_width

    # ------------------------------------------------------------
    #                           لاگ
    # ------------------------------------------------------------
    def _zone_line(self, tag, z, h4, price):
        times = h4['times']
        width = z['high'] - z['low']
        line = (
            f"   {tag} {z['role']} {_fmt(z['low'])} – {_fmt(z['high'])} "
            f"(عرض {_fmt(width)} = {width / price * 100:.2f}%) | "
            f"لمس={z['touches']} (سقف {z['n_high']}/کف {z['n_low']}) واکنش‌دار={z['reacted']} | "
            f"اولین: {_ts(times[z['first_i']])} آخرین: {_ts(times[z['last_i']])} "
            f"({len(times) - 1 - z['last_i']} کندل پیش) | "
            f"فاصله: {z['dist_pct']:.2f}% ({z['dist_atr']:.1f} ATR)"
        )
        if not z['valid']:
            line += f" | ❌ {z['reason']}"
        if LOG_PIVOT_DETAILS:
            pv = "; ".join(f"{p['kind']} {_ts(times[p['i']])} @ {_fmt(p['price'])}" for p in z['pivots'])
            line += f" | سوینگ‌ها: {pv}"
        return line

    def _log_full_report(self, symbol, h4, swings, zones, max_width):
        times = h4['times']
        price, atr = _price(h4), h4['atr']
        n_h = sum(1 for s in swings if s['kind'] == 'H')
        n_l = len(swings) - n_h
        trend = "UP (بالای EMA)" if price > h4['ema_200'] else "DOWN (زیر EMA)"

        near = [z for z in zones if z['dist_pct'] <= LOG_MAX_DISTANCE_PCT]
        far = [z for z in zones if z['dist_pct'] > LOG_MAX_DISTANCE_PCT]
        valid_near = sorted([z for z in near if z['valid']], key=lambda z: z['dist'])
        rej_near = sorted([z for z in near if not z['valid']], key=lambda z: z['dist'])
        valid_far = sum(1 for z in far if z['valid'])

        price_note = "" if h4.get('price_live', True) else " (⚠ قیمت زنده نشد؛ آخرین کلوز ۴ساعته)"
        logger.log_system_event(
            f"🗺️ [V31.1 ZONES] {symbol} | قیمت: {_fmt(price)}{price_note} | ATR(4H): {_fmt(atr)} | "
            f"EMA{h4.get('ema_period_used', 200)}: {_fmt(h4['ema_200'])} → {trend}"
        )
        logger.log_system_event(
            f"   دیتا: {h4['count']} کندل ۴ساعته (UTC) از {_ts(times[0])} تا {_ts(times[-1])} | "
            f"N={SWING_N} | حداکثر عرض زون: {_fmt(max_width)} ({max_width / price * 100:.2f}%)"
        )
        logger.log_system_event(
            f"   سوینگ‌ها: {len(swings)} (سقف {n_h} / کف {n_l}) → خوشه‌ها: {len(zones)} | "
            f"معتبر: {sum(1 for z in zones if z['valid'])} | ردشده: {sum(1 for z in zones if not z['valid'])}"
        )

        logger.log_system_event(f"   ✅ زون‌های معتبر (تا {LOG_MAX_DISTANCE_PCT:.0f}% فاصله): {len(valid_near)}")
        for idx, z in enumerate(valid_near, 1):
            logger.log_system_event(self._zone_line(f"✅Z{idx}", z, h4, price))

        shown = rej_near[:LOG_MAX_REJECTED]
        logger.log_system_event(f"   ⛔ زون‌های ردشده‌ی نزدیک: {len(rej_near)} (نمایش {len(shown)})")
        for idx, z in enumerate(shown, 1):
            logger.log_system_event(self._zone_line(f"⛔R{idx}", z, h4, price))

        logger.log_system_event(
            f"   دورتر از {LOG_MAX_DISTANCE_PCT:.0f}%: {len(far)} زون (معتبر: {valid_far}, ردشده: {len(far) - valid_far})"
        )

    def _log_radar(self, symbol, price, zones):
        valid = [z for z in zones if z['valid']]
        sup = sorted([z for z in valid if z['role'] == "SUPPORT"], key=lambda z: z['dist'])
        res = sorted([z for z in valid if z['role'] == "RESISTANCE"], key=lambda z: z['dist'])
        inside = [z for z in valid if z['role'] == "INSIDE"]
        s_txt = f"{_fmt(sup[0]['low'])}–{_fmt(sup[0]['high'])} ({sup[0]['dist_pct']:.2f}%)" if sup else "—"
        r_txt = f"{_fmt(res[0]['low'])}–{_fmt(res[0]['high'])} ({res[0]['dist_pct']:.2f}%)" if res else "—"
        extra = f" | داخل زون: {_fmt(inside[0]['low'])}–{_fmt(inside[0]['high'])}" if inside else ""
        logger.log_system_event(
            f"📡 [V31.1 RADAR] {symbol} | {_fmt(price)} | نزدیک‌ترین حمایت: {s_txt} | نزدیک‌ترین مقاومت: {r_txt}{extra}"
        )

    # ------------------------------------------------------------
    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        h4 = market_data.get('h4')
        if not h4 or h4.get('count', 0) < 50:
            return None

        swings, zones, max_width = self.analyze_zones(symbol, h4)
        self.zones[symbol] = [z for z in zones if z['valid']]

        now = datetime.now(timezone.utc)
        last_candle = h4['times'][-1]
        prev = self._last_report.get(symbol)
        due = (
            prev is None
            or prev[0] != last_candle
            or (now - prev[1]).total_seconds() / 60 >= REPORT_INTERVAL_MIN
        )

        if due:
            self._log_full_report(symbol, h4, swings, zones, max_width)
            self._last_report[symbol] = (last_candle, now)
        else:
            self._log_radar(symbol, _price(h4), zones)

        if not SIGNALS_ENABLED:
            return None

        return None  # مرحله‌ی بعد: شکست / پولبک روی زون‌های بالا
