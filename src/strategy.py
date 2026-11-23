import asyncio

import time

from datetime import datetime, timezone

from logger import get_trading_logger

try:
    from levels import LEVELS as MANUAL_LEVELS
except Exception:
    MANUAL_LEVELS = {}



logger = get_trading_logger()



TAG = "V75"





def _log(msg):

    logger.log_system_event(msg)





def _fmt(p):

    if p is None:

        return "â€”"

    if p >= 100:

        return f"{p:.2f}"

    if p >= 1:

        return f"{p:.3f}"

    return f"{p:.6f}"





def _ts(ms):

    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%m-%d %H:%M')





def _tail_candles(tf, now_ms, tail=40):

    """Ú©Ù†Ø¯Ù„â€ŒÙ‡Ø§ÛŒ Ø¨Ø³ØªÙ‡â€ŒØ´Ø¯Ù‡: (open_ms, close_ms, o, h, l, c) â€” ÙÙ‚Ø· tail ØªØ§ÛŒ Ø¢Ø®Ø±."""

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



        # ================= Ø³Ø·Ø­â€ŒÛŒØ§Ø¨ÛŒ (Ø¯Ø³Øªâ€ŒÙ†Ø®ÙˆØ±Ø¯Ù‡) =================

        self.LOOKBACK_CANDLES = 12

        self.MAX_LOOKBACK = 30

        self.WEEKLY_LOOKBACK = 60          # Ø§Ú¯Ø± Û´Ø³Ø§Ø¹ØªÙ‡ Ùˆ Ø±ÙˆØ²Ø§Ù†Ù‡ Ø³Ø·Ø­ Ù¾ÛŒØ¯Ø§ Ù†Ú©Ø±Ø¯Ù†Ø¯ØŒ Û¶Û° Ú©Ù†Ø¯Ù„ Ù‡ÙØªÚ¯ÛŒ

        self.ZONE_WIDTH_PCT = 0.0025

        self.DROP_FORMING_CANDLE = 'auto'

        self.BOOK_LOOKBACKS = (12, 30)      # Ø³Ø·ÙˆØ­ ÙÙ‚Ø· Ø§Ø² Û´Ø³Ø§Ø¹ØªÙ‡Ø› Ø­Ø§ÙØ¸Ù‡ = Ø§Ø¬ØªÙ…Ø§Ø¹ Ù¾Ù†Ø¬Ø±Ù‡â€ŒÙ‡Ø§ÛŒ Û±Û² Ùˆ Û³Û° Ú©Ù†Ø¯Ù„ (Ø¨Ø¯ÙˆÙ† ØªÚ©Ø±Ø§Ø±)

        self.DAILY_LOOKBACKS = (12, 30)     # ÙÙ‚Ø· Ø§Ú¯Ø± ÛŒÚ© Ø·Ø±Ù Ø¯Ø± Û´Ø³Ø§Ø¹ØªÙ‡ Ø³Ø·Ø­ÛŒ Ù†Ø¯Ø§Ø´Øª (Ù…Ø«Ù„ V71)



        # ================= Ø§Ù…ØªÛŒØ§Ø²Ø¯Ù‡ÛŒ Ø³Ø·ÙˆØ­ =================

        self.SCORE_WINDOW = 120             # Ø§Ù…ØªÛŒØ§Ø² Ø§Ø² Ø±ÙˆÛŒ Û±Û²Û° Ú©Ù†Ø¯Ù„ Û´Ø³Ø§Ø¹ØªÙ‡â€ŒÛŒ Ø§Ø®ÛŒØ±

        self.REACTION_ATR = 1.0             # Â«Ø¨Ø±Ú¯Ø´Øª ÙˆØ§Ù‚Ø¹ÛŒÂ» = Ø¨Ø¹Ø¯ Ø§Ø² Ù„Ù…Ø³ØŒ Ø­Ø¯Ø§Ù‚Ù„ Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± ATR(4H) Ø¯ÙˆØ± Ø´Ø¯Ù†

        self.REACTION_LOOKAHEAD = 6         # Ø¨Ø±Ú¯Ø´Øª Ø±Ø§ Ø¯Ø± Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ú©Ù†Ø¯Ù„ Ø¨Ø¹Ø¯ Ø§Ø² Ù„Ù…Ø³ Ø¨Ø³Ù†Ø¬

        self.RECENT_REACTION_CANDLES = 30   # Ø¨Ø±Ú¯Ø´Øª Â«ØªØ§Ø²Ù‡Â» = Ø¯Ø± Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ú©Ù†Ø¯Ù„ Ø§Ø®ÛŒØ±

        self.TF_WEIGHT = {'4H': 1.0, '1D': 2.0, '1W': 3.0}

        self.NEAR_MAX_ATR = 4.0             # Ø³Ø·Ø­ Â«Ù†Ø²Ø¯ÛŒÚ©Â»: ØªØ§ Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± ATR(4H) Ø§Ø² Ù‚ÛŒÙ…Øª ...

        self.NEAR_MAX_PCT = 6.0             # ... Ùˆ Ø­Ø¯Ø§Ú©Ø«Ø± Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ø¯Ø±ØµØ¯ (Ú©Ù…ØªØ± Ø§Ø² Ø§ÛŒÙ† Ø¯Ùˆ)

        self.DIST_PENALTY_PER_ATR = 0.75    # Ù‡Ø± ATR ÙØ§ØµÙ„Ù‡ Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ø§Ø² Ø§Ù…ØªÛŒØ§Ø² Ø§Ù†ØªØ®Ø§Ø¨ Ú©Ù… Ù…ÛŒâ€ŒÚ©Ù†Ø¯



        # ================= Ø±ÙˆÛŒØ¯Ø§Ø¯ (Ù„Ù…Ø³/Ø´Ú©Ø³Øª) =================

        self.APPROACH_LOOKBACK_H1 = 8       # Ù‚ÛŒÙ…Øª Ø¨Ø§ÛŒØ¯ Ø¯Ø± Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ú©Ù†Ø¯Ù„ Û±Ø³Ø§Ø¹ØªÙ‡â€ŒÛŒ Ù‚Ø¨Ù„ ...

        self.APPROACH_CLEAR_H1 = 5          # ... Ùˆ Ø¯Ø± Ûµ Ú©Ù†Ø¯Ù„ Ø¢Ø®Ø±Ø´Ø§Ù† Ø§ØµÙ„Ø§Ù‹ Ø¨Ù‡ Ø³Ø·Ø­ Ù†Ø®ÙˆØ±Ø¯Ù‡ Ø¨Ø§Ø´Ø¯

        self.APPROACH_MIN_DIST_ATR1H = 1.0  # ... Ùˆ Ø­Ø¯Ø§Ù‚Ù„ Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± ATR(1H) Ø§Ø² Ø³Ø·Ø­ Ø¯ÙˆØ± Ø´Ø¯Ù‡ Ø¨Ø§Ø´Ø¯

        self.SKIP_BLOCK_H = 1               # Ù„Ù…Ø³ Ø¨ÛŒâ€ŒØ§Ø¹ØªØ¨Ø§Ø±: Ù‡Ù…Ø§Ù† Ø³Ø·Ø­ Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ø³Ø§Ø¹Øª Ø¯ÙˆØ¨Ø§Ø±Ù‡ Ø¨Ø±Ø±Ø³ÛŒ Ù†Ø´ÙˆØ¯

        self.LOG_ONLY_COOLDOWN_H = 2        # Ù„Ø§Ú¯ Ù„Ù…Ø³ Ø³Ø·ÙˆØ­ ØºÛŒØ±ÙØ¹Ø§Ù„: Ù‡Ø± Ø³Ø·Ø­ Ù‡Ø± X Ø³Ø§Ø¹Øª ÛŒÚ© Ø¨Ø§Ø±



        # ================= Ø³ØªØ§Ù¾ =================

        self.MAX_SETUPS = 2                 # Ù‡Ø± Ø·Ø±Ù (Ù…Ù‚Ø§ÙˆÙ…Øª/Ø­Ù…Ø§ÛŒØª) ÛŒÚ© Ø³ØªØ§Ù¾

        self.RETEST_EXPIRE_H = 12           # Ù…Ù‡Ù„Øª Ù¾ÙˆÙ„Ø¨Ú© Ø¨Ø¹Ø¯ Ø§Ø² Â«Ø´Ú©Ø³ØªÂ» Ø³Ø·Ø­ (Ø³Ø§Ø¹Øª)

        self.REJECT_EXPIRE_H = 8            # Ù…Ù‡Ù„Øª Ø³ÛŒÚ¯Ù†Ø§Ù„ Ø¨Ø¹Ø¯ Ø§Ø² Â«Ø±Ø¯ Ø´Ø¯Ù†Â» Ø§Ø² Ø³Ø·Ø­ (Ø³Ø§Ø¹Øª)

        self.ZONE_COOLDOWN_H = 3            # Ø¨Ø¹Ø¯ Ø§Ø² Ø³ÛŒÚ¯Ù†Ø§Ù„/Ø§Ù†Ù‚Ø¶Ø§ Ù‡Ù…Ø§Ù† Ø²ÙˆÙ† ØªØ§ Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± ÙØ¹Ø§Ù„ Ù†Ø´ÙˆØ¯

        self.SIGNAL_COOLDOWN_MIN = 30       # ÙØ§ØµÙ„Ù‡â€ŒÛŒ Ø­Ø¯Ø§Ù‚Ù„ Ø¨ÛŒÙ† Ø¯Ùˆ Ø³ÛŒÚ¯Ù†Ø§Ù„ ÛŒÚ© Ù†Ù…Ø§Ø¯

        self.MAX_DRIFT_ATR15 = 1.5          # Ø§Ú¯Ø± Ù‚ÛŒÙ…Øª Ø²Ù†Ø¯Ù‡ Ø¨ÛŒØ´ØªØ± Ø§Ø² Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± ATR(15m) Ø§Ø² ÙˆØ±ÙˆØ¯ Ø¯ÙˆØ± Ø´Ø¯ØŒ Ø±Ø¯ Ø´ÙˆØ¯



        # ================= Ø­Ø¯ Ø¶Ø±Ø± / Ù‡Ø¯Ù =================

        self.SL_ATR15_BUFFER = 0.5          # SL = Ø³Ø§ÛŒÙ‡â€ŒÛŒ Ú©Ù†Ø¯Ù„ ÛŒØ§ Ù„Ø¨Ù‡â€ŒÛŒ Ø²ÙˆÙ† (Ù‡Ø±Ú©Ø¯Ø§Ù… Ø¯ÙˆØ±ØªØ±) Â± Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± ATR(15m)

        self.MIN_RISK_ATR1H = 0.75          # Ø­Ø¯Ø§Ù‚Ù„ Ø±ÛŒØ³Ú© (Ø¨Ø±Ø­Ø³Ø¨ ATR Û±Ø³Ø§Ø¹ØªÙ‡)

        self.MAX_RISK_ATR1H = 2.0           # Ø¨ÛŒØ´ØªØ± Ø§Ø² Ø§ÛŒÙ† Ø±ÛŒØ³Ú© â†’ Ø³ÛŒÚ¯Ù†Ø§Ù„ Ù†Ù…ÛŒâ€ŒØ¯Ù‡Ø¯

        self.RR = 1.5                       # TP1 = Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ø¨Ø±Ø§Ø¨Ø± Ø±ÛŒØ³Ú©

        self.MIN_ROOM_R = 0                 # ÙÛŒÙ„ØªØ± ÙØ¶Ø§ (ÙØ¹Ù„Ø§Ù‹ Ø®Ø§Ù…ÙˆØ´): Ø§Ú¯Ø± Ø³Ø·Ø­ Ù…Ø®Ø§Ù„Ù Ù†Ø²Ø¯ÛŒÚ©â€ŒØªØ± Ø§Ø² Ø§ÛŒÙ†â€ŒÙ‚Ø¯Ø± Ø¨Ø±Ø§Ø¨Ø± Ø±ÛŒØ³Ú© Ø¨ÙˆØ¯ØŒ Ø³ÛŒÚ¯Ù†Ø§Ù„ Ù†Ø¯Ù‡ (Ù…Ø«Ù„Ø§Ù‹ 1.5)



        # ================= Ø±Ø¯ÛŒØ§Ø¨ÛŒ Ù†ØªÛŒØ¬Ù‡ =================

        self.TRACK_MAX_H = 48



        # ================= Ù„Ø§Ú¯ =================

        self.BOOK_LOG_MIN = 60

        self.BOOK_LOG_MAX_DIST_PCT = 15.0



        self.setups = {}                   # symbol -> Ù„ÛŒØ³Øª Ø³ØªØ§Ù¾â€ŒÙ‡Ø§

        self.active_setups = self.setups   # Ù†Ø§Ù… Ù‚Ø¯ÛŒÙ…ÛŒ (Ø³Ø§Ø²Ú¯Ø§Ø±ÛŒ)

        self.prev_scan = {}                # symbol -> {'levels','price','ms'}

        self.blocked = {}                  # symbol -> [(low, high, until_ms)]

        self.log_only_until = {}           # (symbol, round(low,2)) -> until_ms

        self.last_signal_ms = {}

        self.last_book_log = {}

        self.tracked = []                  # Ø³ÛŒÚ¯Ù†Ø§Ù„â€ŒÙ‡Ø§ÛŒ Ø¯Ø± Ø­Ø§Ù„ Ø±Ø¯ÛŒØ§Ø¨ÛŒ

        self.signal_seq = 0

        self.stats = {'n': 0, 'tp': 0, 'sl': 0, 'r': 0.0}



    def set_market_data_handler(self, handler):

        self.market_data = handler



    def _now_ms(self):

        return int(time.time() * 1000)



    async def start_monitoring(self):

        self.running = True

        _log(f"ðŸš€ {TAG} Started (Trend > Scored Levels > Touch/Break > 1H > 15M)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]



        while self.running:

            for symbol in symbols:

                try:

                    await self.process_symbol(symbol)

                except Exception as e:

                    _log(f"âŒ Error in {symbol}: {e}")

                await asyncio.sleep(15)

            await asyncio.sleep(180)



    async def stop(self):

        self.running = False

        _log("StrategyManager stopped")



    # ==========================================================

    # Ù…Ù†Ø·Ù‚ Ø³Ø·Ø­â€ŒÛŒØ§Ø¨ÛŒ (Ø¯Ø³Øªâ€ŒÙ†Ø®ÙˆØ±Ø¯Ù‡)

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



    # ---------- Ø­Ø§ÙØ¸Ù‡â€ŒÛŒ Ù‡Ù…Ù‡â€ŒÛŒ Ø³Ø·ÙˆØ­ ----------

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



    def _build_book(self, h4, h1d, h1w, price, trend, symbol=None):
        """Ø²ÙˆÙ†â€ŒÙ‡Ø§ÛŒ Ø¯Ø³ØªÛŒ ÙØ§ÛŒÙ„ levels.py Ù…Ù†Ø¨Ø¹ Ø§ØµÙ„ÛŒ Ù‡Ø³ØªÙ†Ø¯Ø› Ø¯Ø± Ù†Ø¨ÙˆØ¯ Ø¢Ù†ØŒ Ø³Ø·Ø­â€ŒÛŒØ§Ø¨ÛŒ Ù‚Ø¨Ù„ÛŒ Ø­ÙØ¸ Ù…ÛŒâ€ŒØ´ÙˆØ¯."""
        manual = MANUAL_LEVELS.get('LEVELS', MANUAL_LEVELS) if isinstance(MANUAL_LEVELS, dict) else {}
        book = []
        for level_symbol, zones in manual.items():
            if symbol is not None and level_symbol != symbol:
                continue
            if level_symbol not in ("BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"):
                continue
            # Ø§ÛŒÙ† ØªØ§Ø¨Ø¹ Ø¨Ø±Ø§ÛŒ Ù†Ù…Ø§Ø¯ Ø¬Ø§Ø±ÛŒ ÙØ±Ø§Ø®ÙˆØ§Ù†ÛŒ Ù…ÛŒâ€ŒØ´ÙˆØ¯Ø› ÙÛŒÙ„ØªØ± Ù†Ù…Ø§Ø¯ Ø¯Ø± process Ø§Ø¹Ù…Ø§Ù„ Ù…ÛŒâ€ŒØ´ÙˆØ¯.
            for pair in zones:
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    continue
                low, high = sorted((float(pair[0]), float(pair[1])))
                if low <= 0 or high <= 0:
                    continue
                book.append({'low': low, 'high': high, 'mid': (low + high) / 2,
                             'tf': 'MANUAL', 'manual': True})
        if book:
            return book

        # Ø§Ú¯Ø± ÙØ§ÛŒÙ„ levels.py Ø®Ø§Ù„ÛŒ/Ø¯Ø± Ø¯Ø³ØªØ±Ø³ Ù†Ø¨ÙˆØ¯ØŒ Ø±ÙˆØ´ Ø³Ø·Ø­â€ŒÛŒØ§Ø¨ÛŒ Ù‚Ø¨Ù„ÛŒ Ø§Ø¬Ø±Ø§ Ù…ÛŒâ€ŒØ´ÙˆØ¯.
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

    # ---------- Ø§Ù…ØªÛŒØ§Ø²Ø¯Ù‡ÛŒ ----------

    def _score_book(self, book, h4):

        """Ø§Ù…ØªÛŒØ§Ø² = ÙˆØ²Ù† ØªØ§ÛŒÙ…â€ŒÙØ±ÛŒÙ… + Ø¨Ø±Ú¯Ø´Øªâ€ŒÙ‡Ø§ÛŒ ÙˆØ§Ù‚Ø¹ÛŒ (Ø­Ø¯Ø§Ú©Ø«Ø± Ûµ) + ØªØ§Ø²Ú¯ÛŒ + Ù†Ù‚Ø´â€ŒØ¹ÙˆØ¶â€ŒÚ©Ø±Ø¯Ú¯ÛŒ."""

        H, L, C = h4['highs'], h4['lows'], h4['closes']

        n = len(C)

        start = max(1, n - self.SCORE_WINDOW)

        atr = h4['atr']

        for b in book:

            lo, hi = b['low'], b['high']



            # Ø¨Ø±Ú¯Ø´Øª ÙˆØ§Ù‚Ø¹ÛŒ: Ù‚ÛŒÙ…Øª Ø§Ø² ÛŒÚ© Ø³Ù…Øª Ø¢Ù…Ø¯Ù‡ØŒ Ø²ÙˆÙ† Ø±Ø§ Ù„Ù…Ø³ Ú©Ø±Ø¯Ù‡ Ùˆ Ø¯ÙˆØ¨Ø§Ø±Ù‡ Ø§Ø² Ù‡Ù…Ø§Ù† Ø³Ù…Øª â‰¥ Û± ATR Ø¯ÙˆØ± Ø´Ø¯Ù‡

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



            # Ù†Ù‚Ø´â€ŒØ¹ÙˆØ¶â€ŒÚ©Ø±Ø¯Ú¯ÛŒ: Ø²ÙˆÙ† Ù‚Ø¨Ù„Ø§Ù‹ Ù†Ù‚Ø´ Ù…Ø®Ø§Ù„Ù Ø¯Ø§Ø´ØªÙ‡ (Ø­Ù…Ø§ÛŒØªÛŒ Ú©Ù‡ Ù…Ù‚Ø§ÙˆÙ…Øª Ø´Ø¯Ù‡ ÛŒØ§ Ø¨Ø±Ø¹Ú©Ø³)

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

        """Ù‡Ø± Ø·Ø±Ù ÙÙ‚Ø· ÛŒÚ© Ø³Ø·Ø­ ÙØ¹Ø§Ù„: Ø¨Ù‡ØªØ±ÛŒÙ† (Ø§Ù…ØªÛŒØ§Ø² âˆ’ Ø¬Ø±ÛŒÙ…Ù‡â€ŒÛŒ ÙØ§ØµÙ„Ù‡) Ø¯Ø± Ù…Ø­Ø¯ÙˆØ¯Ù‡â€ŒÛŒ Ù†Ø²Ø¯ÛŒÚ©Ø› ÙˆÚ¯Ø±Ù†Ù‡ Ù†Ø²Ø¯ÛŒÚ©â€ŒØªØ±ÛŒÙ†."""

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

    # Ø±ÙˆÛŒØ¯Ø§Ø¯Ù‡Ø§: Ù„Ù…Ø³ / Ø´Ú©Ø³Øª

    # ==========================================================

    def _is_blocked(self, symbol, z, now_ms):

        lst = [b for b in self.blocked.get(symbol, []) if b[2] > now_ms]

        self.blocked[symbol] = lst

        tol = z['high'] * 0.001

        return any(not (z['high'] < b[0] - tol or z['low'] > b[1] + tol) for b in lst)



    def _block(self, symbol, z, now_ms, hours=None):

        h = self.ZONE_COOLDOWN_H if hours is None else hours

        self.blocked.setdefault(symbol, []).append((z['low'], z['high'], now_ms + h * 3_600_000))



    def _detect_events(self, symbol, prev, levels, m15c, h1c, price, now_ms):
        """Ø«Ø¨Øª Ø±ÙˆÛŒØ¯Ø§Ø¯ Ø­ØªÛŒ Ø¯Ø± Ø´Ø±ÙˆØ¹ Ø¯Ø§Ø®Ù„ Ø²ÙˆÙ† ÛŒØ§ Ø¹Ø¨ÙˆØ± Ú©Ø§Ù…Ù„ Ø¨ÛŒÙ† Ø¯Ùˆ Ø§Ø³Ú©Ù†."""
        prev_price = prev.get('price') if prev else None
        scan_ms = prev.get('ms', 0) if prev else 0
        candles = [c for c in m15c if c[1] > scan_ms]
        active = self.setups.get(symbol, [])
        found = []
        for lv in levels:
            if self._is_blocked(symbol, lv, now_ms):
                continue
            tol = lv['high'] * 0.001
            if any(not (lv['high'] < st['zone']['low'] - tol or lv['low'] > st['zone']['high'] + tol)
                   for st in active if st['state'] != 'DEAD'):
                continue
            role = None
            ev = None
            # Ø¬Ù‡Øª Ù†Ø²Ø¯ÛŒÚ©â€ŒØ´Ø¯Ù† Ø§Ø² Ù‚ÛŒÙ…Øª Ø§Ø³Ú©Ù† Ù‚Ø¨Ù„ÛŒ ØªØ¹ÛŒÛŒÙ† Ù…ÛŒâ€ŒØ´ÙˆØ¯.
            if prev_price is not None and prev_price < lv['low']:
                role = 'RES'
            elif prev_price is not None and prev_price > lv['high']:
                role = 'SUP'
            # Ø¯Ø± Ø´Ø±ÙˆØ¹ Ø¨Ø±Ù†Ø§Ù…Ù‡ ÛŒØ§ ÙˆÙ‚ØªÛŒ Ø§Ø³Ú©Ù† Ù‚Ø¨Ù„ÛŒ Ø¯Ø§Ø®Ù„ Ø²ÙˆÙ† Ø¨ÙˆØ¯Ù‡ØŒ Ø±ÙˆÛŒØ¯Ø§Ø¯ Ø±Ø§ Ù‡Ù…Ø§Ù† Ù„Ø­Ø¸Ù‡ Ø«Ø¨Øª Ú©Ù†.
            elif lv['low'] <= price <= lv['high'] or (prev_price is not None and lv['low'] <= prev_price <= lv['high']):
                ref = h1c[-1][5] if h1c else price
                role = 'RES' if ref <= lv['mid'] else 'SUP'
                ev = now_ms
            # Ú©Ù†Ø¯Ù„â€ŒÙ‡Ø§ÛŒ Ø¨ÛŒÙ† Ø¯Ùˆ Ø§Ø³Ú©Ù†Ø› high/low Ú©Ù„ Ù…Ø­Ø¯ÙˆØ¯Ù‡ Ø±Ø§ Ø¨Ø±Ø±Ø³ÛŒ Ù…ÛŒâ€ŒÚ©Ù†Ø¯ØŒ Ù†Ù‡ ÙÙ‚Ø· Ù‚ÛŒÙ…Øª Ø¢Ø®Ø±.
            if role is None and prev is not None:
                for c in candles:
                    if c[3] >= lv['low'] and c[4] <= lv['high']:
                        before = prev_price
                        role = 'RES' if before is not None and before < lv['low'] else 'SUP'
                        ev = c[0]
                        break
            if role is not None and ev is None:
                for c in candles:
                    if c[3] >= lv['low'] and c[4] <= lv['high']:
                        ev = c[0]
                        break
                if ev is None and ((role == 'RES' and price >= lv['low']) or (role == 'SUP' and price <= lv['high'])):
                    ev = now_ms
            if role is not None and ev is not None:
                found.append((lv, role, ev))
        found.sort(key=lambda x: (x[2], -x[0].get('score', 0)))
        return found

    def _fresh_approach(self, lv, role, h1c, atr1h, ev_ms, price):

        """Ù„Ù…Ø³ Ù…Ø¹ØªØ¨Ø± = Ù‚ÛŒÙ…Øª Ø§Ø² Ø¯ÙˆØ± Ø¢Ù…Ø¯Ù‡Ø› Ù†Ù‡ Ø§ÛŒÙ†Ú©Ù‡ Ø³Ø§Ø¹Øªâ€ŒÙ‡Ø§ Ú©Ù†Ø§Ø± Ø³Ø·Ø­ Ú†Ø±Ø®ÛŒØ¯Ù‡ Ø¨Ø§Ø´Ø¯."""

        prior = [c for c in h1c if c[1] <= ev_ms][-self.APPROACH_LOOKBACK_H1:]

        if len(prior) < self.APPROACH_CLEAR_H1:

            return True, "Ø¯ÛŒØªØ§ÛŒ Û±Ø³Ø§Ø¹ØªÙ‡ Ú©Ø§ÙÛŒ Ù†ÛŒØ³ØªØ› Ø¨Ø¯ÙˆÙ† ÙÛŒÙ„ØªØ±"

        tol = price * 0.0005

        clear = prior[-self.APPROACH_CLEAR_H1:]

        if role == 'RES':

            touched = any(c[3] >= lv['low'] - tol for c in clear)

            far = lv['low'] - min(c[4] for c in prior)

        else:

            touched = any(c[4] <= lv['high'] + tol for c in clear)

            far = max(c[3] for c in prior) - lv['high']

        if touched:

            return False, f"Ù‚ÛŒÙ…Øª Ø¯Ø± {self.APPROACH_CLEAR_H1} Ø³Ø§Ø¹Øª Ø§Ø®ÛŒØ± Ú©Ù†Ø§Ø± Ø³Ø·Ø­ Ø¨ÙˆØ¯Ù‡"

        if atr1h and far < self.APPROACH_MIN_DIST_ATR1H * atr1h:

            return False, f"Ø¯ÙˆØ±ØªØ±ÛŒÙ† ÙØ§ØµÙ„Ù‡â€ŒÛŒ Ù‚ÛŒÙ…Øª Ø§Ø² Ø³Ø·Ø­ ÙÙ‚Ø· {far / atr1h:.2f} ATR(1H) Ø¨ÙˆØ¯Ù‡"

        return True, "ok"



    @staticmethod

    def _beyond(role, z, close):

        """Ú©Ù„ÙˆØ² Û±Ø³Ø§Ø¹ØªÙ‡ Ø¨ÛŒØ±ÙˆÙ† Ø²ÙˆÙ† (Ø¨Ø¯Ù†Ù‡ Ø±Ø¯ Ø´Ø¯Ù‡)ØŸ"""

        return (role == 'RES' and close > z['high']) or (role == 'SUP' and close < z['low'])



    @staticmethod

    def _zs(z):

        return f"{_fmt(z['low'])}â€“{_fmt(z['high'])}"



    # ==========================================================

    # Ù…Ø§Ø´ÛŒÙ† Ø­Ø§Ù„Øª: WAIT_1H  ->  ARMED  ->  Ø³ÛŒÚ¯Ù†Ø§Ù„

    # ==========================================================

    def _advance(self, symbol, st, trend, price, h1c, m15c, atr15, atr1h, atr4h, book, now_ms):
        z = st['zone']

        if st['state'] == 'WAIT_1H':
            after = [c for c in h1c if c[1] > st['event_ms']]
            if not after:
                return None
            decision = None
            for c in after:
                close = c[5]
                if st['role'] == 'RES':
                    if close > z['high']:
                        decision = ('BREAKOUT', 'LONG', c)
                        break
                    if close < z['low']:
                        decision = ('REJECT', 'SHORT', c)
                        break
                else:  # Ø­Ù…Ø§ÛŒØª: Ù…Ù†Ø·Ù‚ Ù…Ø¹Ú©ÙˆØ³
                    if close < z['low']:
                        decision = ('BREAKDOWN', 'SHORT', c)
                        break
                    if close > z['high']:
                        decision = ('REJECT', 'LONG', c)
                        break
                # Ú©Ù„ÙˆØ² Ø¯Ø§Ø®Ù„ Ø²ÙˆÙ† ØªØ¹ÛŒÛŒÙ† ØªÚ©Ù„ÛŒÙ Ù†ÛŒØ³ØªØ› Ù…Ù†ØªØ¸Ø± Ú©Ù†Ø¯Ù„ Ø¨Ø³ØªÙ‡â€ŒØ´Ø¯Ù‡ Ø¨Ø¹Ø¯ÛŒ Ø¨Ù…Ø§Ù†.
            if decision is None:
                return None
            kind, direction, candle = decision
            st.update(state='ARMED', kind=kind, direction=direction,
                      since_ms=candle[1], checked_ms=candle[1])
            _log(f"{'ðŸ’¥' if kind != 'REJECT' else 'â†©ï¸'} [{symbol}] ØªØ§ÛŒÛŒØ¯ 1H: {kind} | Ø²ÙˆÙ† {self._zs(z)} | Ú©Ù„ÙˆØ² {_fmt(candle[5])} | Ù…Ù†ØªØ¸Ø± ØªØ±ÛŒÚ¯Ø± Û±ÛµØ¯Ù‚ÛŒÙ‚Ù‡")

        if st['state'] != 'ARMED':
            return None

        hours = self.RETEST_EXPIRE_H if st['kind'] in ('BREAKOUT', 'BREAKDOWN') else self.REJECT_EXPIRE_H
        if now_ms - st['since_ms'] > hours * 3_600_000:
            _log(f"âŒ› [{symbol}] Ø³ØªØ§Ù¾ {st['kind']} Ø±ÙˆÛŒ Ø²ÙˆÙ† {self._zs(z)} Ø¨Ø¯ÙˆÙ† Ø³ÛŒÚ¯Ù†Ø§Ù„ Ù…Ù†Ù‚Ø¶ÛŒ Ø´Ø¯")
            self._block(symbol, z, now_ms)
            st['state'] = 'DEAD'
            return None

        direction = st['direction']
        trig = None
        last_cm = st['checked_ms']
        for c in m15c:
            if c[1] <= max(st['since_ms'], st['checked_ms']):
                continue
            last_cm = max(last_cm, c[1])
            _, _, o, h, l, cl = c
            touched = h >= z['low'] and l <= z['high']
            if not touched:
                continue
            if direction == 'LONG' and cl > z['high']:
                # Ø´Ú©Ø³Øª Ø±Ùˆ Ø¨Ù‡ Ø¨Ø§Ù„Ø§: Ù¾ÙˆÙ„Ø¨Ú© Ø¨Ù‡ Ø²ÙˆÙ† Ùˆ Ø¨Ø³ØªÙ‡â€ŒØ´Ø¯Ù† Ø¯ÙˆØ¨Ø§Ø±Ù‡ Ø¨Ø§Ù„Ø§ÛŒ Ø¢Ù†
                if st['kind'] == 'REJECT' and cl > o:
                    trig = c
                elif st['kind'] != 'REJECT':
                    trig = c
            elif direction == 'SHORT' and cl < z['low']:
                # Ø´Ú©Ø³Øª Ø±Ùˆ Ø¨Ù‡ Ù¾Ø§ÛŒÛŒÙ† ÛŒØ§ Ø±Ø¯ Ù…Ù‚Ø§ÙˆÙ…Øª: Ø¨Ø³ØªÙ‡â€ŒØ´Ø¯Ù† Ø²ÛŒØ± Ø²ÙˆÙ†
                if st['kind'] == 'REJECT' and cl < o:
                    trig = c
                elif st['kind'] != 'REJECT':
                    trig = c
        st['checked_ms'] = last_cm
        if not trig:
            return None

        # EMA200 Ú†Ù‡Ø§Ø±Ø³Ø§Ø¹ØªÙ‡ ÙÙ‚Ø· Ø¨Ø±Ú†Ø³Ø¨ Ù‡Ù…â€ŒØ¬Ù‡ØªÛŒ Ø§Ø³Øª Ùˆ Ù‡Ø±Ú¯Ø² Ø³ÛŒÚ¯Ù†Ø§Ù„ Ø±Ø§ Ù…Ø³Ø¯ÙˆØ¯ Ù†Ù…ÛŒâ€ŒÚ©Ù†Ø¯.
        aligned = (direction == 'LONG' and trend == 'UPTREND') or (direction == 'SHORT' and trend == 'DOWNTREND')
        trend_note = 'Ù‡Ù…â€ŒØ¬Ù‡Øª Ø¨Ø§ EMA200' if aligned else 'âš ï¸ Ø®Ù„Ø§Ù Ø¬Ù‡Øª EMA200'
        entry = trig[5]
        if atr15 and abs(price - entry) > self.MAX_DRIFT_ATR15 * atr15:
            _log(f"â­ï¸ [{symbol}] ØªØ±ÛŒÚ¯Ø± {direction} Ø¯ÛŒØ¯Ù‡ Ø´Ø¯ ÙˆÙ„ÛŒ Ù‚ÛŒÙ…Øª Ø§Ø² ÙˆØ±ÙˆØ¯ ({_fmt(entry)}) Ø¯ÙˆØ± Ø´Ø¯Ù‡ ({_fmt(price)})Ø› ÙØ±ØµØª Ø§Ø² Ø¯Ø³Øª Ø±ÙØª")
            st['state'] = 'DEAD'
            return None
        if now_ms - self.last_signal_ms.get(symbol, 0) < self.SIGNAL_COOLDOWN_MIN * 60_000:
            _log(f"â¸ï¸ [{symbol}] ØªØ±ÛŒÚ¯Ø± {direction} Ø±ÙˆÛŒ {self._zs(z)} Ø¯ÛŒØ¯Ù‡ Ø´Ø¯ ÙˆÙ„ÛŒ Ú©Ù…ØªØ± Ø§Ø² {self.SIGNAL_COOLDOWN_MIN} Ø¯Ù‚ÛŒÙ‚Ù‡ Ø§Ø² Ø³ÛŒÚ¯Ù†Ø§Ù„ Ù‚Ø¨Ù„ÛŒ Ú¯Ø°Ø´ØªÙ‡")
            return None
        trade = self._build_trade(symbol, direction, entry, z, trig, atr15, atr1h, atr4h, book)
        if trade is None:
            return None

        self.last_signal_ms[symbol] = now_ms
        self.signal_seq += 1
        sid = self.signal_seq
        kind_t = 'Ù¾ÙˆÙ„Ø¨Ú© Ø¨Ø¹Ø¯ Ø§Ø² Ø´Ú©Ø³Øª' if st['kind'] in ('BREAKOUT', 'BREAKDOWN') else 'Ø±Ø¯ Ø´Ø¯Ù† Ø§Ø² Ø³Ø·Ø­'
        lv = st.get('level') or {}
        _log(f"ðŸ† [{symbol}] {direction} Entry Triggered! #{sid} ({kind_t}) | Ú©Ù†Ø¯Ù„ Û±ÛµØ¯Ù‚ÛŒÙ‚Ù‡ {_ts(trig[0])} | Ø²ÙˆÙ† {self._zs(z)}")
        _log(f"ðŸ“’ JOURNAL | #{sid} | {symbol} | {direction} | {st['kind']} | Ø³Ø·Ø­ {lv.get('tf', '?')} Ø§Ù…ØªÛŒØ§Ø²={lv.get('score', 0):.1f} "
             f"| Ø²ÙˆÙ† {self._zs(z)} | ÙˆØ±ÙˆØ¯ {_fmt(entry)} SL {_fmt(trade['sl'])} TP1 {_fmt(trade['tp1'])} "
             f"TP2 {_fmt(trade['tp2']) if trade['tp2'] else 'â€”'} | Ø±ÙˆÙ†Ø¯ {trend} | {trend_note} | {_ts(now_ms)} UTC")
        self.send_signal(symbol, direction, entry, trade, trend_note=trend_note)
        self._track_add(sid, symbol, direction, entry, trade, trig[1], now_ms)
        self._block(symbol, z, now_ms)
        st['state'] = 'DEAD'
        return True

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

                _log(f"ðŸš« [{symbol}] {direction} Ø±ÙˆÛŒ {self._zs(z)} Ø±Ø¯ Ø´Ø¯: Ø±ÛŒØ³Ú© {risk / atr1h:.2f} ATR(1H) Ø§Ø² Ø³Ù‚Ù {self.MAX_RISK_ATR1H} Ø¨ÛŒØ´ØªØ± Ø§Ø³Øª "

                     f"(ÙˆØ±ÙˆØ¯ {_fmt(entry)} SL {_fmt(sl)})")

                return None

            if risk < min_risk:

                risk = min_risk

                sl = entry + risk if direction == 'SHORT' else entry - risk

        if risk <= 0:

            return None



        # Ø³Ø·Ø­ Ù…Ø®Ø§Ù„Ù Ø¨Ø¹Ø¯ÛŒ (Ø¨Ø±Ø§ÛŒ ÙØ¶Ø§ÛŒ Ø­Ø±Ú©Øª Ùˆ TP2)

        if direction == 'SHORT':

            opp = [b for b in book if b['high'] < entry - 1e-9 and b['mid'] < z['low']]

            nxt = max(opp, key=lambda b: b['high'])['high'] if opp else None

            room = (entry - nxt) if nxt is not None else None

        else:

            opp = [b for b in book if b['low'] > entry + 1e-9 and b['mid'] > z['high']]

            nxt = min(opp, key=lambda b: b['low'])['low'] if opp else None

            room = (nxt - entry) if nxt is not None else None



        if self.MIN_ROOM_R and room is not None and room < self.MIN_ROOM_R * risk:

            _log(f"ðŸš« [{symbol}] {direction} Ø±ÙˆÛŒ {self._zs(z)} Ø±Ø¯ Ø´Ø¯: Ø³Ø·Ø­ Ù…Ø®Ø§Ù„Ù ÙÙ‚Ø· {room / risk:.2f}R Ø¢Ù†â€ŒØ·Ø±Ùâ€ŒØªØ± Ø§Ø³Øª "

                 f"({_fmt(nxt)}Ø› Ø­Ø¯Ø§Ù‚Ù„ {self.MIN_ROOM_R}R Ù„Ø§Ø²Ù… Ø§Ø³Øª)")

            return None



        tp1 = entry - risk * self.RR if direction == 'SHORT' else entry + risk * self.RR

        tp2 = nxt if nxt is not None else 0

        return {'sl': sl, 'tp1': tp1, 'tp2': tp2, 'risk': risk,

                'room_txt': (f"{room / risk:.2f}R ({_fmt(nxt)})" if room is not None else "Ø³Ø·Ø­ÛŒ Ù†ÛŒØ³Øª")}



    def send_signal(self, symbol, direction, entry, trade, trend_note=""):

        signal = {

            'symbol': symbol,

            'type': 'SELL' if direction == 'SHORT' else 'BUY',

            'direction': direction,

            'entry_price': entry,

            'stop_loss': round(trade['sl'], 4),

            'take_profit_1': round(trade['tp1'], 4),

            'take_profit_2': round(trade['tp2'], 4) if trade['tp2'] else 0,

            'take_profit_3': 0,

            'strategy': f'{TAG}_4H_Levels_1H_15M | {trend_note}',
            'trend_note': trend_note,

        }

        _log(f"ðŸ“¤ [{symbol}] SIGNAL {signal['type']} | Entry {_fmt(entry)} | SL {_fmt(trade['sl'])} | "

             f"TP1 {_fmt(trade['tp1'])} | TP2 {_fmt(trade['tp2']) if trade['tp2'] else 'â€”'}")



        if not self.telegram_bot:

            _log(f"âš ï¸ [{symbol}] telegram_bot ØªÙ†Ø¸ÛŒÙ… Ù†Ø´Ø¯Ù‡Ø› Ø³ÛŒÚ¯Ù†Ø§Ù„ Ø§Ø±Ø³Ø§Ù„ Ù†Ø´Ø¯")

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

            _log(f"âŒ Ø§Ø±Ø³Ø§Ù„ Ø³ÛŒÚ¯Ù†Ø§Ù„ Ø¨Ù‡ ØªÙ„Ú¯Ø±Ø§Ù… Ø´Ú©Ø³Øª Ø®ÙˆØ±Ø¯: {exc!r}")

        else:

            _log("âœ… Ø³ÛŒÚ¯Ù†Ø§Ù„ Ø¨Ù‡ ØªÙ„Ú¯Ø±Ø§Ù… ÙØ±Ø³ØªØ§Ø¯Ù‡ Ø´Ø¯")



    # ==========================================================

    # Ø±Ø¯ÛŒØ§Ø¨ÛŒ Ù†ØªÛŒØ¬Ù‡â€ŒÛŒ Ø³ÛŒÚ¯Ù†Ø§Ù„â€ŒÙ‡Ø§ (Ø¨Ø±Ø§ÛŒ Ø¨Ú©â€ŒØªØ³Øª Ø²Ù†Ø¯Ù‡)

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

                    amb = hit_sl and hit_tp   # Ù‡Ø± Ø¯Ùˆ Ø¯Ø± ÛŒÚ© Ú©Ù†Ø¯Ù„: Ù…Ø­ØªØ§Ø·Ø§Ù†Ù‡ SL Ø­Ø³Ø§Ø¨ Ù…ÛŒâ€ŒØ´ÙˆØ¯

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

        _log(f"ðŸ“’ RESULT | #{t['id']} | {t['symbol']} | {t['direction']} | {result}{' (Ù‡Ø± Ø¯Ùˆ Ø¯Ø± ÛŒÚ© Ú©Ù†Ø¯Ù„Ø› SL ÙØ±Ø¶ Ø´Ø¯)' if ambiguous else ''} | "

             f"R={r:+.2f} | Ø¨ÛŒØ´ØªØ±ÛŒÙ† Ù¾ÛŒØ´Ø±ÙˆÛŒ {t['mfe']:.2f}R / Ø¨ÛŒØ´ØªØ±ÛŒÙ† Ø¨Ø±Ú¯Ø´Øª {t['mae']:.2f}R | Ù…Ø¯Øª {hours:.1f}h | "

             f"ÙˆØ±ÙˆØ¯ {_fmt(t['entry'])} SL {_fmt(t['sl'])} TP {_fmt(t['tp'])}")

        s = self.stats

        if s['n']:

            _log(f"ðŸ“ˆ Ø¢Ù…Ø§Ø± Ú©Ù„: {s['n']} Ù…Ø¹Ø§Ù…Ù„Ù‡ | TP {s['tp']} | SL {s['sl']} | ÙˆÛŒÙ†â€ŒØ±ÛŒØª {s['tp'] / s['n'] * 100:.0f}% | Ø¬Ù…Ø¹ R = {s['r']:+.2f}")

        self.tracked = [x for x in self.tracked if x['status'] == 'OPEN']



    # ==========================================================

    # Ù„Ø§Ú¯

    # ==========================================================

    def _log_book(self, symbol, book, price):

        near = [b for b in book if abs(b['mid'] - price) / price * 100 <= self.BOOK_LOG_MAX_DIST_PCT]

        res = sorted([b for b in near if b['role'] == 'RES'], key=lambda b: b['low'])

        sup = sorted([b for b in near if b['role'] == 'SUP'], key=lambda b: -b['high'])

        _log(f"ðŸ—‚ï¸ [{symbol}] Ø­Ø§ÙØ¸Ù‡â€ŒÛŒ Ø³Ø·ÙˆØ­ (ØªØ§ {self.BOOK_LOG_MAX_DIST_PCT:.0f}%): Ù…Ù‚Ø§ÙˆÙ…Øª {len(res)} | Ø­Ù…Ø§ÛŒØª {len(sup)} | Ù‚ÛŒÙ…Øª {_fmt(price)}")

        for tag, lst in (("RES", res), ("SUP", sup)):

            for b in lst:

                mark = "â­ÙØ¹Ø§Ù„" if b['active'] else "     "

                _log(f"   {mark} {tag} {_fmt(b['low'])}â€“{_fmt(b['high'])} [{b['tf']}] Ø§Ù…ØªÛŒØ§Ø²={b['score']:.1f} "

                     f"Ø¨Ø±Ú¯Ø´Øªâ€ŒÙ‡Ø§={b['reactions']} {'Ù†Ù‚Ø´â€ŒØ¹ÙˆØ¶ ' if b['flipped'] else ''}{'ØªØ§Ø²Ù‡ ' if b['recent'] else ''}"

                     f"ÙØ§ØµÙ„Ù‡={b['dist'] / price * 100:.2f}% ({b['dist_atr']:.1f} ATR)")



    async def process_symbol(self, symbol):

        h4 = await self.market_data.get_tf_data(symbol, "240")

        h1 = await self.market_data.get_tf_data(symbol, "60")

        m15 = await self.market_data.get_tf_data(symbol, "15")

        h1d = await self.market_data.get_tf_data(symbol, "D")

        h1w = await self.market_data.get_tf_data(symbol, "W")



        if not all([h4, h1, m15]):

            _log(f"âš ï¸ {symbol}: data missing (4H={bool(h4)}, 1H={bool(h1)}, 15m={bool(m15)})")

            return



        now_ms = self._now_ms()

        price = h4['price']

        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        atr4h, atr1h, atr15 = h4['atr'], h1['atr'], m15['atr']



        book = self._build_book(h4, h1d, h1w, price, trend, symbol)

        self._score_book(book, h4)

        self._pick_active(book, price, atr4h)



        h1c = _tail_candles(h1, now_ms, 60)

        m15c = _tail_candles(m15, now_ms, 60)

        lst = self.setups.setdefault(symbol, [])



        # --- Ø±ÙˆÛŒØ¯Ø§Ø¯Ù‡Ø§ÛŒ Ø¬Ø¯ÛŒØ¯: Ø§Ø² Ø§Ø³Ú©Ù† Ø§ÙˆÙ„ Ù‡Ù… ÙØ¹Ø§Ù„Ø› Ø²ÙˆÙ†â€ŒÙ‡Ø§ÛŒ Ø¯Ø³ØªÛŒ Ø§ÙˆÙ„ÙˆÛŒØª Ø¯Ø§Ø±Ù†Ø¯ ---
        prev = self.prev_scan.get(symbol)
        for lv, role, ev_ms in self._detect_events(symbol, prev, book, m15c, h1c, price, now_ms):
            if not lv.get('active'):
                key = (symbol, round(lv['low'], 2))
                if self.log_only_until.get(key, 0) <= now_ms:
                    self.log_only_until[key] = now_ms + self.LOG_ONLY_COOLDOWN_H * 3_600_000
                    _log(f"ðŸ“ [{symbol}] Ø³Ø·Ø­ ØºÛŒØ±ÙØ¹Ø§Ù„ {self._zs(lv)} Ù„Ù…Ø³ Ø´Ø¯Ø› ÙÙ‚Ø· Ø«Ø¨Øª Ø´Ø¯")
                continue
            if len([x for x in lst if x['state'] != 'DEAD']) >= self.MAX_SETUPS:
                continue
            st = {'state': 'WAIT_1H', 'role': role,
                  'zone': {'low': lv['low'], 'high': lv['high']},
                  'level': dict(lv), 'event_ms': ev_ms, 'kind': None,
                  'since_ms': ev_ms, 'checked_ms': ev_ms}
            lst.append(st)
            _log(f"ðŸŽ¯ [{symbol}] Ø±ÙˆÛŒØ¯Ø§Ø¯ {'Ù…Ù‚Ø§ÙˆÙ…Øª' if role == 'RES' else 'Ø­Ù…Ø§ÛŒØª'} {self._zs(lv)} Ø«Ø¨Øª Ø´Ø¯ | Ù‚ÛŒÙ…Øª {_fmt(price)} | Ù…Ù†ØªØ¸Ø± Ú©Ù„ÙˆØ² ØªØ¹ÛŒÛŒÙ†â€ŒÚ©Ù†Ù†Ø¯Ù‡ Û±Ø³Ø§Ø¹ØªÙ‡")

        # --- Ù¾ÛŒØ´Ø¨Ø±Ø¯ Ø³ØªØ§Ù¾â€ŒÙ‡Ø§ ---

        for st in lst:

            if st['state'] != 'DEAD':

                self._advance(symbol, st, trend, price, h1c, m15c, atr15, atr1h, atr4h, book, now_ms)

        self.setups[symbol] = lst = [x for x in lst if x['state'] != 'DEAD']



        # --- Ù†ØªÛŒØ¬Ù‡â€ŒÛŒ Ø³ÛŒÚ¯Ù†Ø§Ù„â€ŒÙ‡Ø§ÛŒ Ù‚Ø¨Ù„ÛŒ ---

        self._track_update(symbol, m15c, now_ms)



        # --- Ø°Ø®ÛŒØ±Ù‡ Ø¨Ø±Ø§ÛŒ Ø§Ø³Ú©Ù† Ø¨Ø¹Ø¯ ---

        self.prev_scan[symbol] = {'levels': book, 'price': price, 'ms': now_ms}



        # --- Ù„Ø§Ú¯ ---

        if now_ms - self.last_book_log.get(symbol, 0) >= self.BOOK_LOG_MIN * 60_000:

            self._log_book(symbol, book, price)

            self.last_book_log[symbol] = now_ms



        act = {b['role']: b for b in book if b['active']}

        def _a(role):

            b = act.get(role)

            return f"{self._zs(b)} [{b['tf']} s={b['score']:.1f}{' âš Ø¯ÙˆØ±' if b['why'] == 'fallback' else ''}]" if b else "â€”"

        tail15 = _tail_candles(m15, now_ms, 1)

        last15 = tail15[-1] if tail15 else None

        parts = []

        for st in lst:

            z = st['zone']

            p = f"{st['state']}/{st['kind'] or 'EVENT'} {st['role']} {self._zs(z)}"

            if st['state'] == 'ARMED' and last15:
                if st.get('direction') == 'LONG':
                    color_note = ' Ú©Ù†Ø¯Ù„ Ø³Ø¨Ø²' if st['kind'] == 'REJECT' else ''
                    p += f" (Ù†ÛŒØ§Ø²: wick Ø¯Ø§Ø®Ù„ Ø²ÙˆÙ† Ùˆ close>{_fmt(z['high'])}{color_note} | Ø¢Ø®Ø±ÛŒÙ† Û±ÛµÙ… L{_fmt(last15[4])} C{_fmt(last15[5])})"
                else:
                    color_note = ' Ú©Ù†Ø¯Ù„ Ù‚Ø±Ù…Ø²' if st['kind'] == 'REJECT' else ''
                    p += f" (Ù†ÛŒØ§Ø²: wick Ø¯Ø§Ø®Ù„ Ø²ÙˆÙ† Ùˆ close<{_fmt(z['low'])}{color_note} | Ø¢Ø®Ø±ÛŒÙ† Û±ÛµÙ… H{_fmt(last15[3])} C{_fmt(last15[5])})"
            parts.append(p)

        _log(f"ðŸ“Š {symbol} â€” {trend} | Ù‚ÛŒÙ…Øª {_fmt(price)} | Ø³Ø·ÙˆØ­ Ø¯Ø± Ø­Ø§ÙØ¸Ù‡ {len(book)} | ÙØ¹Ø§Ù„: Ù…Ù‚Ø§ÙˆÙ…Øª {_a('RES')} / Ø­Ù…Ø§ÛŒØª {_a('SUP')} | "

             f"Ø³ØªØ§Ù¾â€ŒÙ‡Ø§: " + (" || ".join(parts) if parts else "â€”"))
