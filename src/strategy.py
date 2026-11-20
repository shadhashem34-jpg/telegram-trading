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

        # تنظیمات ثابت استراتژی
        self.LOOKBACK_CANDLES = 12
        self.MAX_LOOKBACK = 30
        self.ZONE_WIDTH_PCT = 0.0025
        self.DROP_FORMING_CANDLE = 'auto'

        self.last_signal_h1 = {}
        self.last_signal_15m = {}

        # 🧠 حافظه سطوح ثبت‌شده (برای ۱۲ ساعت)
        self.recorded_levels = {}

        # نگهداری زون‌های چرخه قبلی برای تشخیص شکست
        self.last_zones = {}

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V71 Started (4H Priority + 1D Backup)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            await asyncio.sleep(180)

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

    def _levels_from_candles(self, data, price, trend, drop_forming):
        o, h, l, c = data['opens'], data['highs'], data['lows'], data['closes']

        if drop_forming == 'auto':
            if abs(c[-1] - price) <= price * 0.0002:
                o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]

        elif drop_forming:
            o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]

        res_zone, sup_zone = None, None
        res_c, sup_c = [], []

        for lookback in (self.LOOKBACK_CANDLES, self.MAX_LOOKBACK):
            r, s = self._collect_candidates(o, h, l, c, price, lookback, trend)

            if res_zone is None and r:
                res_c = r
                body = max(x[0] for x in r)
                wick = max(x[1] for x in r if x[0] == body)

                top = min(max(wick, body), body * (1 + self.ZONE_WIDTH_PCT))
                res_zone = {
                    'low': body,
                    'high': top,
                    'type': 'res'
                }

            if sup_zone is None and s:
                sup_c = s
                body = max(x[0] for x in s)
                wick = min(x[1] for x in s if x[0] == body)

                bottom = max(min(wick, body), body * (1 - self.ZONE_WIDTH_PCT))
                sup_zone = {
                    'low': bottom,
                    'high': body,
                    'type': 'sup'
                }

            if res_zone and sup_zone:
                break

        return res_zone, sup_zone, res_c, sup_c

    def _find_levels(self, h4, h1d, price, trend):
        # ۱. اول ۴ ساعته
        res_zone, sup_zone, res_c, sup_c = self._levels_from_candles(
            h4, price, trend, self.DROP_FORMING_CANDLE
        )

        source_res = "4H" if res_zone else None
        source_sup = "4H" if sup_zone else None

        # ۲. اگر ۴ ساعته کور بود، سراغ روزانه می‌ریم
        if h1d is not None and (res_zone is None or sup_zone is None):
            r1d, s1d, r1dc, s1dc = self._levels_from_candles(
                h1d, price, trend, drop_forming=False
            )

            if res_zone is None and r1d:
                res_zone, res_c = r1d, r1dc
                source_res = "1D"

            if sup_zone is None and s1d:
                sup_zone, sup_c = s1d, s1dc
                source_sup = "1D"

        return res_zone, sup_zone, res_c, sup_c, source_res, source_sup

    # ==========================================================
    # ثبت شکست با زون‌های قبلی + مدیریت زمان ۱۲ ساعت
    # ==========================================================
    def _register_and_manage(self, symbol, res_zone, sup_zone, price):
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)

        # زون‌های چرخه قبلی
        previous = self.last_zones.get(symbol, {})
        prev_res = previous.get('res')
        prev_sup = previous.get('sup')

        # ثبت شکست مقاومت (LONG)
        if prev_res and price > prev_res['high']:
            if symbol not in self.recorded_levels:
                self.recorded_levels[symbol] = {
                    'zone': prev_res.copy(),
                    'type': 'LONG',
                    'record_time': now,
                    'break_time_ms': now_ms
                }

                logger.log_system_event(
                    f"🧠 [{symbol}] Breakout recorded above "
                    f"{prev_res['high']:.2f} | Price={price:.2f}"
                )

        # ثبت شکست حمایت (SHORT)
        elif prev_sup and price < prev_sup['low']:
            if symbol not in self.recorded_levels:
                self.recorded_levels[symbol] = {
                    'zone': prev_sup.copy(),
                    'type': 'SHORT',
                    'record_time': now,
                    'break_time_ms': now_ms
                }

                logger.log_system_event(
                    f"🧠 [{symbol}] Breakdown recorded below "
                    f"{prev_sup['low']:.2f} | Price={price:.2f}"
                )

        # فراموشی شکست پس از ۱۲ ساعت
        if symbol in self.recorded_levels:
            mem = self.recorded_levels[symbol]
            time_passed = (now - mem['record_time']).total_seconds()

            if time_passed > (12 * 3600):
                logger.log_system_event(
                    f"🧹 [{symbol}] Recorded breakout expired after 12 hours."
                )
                del self.recorded_levels[symbol]

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")
        h1d = await self.market_data.get_tf_data(symbol, "D")

        if not all([h4, h1, m15]):
            return

        price = h4['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        # ۱. پیدا کردن سطوح (اول ۴ ساعته، بعد روزانه)
        res_zone, sup_zone, res_c, sup_c, source_res, source_sup = self._find_levels(
            h4, h1d, price, trend
        )

        # ۲. تشخیص شکست بر اساس زون‌های ذخیره‌شده از چرخه قبلی
        self._register_and_manage(symbol, res_zone, sup_zone, price)

        # فقط وقتی شکست ثبت نشده، زون‌های جدید ذخیره شوند
        if symbol not in self.recorded_levels:
            self.last_zones[symbol] = {
                'res': res_zone.copy() if res_zone else None,
                'sup': sup_zone.copy() if sup_zone else None
            }

        if not res_zone or not sup_zone:
            if symbol not in self.recorded_levels:
                logger.log_system_event(
                    f"⚠️ {symbol}: level not found "
                    f"(res={bool(res_zone)}, sup={bool(sup_zone)}) "
                    f"| price={price:.4f}"
                )
                return
            else:
                mem = self.recorded_levels[symbol]

                if mem['type'] == 'SHORT':
                    sup_zone = mem['zone']
                    source_sup = "MEM"

                elif mem['type'] == 'LONG':
                    res_zone = mem['zone']
                    source_res = "MEM"

        # 📊 لاگ سطوح
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += (
            f"   ↳ Res Zone [{source_res}]: "
            f"{res_zone['low']:.2f} - {res_zone['high']:.2f}\n"
        )
        log_msg += (
            f"   ↳ Sup Zone [{source_sup}]: "
            f"{sup_zone['low']:.2f} - {sup_zone['high']:.2f}\n"
        )
        logger.log_system_event(log_msg)

        if symbol not in self.active_setups:
            self.active_setups[symbol] = {
                'state': 'IDLE',
                'zone': None,
                'direction': None
            }

        state = self.active_setups[symbol]

        h1_close = h1['closes'][-1]
        h1_open = h1['opens'][-1]
        h1_high = h1['highs'][-1]
        h1_low = h1['lows'][-1]
        h1_time = h1['times'][-1]
        m15_time = m15['times'][-1]

        # ======================================================
        # شروع بررسی تأیید شکست یک‌ساعته
        # ======================================================
        if state['state'] == 'IDLE' and symbol in self.recorded_levels:
            mem = self.recorded_levels[symbol]
            z = mem['zone']

            if mem['type'] == 'SHORT' and trend == "DOWNTREND":
                state.update({
                    'state': 'WAIT_1H_BREAK',
                    'zone': z,
                    'direction': 'SHORT'
                })

                logger.log_system_event(
                    f"🔔 [{symbol}] Breakdown recorded. "
                    f"Waiting for 1H close below {z['low']:.2f}..."
                )

            elif mem['type'] == 'LONG' and trend == "UPTREND":
                state.update({
                    'state': 'WAIT_1H_BREAK',
                    'zone': z,
                    'direction': 'LONG'
                })

                logger.log_system_event(
                    f"🔔 [{symbol}] Breakout recorded. "
                    f"Waiting for 1H close above {z['high']:.2f}..."
                )

        # ======================================================
        # انتظار برای تأیید بسته شدن کندل ۱ ساعته پس از شکست
        # ======================================================
        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']

            # زمان بسته شدن آخرین کندل کامل ۱ ساعته
            h1_close_time_ms = h1_time + (60 * 60 * 1000)

            break_time_ms = self.recorded_levels.get(
                symbol, {}
            ).get('break_time_ms', 0)

            # کندل قبل از ثبت شکست نباید تأیید کند
            if h1_close_time_ms <= break_time_ms:
                return

            # تأیید شکست حمایت (SHORT)
            if state['direction'] == 'SHORT':
                if h1_close < z['low']:
                    state.update({'state': 'WAIT_15M_RETEST'})

                    logger.log_system_event(
                        f"✅ [{symbol}] 1H candle CLOSED below support zone. "
                        f"Close={h1_close:.2f} | Waiting for 15M Retest..."
                    )

                elif h1_close > z['high']:
                    state.update({
                        'state': 'IDLE',
                        'zone': None,
                        'direction': None
                    })

                    if symbol in self.recorded_levels:
                        del self.recorded_levels[symbol]

                    self.last_zones[symbol] = {
                        'res': res_zone.copy() if res_zone else None,
                        'sup': sup_zone.copy() if sup_zone else None
                    }

                    logger.log_system_event(
                        f"🔄 [{symbol}] Fakeout! 1H closed above zone. "
                        f"Break cancelled."
                    )

            # تأیید شکست مقاومت (LONG)
            elif state['direction'] == 'LONG':
                if h1_close > z['high']:
                    state.update({'state': 'WAIT_15M_RETEST'})

                    logger.log_system_event(
                        f"✅ [{symbol}] 1H candle CLOSED above resistance zone. "
                        f"Close={h1_close:.2f} | Waiting for 15M Retest..."
                    )

                elif h1_close < z['low']:
                    state.update({
                        'state': 'IDLE',
                        'zone': None,
                        'direction': None
                    })

                    if symbol in self.recorded_levels:
                        del self.recorded_levels[symbol]

                    self.last_zones[symbol] = {
                        'res': res_zone.copy() if res_zone else None,
                        'sup': sup_zone.copy() if sup_zone else None
                    }

                    logger.log_system_event(
                        f"🔄 [{symbol}] Fakeout! 1H closed below zone. "
                        f"Break cancelled."
                    )

        # ======================================================
        # انتظار برای Retest در تایم‌فریم ۱۵ دقیقه
        # ======================================================
        elif state['state'] == 'WAIT_15M_RETEST':
            z = state['zone']

            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            # بررسی فیک‌بریک در زمان انتظار Retest
            if state['direction'] == 'SHORT' and h1_close > z['high']:
                state.update({
                    'state': 'IDLE',
                    'zone': None
                })

                logger.log_system_event(
                    f"🔄 [{symbol}] Fakeout in 15M wait! "
                    f"Reverting to IDLE."
                )
                return

            elif state['direction'] == 'LONG' and h1_close < z['low']:
                state.update({
                    'state': 'IDLE',
                    'zone': None
                })

                logger.log_system_event(
                    f"🔄 [{symbol}] Fakeout in 15M wait! "
                    f"Reverting to IDLE."
                )
                return

            triggered = False

            # شرط ورود SHORT
            if state['direction'] == 'SHORT':
                if (
                    m15_high >= z['low']
                    and m15_close < z['low']
                    and m15_close < m15_open
                ):
                    triggered = True

            # شرط ورود LONG
            elif state['direction'] == 'LONG':
                if (
                    m15_low <= z['high']
                    and m15_close > z['high']
                    and m15_close > m15_open
                ):
                    triggered = True

            if triggered:
                if (
                    self.last_signal_h1.get(symbol) == h1_time
                    or self.last_signal_15m.get(symbol) == m15_time
                ):
                    return

                logger.log_system_event(
                    f"🏆 [{symbol}] {state['direction']} Entry Triggered!"
                )

                self.send_signal(
                    symbol,
                    state['direction'],
                    m15_close,
                    z,
                    h4['atr'],
                    m15_high,
                    m15_low
                )

                self.last_signal_h1[symbol] = h1_time
                self.last_signal_15m[symbol] = m15_time

                state.update({
                    'state': 'IDLE',
                    'zone': None
                })

                if symbol in self.recorded_levels:
                    del self.recorded_levels[symbol]

    def send_signal(
        self,
        symbol,
        direction,
        entry,
        zone,
        atr,
        m15_high,
        m15_low
    ):
        if not self.telegram_bot:
            return

        if direction == 'SHORT':
            sl = max(zone['high'], m15_high)
        else:
            sl = min(zone['low'], m15_low)

        risk = abs(entry - sl)

        if risk < atr * 0.2:
            risk = atr * 1.0

        tp = (
            entry - (risk * 1.5)
            if direction == 'SHORT'
            else entry + (risk * 1.5)
        )

        signal = {
            'symbol': symbol,
            'type': direction,
            'entry_price': entry,
            'stop_loss': sl,
            'take_profit_1': tp,
            'take_profit_2': 0,
            'strategy': 'V71_4H_Priority_1D_Backup'
        }

        asyncio.create_task(
            self.telegram_bot.send_signal(signal)
        )
