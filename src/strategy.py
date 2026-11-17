
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

        # V59 SETTINGS
        self.LOOKBACK_CANDLES = 12
        self.MAX_LOOKBACK = 30
        self.ZONE_WIDTH_PCT = 0.0025
        self.DROP_FORMING_CANDLE = 'auto'

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V59 Started (30 Candle Level Scan)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]

        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(
                        f"❌ Error in {symbol}: {e}"
                    )

                await asyncio.sleep(15)

            await asyncio.sleep(180)

    def _collect_candidates(self, o, h, l, c, price, lookback, trend):

        O = o[-lookback:]
        H = h[-lookback:]
        L = l[-lookback:]
        C = c[-lookback:]

        n = len(O)

        res_c = []
        sup_c = []

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

                if not (
                    check_left_res
                    and inside_left_body(top, i)
                ):
                    res_c.append((top, H[i]))

            if bot < price and bot <= later_bot:

                if not (
                    check_left_sup
                    and inside_left_body(bot, i)
                ):
                    sup_c.append((bot, L[i]))

            later_top = max(later_top, top)
            later_bot = min(later_bot, bot)

        return res_c, sup_c

    def _find_nearest_levels(self, h4, price, trend):

        o = list(h4['opens'])
        h = list(h4['highs'])
        l = list(h4['lows'])
        c = list(h4['closes'])

        # Validate candle data
        n = min(len(o), len(h), len(l), len(c))

        if n < 5:
            return None, None, [], []

        o = o[-n:]
        h = h[-n:]
        l = l[-n:]
        c = c[-n:]

        # Forming candle handling
        if self.DROP_FORMING_CANDLE == 'auto':

            if abs(c[-1] - price) <= price * 0.0002:
                o, h, l, c = (
                    o[:-1],
                    h[:-1],
                    l[:-1],
                    c[:-1]
                )

        elif self.DROP_FORMING_CANDLE:
            o, h, l, c = (
                o[:-1],
                h[:-1],
                l[:-1],
                c[:-1]
            )

        if len(o) < 5:
            return None, None, [], []

        # V59: Scan all available candles up to MAX_LOOKBACK
        lookback = min(self.MAX_LOOKBACK, len(o))

        res_c, sup_c = self._collect_candidates(
            o, h, l, c,
            price,
            lookback,
            trend
        )

        res_zone = None
        sup_zone = None

        # Nearest resistance above current price
        if res_c:

            body = min(x[0] for x in res_c)

            wick = max(
                x[1] for x in res_c
                if x[0] == body
            )

            top = min(
                max(wick, body),
                body * (1 + self.ZONE_WIDTH_PCT)
            )

            res_zone = {
                'low': body,
                'high': top,
                'type': 'res'
            }

        # Nearest support below current price
        if sup_c:

            body = max(x[0] for x in sup_c)

            wick = min(
                x[1] for x in sup_c
                if x[0] == body
            )

            bottom = max(
                min(wick, body),
                body * (1 - self.ZONE_WIDTH_PCT)
            )

            sup_zone = {
                'low': bottom,
                'high': body,
                'type': 'sup'
            }

        return res_zone, sup_zone, res_c, sup_c

    async def process_symbol(self, symbol):

        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]):
            return

        price = h4['price']

        trend = (
            "UPTREND"
            if price > h4['ema_200']
            else "DOWNTREND"
        )

        res_zone, sup_zone, res_c, sup_c = (
            self._find_nearest_levels(h4, price, trend)
        )

        if not res_zone or not sup_zone:

            logger.log_system_event(
                f"⚠️ {symbol}: level not found "
                f"(res={bool(res_zone)}, sup={bool(sup_zone)})"
            )

            return

        # Log last candles
        last3 = [
            (
                round(h4['opens'][i], 2),
                round(h4['closes'][i], 2)
            )
            for i in (-3, -2, -1)
        ]

        log_msg = (
            f"📊 {symbol} — 4H: {trend} | "
            f"Price: {price:.2f}\n"
        )

        log_msg += f"   ↳ Last 3 candles (open,close): {last3}\n"

        log_msg += (
            f"   ↳ Res Zone: "
            f"{res_zone['low']:.2f} - {res_zone['high']:.2f} "
            f"| candidates(body): "
            f"{sorted(round(x[0], 2) for x in res_c)}\n"
        )

        log_msg += (
            f"   ↳ Sup Zone: "
            f"{sup_zone['low']:.2f} - {sup_zone['high']:.2f} "
            f"| candidates(body): "
            f"{sorted((round(x[0], 2) for x in sup_c), reverse=True)}\n"
        )

        logger.log_system_event(log_msg)

        if symbol not in self.active_setups:

            self.active_setups[symbol] = {
                'state': 'IDLE',
                'zone': None,
                'direction': None
            }

        state = self.active_setups[symbol]

        all_zones = [res_zone, sup_zone]

        h1_close = h1['closes'][-1]
        h1_open = h1['opens'][-1]
        h1_high = h1['highs'][-1]
        h1_low = h1['lows'][-1]

        # =====================================
        # STATE 1: IDLE
        # =====================================

        if state['state'] == 'IDLE':

            # Fast bearish breakdown from resistance
            if (
                res_zone
                and h1_close < res_zone['low']
                and h1_close < h1_open
            ):

                if h1['highs'][-2] >= res_zone['low']:

                    state.update({
                        'state': 'WAIT_15M_RETEST',
                        'zone': res_zone,
                        'direction': 'SHORT'
                    })

                    logger.log_system_event(
                        f"⚡ [{symbol}] Fast 1H Breakdown "
                        f"below Res! Waiting for 15M Retest..."
                    )

                    return

            # Fast bullish breakout above support
            if (
                sup_zone
                and h1_close > sup_zone['high']
                and h1_close > h1_open
            ):

                if h1['lows'][-2] <= sup_zone['high']:

                    state.update({
                        'state': 'WAIT_15M_RETEST',
                        'zone': sup_zone,
                        'direction': 'LONG'
                    })

                    logger.log_system_event(
                        f"⚡ [{symbol}] Fast 1H Breakout "
                        f"above Sup! Waiting for 15M Retest..."
                    )

                    return

            # Normal zone touch
            for z in all_zones:

                if z['low'] <= price <= z['high']:

                    direction = (
                        'SHORT'
                        if z['type'] == 'res'
                        else 'LONG'
                    )

                    state.update({
                        'state': 'WAIT_1H_BREAK',
                        'zone': z,
                        'direction': direction
                    })

                    logger.log_system_event(
                        f"🔔 [{symbol}] Price entered "
                        f"{z['type'].upper()} Zone. "
                        f"Waiting for 1H Confirmation..."
                    )

                    break

        # =====================================
        # STATE 2: WAIT_1H_BREAK
        # =====================================

        elif state['state'] == 'WAIT_1H_BREAK':

            z = state['zone']

            if state['direction'] == 'SHORT':

                if h1_close < z['low']:

                    state.update({
                        'state': 'WAIT_15M_RETEST',
                        'direction': 'SHORT'
                    })

                    logger.log_system_event(
                        f"✅ [{symbol}] 1H Body CLOSED "
                        f"below Res. Waiting for 15M Retest..."
                    )

                elif (
                    h1_high >= z['low']
                    and h1_close < z['low']
                    and h1_close < h1_open
                ):

                    state.update({
                        'state': 'WAIT_15M_RETEST',
                        'direction': 'SHORT'
                    })

                    logger.log_system_event(
                        f"✅ [{symbol}] 1H Rejection at Res. "
                        f"Waiting for 15M Retest..."
                    )

                elif h1_close > z['high'] + (h4['atr'] * 0.5):

                    state.update({
                        'state': 'IDLE',
                        'zone': None
                    })

            elif state['direction'] == 'LONG':

                if h1_close > z['high']:

                    state.update({
                        'state': 'WAIT_15M_RETEST',
                        'direction': 'LONG'
                    })

                    logger.log_system_event(
                        f"✅ [{symbol}] 1H Body CLOSED "
                        f"above Sup. Waiting for 15M Retest..."
                    )

                elif (
                    h1_low <= z['high']
                    and h1_close > z['high']
                    and h1_close > h1_open
                ):

                    state.update({
                        'state': 'WAIT_15M_RETEST',
                        'direction': 'LONG'
                    })

                    logger.log_system_event(
                        f"✅ [{symbol}] 1H Rejection at Sup. "
                        f"Waiting for 15M Retest..."
                    )

                elif h1_close < z['low'] - (h4['atr'] * 0.5):

                    state.update({
                        'state': 'IDLE',
                        'zone': None
                    })

        # =====================================
        # STATE 3: WAIT_15M_RETEST
        # =====================================

        elif state['state'] == 'WAIT_15M_RETEST':

            z = state['zone']

            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if state['direction'] == 'SHORT':

                if (
                    m15_high >= z['low']
                    and m15_close < z['low']
                    and m15_close < m15_open
                ):

                    logger.log_system_event(
                        f"🏆 [{symbol}] SHORT Entry Triggered!"
                    )

                    self.send_signal(
                        symbol,
                        'SHORT',
                        m15_close,
                        z,
                        h4['atr']
                    )

                    state.update({
                        'state': 'IDLE',
                        'zone': None
                    })

            elif state['direction'] == 'LONG':

                if (
                    m15_low <= z['high']
                    and m15_close > z['high']
                    and m15_close > m15_open
                ):

                    logger.log_system_event(
                        f"🏆 [{symbol}] LONG Entry Triggered!"
                    )

                    self.send_signal(
                        symbol,
                        'LONG',
                        m15_close,
                        z,
                        h4['atr']
                    )

                    state.update({
                        'state': 'IDLE',
                        'zone': None
                    })

    def send_signal(self, symbol, direction, entry, zone, atr):

        if not self.telegram_bot:
            return

        sl = (
            zone['high']
            if direction == 'SHORT'
            else zone['low']
        )

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
            'strategy': 'V59_30Candle_Levels'
        }

        asyncio.create_task(
            self.telegram_bot.send_signal(signal)
        )
