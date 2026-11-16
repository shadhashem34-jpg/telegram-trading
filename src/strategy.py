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
        
        # تنظیمات
        self.LOOKBACK_CANDLES = 24  # بررسی ۲۴ کندل ۴ ساعته
        self.ZONE_WIDTH_PCT = 0.0025 # عرض زون 0.25%

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V53 Started (Body-Based Levels)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]): return

        price = h4['price'] 
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        opens = h4['opens'][-self.LOOKBACK_CANDLES:]
        closes = h4['closes'][-self.LOOKBACK_CANDLES:]
        
        if not opens or not closes: return

        # 🟢 منطق جدید: پیدا کردن بالاترین و پایین‌ترین بدنه (Body)
        # بدنه یعنی بیشترین و کمترین مقدار بین Open و Close
        highest_body = max([max(o, c) for o, c in zip(opens, closes)])
        lowest_body = min([min(o, c) for o, c in zip(opens, closes)])

        # ساخت زون‌ها با عرض ۰.۲۵ درصد
        res_zone = {'low': highest_body, 'high': highest_body + (highest_body * self.ZONE_WIDTH_PCT), 'type': 'res'}
        sup_zone = {'low': lowest_body - (lowest_body * self.ZONE_WIDTH_PCT), 'high': lowest_body, 'type': 'sup'}

        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += f"   ↳ Nearest Res (Highest Body): {highest_body:.2f} | Zone: {res_zone['low']:.2f} - {res_zone['high']:.2f}\n"
        log_msg += f"   ↳ Nearest Sup (Lowest Body): {lowest_body:.2f} | Zone: {sup_zone['low']:.2f} - {sup_zone['high']:.2f}\n"
        logger.log_system_event(log_msg)

        # --- ماشین وضعیت (State Machine) ---
        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None, 'direction': None}

        state = self.active_setups[symbol]
        all_zones = [res_zone, sup_zone]

        if state['state'] == 'IDLE':
            for z in all_zones:
                if z['low'] <= price <= z['high']:
                    direction = 'SHORT' if z['type'] == 'res' else 'LONG'
                    state.update({'state': 'WAIT_1H_BREAK', 'zone': z, 'direction': direction})
                    logger.log_system_event(f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone ({z['low']:.2f}-{z['high']:.2f}). Waiting for 1H Confirmation...")
                    break

        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']
            h1_close = h1['closes'][-1]
            h1_open = h1['opens'][-1]
            h1_high = h1['highs'][-1]
            h1_low = h1['lows'][-1]
            
            if state['direction'] == 'SHORT':
                if h1_close < z['low']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res. Waiting for 15M Retest...")
                elif h1_high >= z['low'] and h1_close < z['low'] and h1_close < h1_open:
                     state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                     logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Res. Waiting for 15M Retest...")
                elif h1_close > z['high'] + (h4['atr'] * 0.5): 
                    state.update({'state': 'IDLE', 'zone': None})
            
            elif state['direction'] == 'LONG':
                if h1_close > z['high']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup. Waiting for 15M Retest...")
                elif h1_low <= z['high'] and h1_close > z['high'] and h1_close > h1_open:
                     state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                     logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Sup. Waiting for 15M Retest...")
                elif h1_close < z['low'] - (h4['atr'] * 0.5):
                    state.update({'state': 'IDLE', 'zone': None})

        elif state['state'] == 'WAIT_15M_RETEST':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if state['direction'] == 'SHORT':
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry Triggered!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h4['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
            elif state['direction'] == 'LONG':
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry Triggered!")
                    self.send_signal(symbol, 'LONG', m15_close, z, h4['atr'])
                    state.update({'state': 'IDLE', 'zone': None})

    def send_signal(self, symbol, direction, entry, zone, atr):
        if not self.telegram_bot: return
        sl = zone['high'] if direction == 'SHORT' else zone['low']
        risk = abs(entry - sl)
        if risk < atr * 0.2: risk = atr * 1.0 
        
        tp = entry - (risk * 1.5) if direction == 'SHORT' else entry + (risk * 1.5)
        
        signal = {
            'symbol': symbol, 'type': direction, 'entry_price': entry,
            'stop_loss': sl, 'take_profit_1': tp, 'take_profit_2': 0,
            'strategy': 'V53_Body_Levels'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
