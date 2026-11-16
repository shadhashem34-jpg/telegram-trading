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
        self.LOOKBACK_4H = 12   # ۱۲ کندل ۴ ساعته = ۴۸ ساعت گذشته
        self.MAX_ZONE_WIDTH_PCT = 0.0025 # عرض زون 0.25%

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V47 Started (Simple Nearest Bodies 4H -> 1H Confirm -> 15M Entry)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    def find_nearest_zones(self, data, current_price):
        """پیدا کردن نزدیک‌ترین سطوح با استفاده از بدنه (Close) ۱۲ کندل ۴ ساعته"""
        closes = data['closes'][-self.LOOKBACK_4H:]
        
        # محاسبه حداکثر عرض مجاز زون (۰.۲۵ درصد)
        max_width = current_price * self.MAX_ZONE_WIDTH_PCT

        # ۱. پیدا کردن نزدیک‌ترین مقاومت (بدنه‌هایی که بالای قیمت فعلی هستند)
        res_bodies = [c for c in closes if c > current_price]
        nearest_res = None
        if res_bodies:
            base = min(res_bodies) # پایین‌ترین بدنه بالای قیمت (نزدیک‌ترین به قیمت)
            nearest_res = {'low': base, 'high': base + max_width, 'type': 'res'}

        # ۲. پیدا کردن نزدیک‌ترین حمایت (بدنه‌هایی که زیر قیمت فعلی هستند)
        sup_bodies = [c for c in closes if c < current_price]
        nearest_sup = None
        if sup_bodies:
            base = max(sup_bodies) # بالاترین بدنه زیر قیمت (نزدیک‌ترین به قیمت)
            nearest_sup = {'low': base - max_width, 'high': base, 'type': 'sup'}

        return nearest_res, nearest_sup

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]): return

        price = h4['price'] 
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        res_4h, sup_4h = self.find_nearest_zones(h4, price)

        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += "⚙️ 4H Levels:\n"
        log_msg += f"   ↳ Nearest Res: {res_4h['low']:.2f} - {res_4h['high']:.2f}\n" if res_4h else "   ↳ Nearest Res: None\n"
        log_msg += f"   ↳ Nearest Sup: {sup_4h['low']:.2f} - {sup_4h['high']:.2f}\n" if sup_4h else "   ↳ Nearest Sup: None\n"
        logger.log_system_event(log_msg)

        # --- ماشین وضعیت (State Machine) ---
        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None, 'direction': None}

        state = self.active_setups[symbol]
        all_zones = [z for z in [res_4h, sup_4h] if z is not None]

        if state['state'] == 'IDLE':
            for z in all_zones:
                # قیمت وارد زون شده
                if z['low'] <= price <= z['high']:
                    direction = 'SHORT' if z['type'] == 'res' else 'LONG'
                    state.update({'state': 'WAIT_1H_BREAK', 'zone': z, 'direction': direction})
                    logger.log_system_event(f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone ({z['low']:.2f}-{z['high']:.2f}). Waiting for 1H Breakout/Rejection...")
                    break

        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']
            h1_close = h1['closes'][-1]
            
            if state['direction'] == 'SHORT':
                # شکست به پایین
                if h1_close < z['low']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res (Breakout). Waiting for 15M Retest...")
                # ریجکشن (اگر قیمت به زون خورده و قرمز بسته شده بالاتر از کف زون)
                elif h1_close < z['high'] and h1_close < h1['opens'][-1] and h1['highs'][-1] >= z['low']:
                     state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                     logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Res. Waiting for 15M Retest...")
                # اگر برگشت بالا (فیک اوت)
                elif h1_close > z['high'] + (h4['atr'] * 0.5): 
                    state.update({'state': 'IDLE', 'zone': None})
            
            elif state['direction'] == 'LONG':
                # شکست به بالا
                if h1_close > z['high']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup (Breakout). Waiting for 15M Retest...")
                # ریجکشن (اگر قیمت به زون خورده و سبز بسته شده پایین‌تر از سقف زون)
                elif h1_close > z['low'] and h1_close > h1['opens'][-1] and h1['lows'][-1] <= z['high']:
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
                # سایه بالا داخل زون، بدنه زیر زون
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry Triggered!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h4['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
            elif state['direction'] == 'LONG':
                # سایه پایین داخل زون، بدنه بالای زون
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
            'strategy': 'V47_Simple_Levels'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
