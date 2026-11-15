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
        self.LOOKBACK_2H = 24   # برای پیدا کردن پیوت‌های ۲ ساعته
        self.LOOKBACK_4H = 24   # برای پیدا کردن پیوت‌های ۴ ساعته

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V40 Started (Simple Nearest Swing Zones)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    def find_nearest_zones(self, data, lookback, current_price):
        """پیدا کردن نزدیک‌ترین سقف و کف (Swing) بالای/زیر قیمت و ساخت زون"""
        highs = data['highs'][-lookback:]
        lows = data['lows'][-lookback:]
        closes = data['closes'][-lookback:]
        
        swing_highs = []
        swing_lows = []

        # پیدا کردن سقف و کف‌های معتبر (۲ کندل چپ و ۲ کندل راست)
        for i in range(2, len(highs) - 2):
            # سقف
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
               highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                # زون مقاومت: از بدنه (Close) تا سایه (High)
                swing_highs.append({'low': closes[i], 'high': highs[i], 'type': 'res'})
            
            # کف
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
               lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                # زون حمایت: از سایه (Low) تا بدنه (Close)
                swing_lows.append({'low': lows[i], 'high': closes[i], 'type': 'sup'})

        # پیدا کردن نزدیک‌ترین سقف بالای قیمت
        nearest_res = None
        valid_highs = [z for z in swing_highs if z['low'] > current_price] # کف زون بالای قیمت باشه
        if valid_highs:
            nearest_res = min(valid_highs, key=lambda z: z['low'])

        # پیدا کردن نزدیک‌ترین کف زیر قیمت
        nearest_sup = None
        valid_lows = [z for z in swing_lows if z['high'] < current_price] # سقف زون زیر قیمت باشه
        if valid_lows:
            nearest_sup = max(valid_lows, key=lambda z: z['high'])

        return nearest_res, nearest_sup

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h2 = await self.market_data.get_tf_data(symbol, "120")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h2, h1, m15]): return

        price = h2['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        # پیدا کردن سطوح ۲ ساعته و ۴ ساعته
        res_2h, sup_2h = self.find_nearest_zones(h2, self.LOOKBACK_2H, price)
        res_4h, sup_4h = self.find_nearest_zones(h4, self.LOOKBACK_4H, price)

        # لاگ‌گیری دقیقاً مثل اعداد نقطه‌چین تو
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        
        log_msg += "⚙️ 2H Levels:\n"
        log_msg += f"   ↳ Nearest Res: {res_2h['low']:.2f} - {res_2h['high']:.2f}\n" if res_2h else "   ↳ Nearest Res: None\n"
        log_msg += f"   ↳ Nearest Sup: {sup_2h['low']:.2f} - {sup_2h['high']:.2f}\n" if sup_2h else "   ↳ Nearest Sup: None\n"

        log_msg += "⚙️ 4H Levels:\n"
        log_msg += f"   ↳ Nearest Res: {res_4h['low']:.2f} - {res_4h['high']:.2f}\n" if res_4h else "   ↳ Nearest Res: None\n"
        log_msg += f"   ↳ Nearest Sup: {sup_4h['low']:.2f} - {sup_4h['high']:.2f}\n" if sup_4h else "   ↳ Nearest Sup: None\n"
        
        logger.log_system_event(log_msg)

        # --- ماشین وضعیت (State Machine) ---
        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None}

        state = self.active_setups[symbol]
        
        # بررسی زون‌های ۲ ساعته (چون برای ترید روزانه نزدیک‌ترن)
        all_zones = []
        if res_2h: all_zones.append(res_2h)
        if sup_2h: all_zones.append(sup_2h)
        if res_4h: all_zones.append(res_4h)
        if sup_4h: all_zones.append(sup_4h)

        if state['state'] == 'IDLE':
            for z in all_zones:
                if z['low'] <= price <= z['high']:
                    state.update({'state': 'IN_ZONE', 'zone': z})
                    logger.log_system_event(f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone ({z['low']:.2f}-{z['high']:.2f}). Waiting for 15M Rejection...")
                    break

        elif state['state'] == 'IN_ZONE':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if z['type'] == 'res':
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry (Rejection at Res)!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
                elif m15_close > z['high']:
                    state.update({'state': 'WAIT_PB_SHORT', 'zone': z})
            
            elif z['type'] == 'sup':
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry (Rejection at Sup)!")
                    self.send_signal(symbol, 'LONG', m15_close, z, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
                elif m15_close < z['low']:
                    state.update({'state': 'WAIT_PB_LONG', 'zone': z})

        elif state['state'] in ['WAIT_PB_SHORT', 'WAIT_PB_LONG']:
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if state['state'] == 'WAIT_PB_SHORT':
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    self.send_signal(symbol, 'SHORT', m15_close, z, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
            elif state['state'] == 'WAIT_PB_LONG':
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    self.send_signal(symbol, 'LONG', m15_close, z, h2['atr'])
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
            'strategy': 'V40_SwingZones'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
