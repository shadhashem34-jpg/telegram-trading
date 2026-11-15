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
        
        # تنظیمات زون
        self.ZONE_WIDTH_PCT = 0.00225 # 0.225% عرض زون
        self.LOOKBACK_2H = 12         # ۱۲ کندل ۲ ساعته
        self.LOOKBACK_4H = 18         # ۱۸ کندل ۴ ساعته (۳ روز)

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V38 (Lightweight) Started - 2H & 4H Zones")

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
        h2 = await self.market_data.get_tf_data(symbol, "120")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h2, h1, m15]): return

        price = h2['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        zone_width = price * self.ZONE_WIDTH_PCT
        half_w = zone_width / 2

        # --- محاسبه سطوح 2H (12 کندل) ---
        h2_high = max(h2['highs'][-self.LOOKBACK_2H:])
        h2_low = min(h2['lows'][-self.LOOKBACK_2H:])
        
        res_2h = {'low': h2_high - half_w, 'high': h2_high + half_w, 'type': 'res'}
        sup_2h = {'low': h2_low - half_w, 'high': h2_low + half_w, 'type': 'sup'}

        # --- محاسبه سطوح 4H (18 کندل) ---
        h4_high = max(h4['highs'][-self.LOOKBACK_4H:])
        h4_low = min(h4['lows'][-self.LOOKBACK_4H:])
        
        res_4h = {'low': h4_high - half_w, 'high': h4_high + half_w, 'type': 'res'}
        sup_4h = {'low': h4_low - half_w, 'high': h4_low + half_w, 'type': 'sup'}

        # ترکیب زون‌ها
        all_zones = [res_2h, sup_2h, res_4h, sup_4h]

        # لاگ‌گیری شفاف
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += f"   ↳ 2H Res Zone: {res_2h['low']:.2f} - {res_2h['high']:.2f}\n"
        log_msg += f"   ↳ 2H Sup Zone: {sup_2h['low']:.2f} - {sup_2h['high']:.2f}\n"
        log_msg += f"   ↳ 4H Res Zone: {res_4h['low']:.2f} - {res_4h['high']:.2f}\n"
        log_msg += f"   ↳ 4H Sup Zone: {sup_4h['low']:.2f} - {sup_4h['high']:.2f}\n"
        logger.log_system_event(log_msg)

        # --- ماشین وضعیت (State Machine) ---
        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None}

        state = self.active_setups[symbol]

        if state['state'] == 'IDLE':
            for z in all_zones:
                # بررسی برخورد قیمت با هر زون
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
                # سایه وارد زون شده اما بدنه زیر زون بسته شده (Rejection)
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry (Rejection at Res)!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
                
                # شکست به بالا (Breakout)
                elif m15_close > z['high']:
                    state.update({'state': 'WAIT_PB_SHORT', 'zone': z})
                    logger.log_system_event(f"📝 [{symbol}] Resistance broken. Waiting for pullback...")
            
            elif z['type'] == 'sup':
                # سایه وارد زون شده اما بدنه بالای زون بسته شده (Rejection)
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry (Rejection at Sup)!")
                    self.send_signal(symbol, 'LONG', m15_close, z, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
                
                # شکست به پایین (Breakout)
                elif m15_close < z['low']:
                    state.update({'state': 'WAIT_PB_LONG', 'zone': z})
                    logger.log_system_event(f"📝 [{symbol}] Support broken. Waiting for pullback...")

        elif state['state'] in ['WAIT_PB_SHORT', 'WAIT_PB_LONG']:
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if state['state'] == 'WAIT_PB_SHORT':
                # پولبک به مقاومت شکسته شده و برگشت
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry (Pullback after Breakout)!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h2['atr'])
                    state.update({'state': 'IDLE', 'zone': None})

            elif state['state'] == 'WAIT_PB_LONG':
                # پولبک به حمایت شکسته شده و برگشت
                if m15_low <= z['high'] and m15_close > z['high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry (Pullback after Breakout)!")
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
            'strategy': 'V38_Lightweight'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
