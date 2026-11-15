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
        self.LOOKBACK_4H = 24   # بررسی ۲۴ کندل ۴ ساعته
        self.MAX_ZONE_WIDTH_PCT = 0.0025 # عرض زون 0.25%

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V44 Started (1-Candle Pivot -> 4H Zones)")

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
        highs = data['highs'][-lookback:]
        lows = data['lows'][-lookback:]
        closes = data['closes'][-lookback:]
        
        swing_highs = []
        swing_lows = []

        # حداکثر عرض مجاز زون (۰.۲۵ درصد قیمت)
        max_width = current_price * self.MAX_ZONE_WIDTH_PCT

        # 🟢 تغییر یافته: بررسی ۱ کندل چپ و ۱ کندل راست
        for i in range(1, len(highs) - 1):
            # مقاومت (سقف)
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                base = closes[i] 
                wick = highs[i]  
                
                if abs(wick - base) > max_width:
                    zone_low = base
                    zone_high = base + max_width
                else:
                    zone_low = base
                    zone_high = wick
                
                if zone_low > zone_high: zone_low, zone_high = zone_high, zone_low
                swing_highs.append({'low': zone_low, 'high': zone_high, 'type': 'res'})
            
            # حمایت (کف)
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                base = closes[i] 
                wick = lows[i]   
                
                if abs(wick - base) > max_width:
                    zone_high = base
                    zone_low = base - max_width
                else:
                    zone_high = base
                    zone_low = wick
                
                if zone_low > zone_high: zone_low, zone_high = zone_high, zone_low
                swing_lows.append({'low': zone_low, 'high': zone_high, 'type': 'sup'})

        nearest_res = None
        valid_highs = [z for z in swing_highs if z['low'] > current_price] 
        if valid_highs: nearest_res = min(valid_highs, key=lambda z: z['low'])

        nearest_sup = None
        valid_lows = [z for z in swing_lows if z['high'] < current_price] 
        if valid_lows: nearest_sup = max(valid_lows, key=lambda z: z['high'])

        return nearest_res, nearest_sup

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]): return

        price = h4['price'] 
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        res_4h, sup_4h = self.find_nearest_zones(h4, self.LOOKBACK_4H, price)

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
                # قیمت وارد زون ۴ ساعته شده
                if z['low'] <= price <= z['high']:
                    direction = 'SHORT' if z['type'] == 'res' else 'LONG'
                    state.update({'state': 'WAIT_1H_BREAK', 'zone': z, 'direction': direction})
                    logger.log_system_event(f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone ({z['low']:.2f}-{z['high']:.2f}). Waiting for 1H Body Break...")
                    break

        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']
            h1_close = h1['closes'][-1]
            
            if state['direction'] == 'SHORT':
                if h1_close < z['low']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res. Waiting for 15M Pullback...")
                elif h1_close > z['high'] + (h4['atr'] * 0.5): 
                    state.update({'state': 'IDLE', 'zone': None})
            elif state['direction'] == 'LONG':
                if h1_close > z['high']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup. Waiting for 15M Pullback...")
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
            'strategy': 'V44_4H_Structure'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
