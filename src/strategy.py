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
        self.LOOKBACK_CANDLES = 40  # بررسی ۴۰ کندل ۴ ساعته (حدود ۶ روز) برای پیدا کردن سطوحی که واکنش داشتن
        self.ZONE_WIDTH_PCT = 0.0025 # عرض زون 0.25%
        self.ATR_MULTIPLIER = 1.5    # حداقل حرکت قوی برای تایید سطح

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V51 Started (Fractal + Reaction Levels)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    def find_reaction_levels(self, data, current_price, atr):
        """پیدا کردن سطوح معتبر بر اساس فراکتال و تایید با حرکت قوی (ATR)"""
        highs = data['highs'][-self.LOOKBACK_CANDLES:]
        lows = data['lows'][-self.LOOKBACK_CANDLES:]
        closes = data['closes'][-self.LOOKBACK_CANDLES:]
        opens = data['opens'][-self.LOOKBACK_CANDLES:]
        
        swing_highs = []
        swing_lows = []
        max_width = current_price * self.ZONE_WIDTH_PCT

        # بررسی کندل‌ها برای پیدا کردن فراکتال‌ها (۲ کندل چپ و ۲ کندل راست)
        for i in range(2, len(highs) - 2):
            # ۱. فراکتال سقف
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
               highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                # تایید با واکنش: آیا بعد از این سقف، قیمت حرکت قوی به پایین داشته؟
                future_move_down = closes[i] - min(lows[i+1:i+4])
                if future_move_down > (atr * self.ATR_MULTIPLIER):
                    base = closes[i]
                    wick = highs[i]
                    if abs(wick - base) > max_width:
                        zone_low, zone_high = base, base + max_width
                    else:
                        zone_low, zone_high = base, wick
                    swing_highs.append({'low': zone_low, 'high': zone_high, 'type': 'res'})

            # ۲. فراکتال کف
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
               lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                # تایید با واکنش: آیا بعد از این کف، قیمت حرکت قوی به بالا داشته؟
                future_move_up = max(highs[i+1:i+4]) - closes[i]
                if future_move_up > (atr * self.ATR_MULTIPLIER):
                    base = closes[i]
                    wick = lows[i]
                    if abs(wick - base) > max_width:
                        zone_high, zone_low = base, base - max_width
                    else:
                        zone_high, zone_low = base, wick
                    swing_lows.append({'low': zone_low, 'high': zone_high, 'type': 'sup'})

        # ادغام سطوح نزدیک به هم برای جلوگیری از تکرار
        swing_highs = self._merge_zones(swing_highs)
        swing_lows = self._merge_zones(swing_lows)

        # پیدا کردن نزدیک‌ترین سقف بالای قیمت و نزدیک‌ترین کف زیر قیمت
        nearest_res = None
        valid_highs = [z for z in swing_highs if z['low'] > current_price] 
        if valid_highs: nearest_res = min(valid_highs, key=lambda z: z['low'])

        nearest_sup = None
        valid_lows = [z for z in swing_lows if z['high'] < current_price] 
        if valid_lows: nearest_sup = max(valid_lows, key=lambda z: z['high'])

        return nearest_res, nearest_sup

    def _merge_zones(self, zones):
        """ادغام زون‌هایی که همپوشانی دارند"""
        if not zones: return []
        zones.sort(key=lambda x: x['low'])
        merged = [zones[0]]
        for current in zones[1:]:
            last = merged[-1]
            # اگر سطح فعلی با سطح قبلی همپوشانی داشت، ادغام کن
            if current['low'] <= last['high']:
                last['high'] = max(last['high'], current['high'])
            else:
                merged.append(current)
        return merged

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h1, m15]): return

        price = h4['price'] 
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        # استخراج سطوح بر اساس منطق جدید
        res_4h, sup_4h = self.find_reaction_levels(h4, price, h4['atr'])

        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += "⚙️ 4H Levels (Fractal + Reaction):\n"
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
                    logger.log_system_event(f"🔔 [{symbol}] Price entered {z['type'].upper()} Zone ({z['low']:.2f}-{z['high']:.2f}). Waiting for 1H Confirmation...")
                    break

        elif state['state'] == 'WAIT_1H_BREAK':
            z = state['zone']
            h1_close = h1['closes'][-1]
            h1_open = h1['opens'][-1]
            h1_high = h1['highs'][-1]
            h1_low = h1['lows'][-1]
            
            if state['direction'] == 'SHORT':
                # شکست به پایین (بدنه زیر زون)
                if h1_close < z['low']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED below Res (Breakout). Waiting for 15M Retest...")
                # ریجکشن (سایه داخل زون، بدنه قرمز زیر زون بسته شده)
                elif h1_high >= z['low'] and h1_close < z['low'] and h1_close < h1_open:
                     state.update({'state': 'WAIT_15M_RETEST', 'direction': 'SHORT'})
                     logger.log_system_event(f"✅ [{symbol}] 1H Rejection at Res. Waiting for 15M Retest...")
                # اگر برگشت بالا (فیک اوت)
                elif h1_close > z['high'] + (h4['atr'] * 0.5): 
                    state.update({'state': 'IDLE', 'zone': None})
            
            elif state['direction'] == 'LONG':
                # شکست به بالا (بدنه بالای زون)
                if h1_close > z['high']:
                    state.update({'state': 'WAIT_15M_RETEST', 'direction': 'LONG'})
                    logger.log_system_event(f"✅ [{symbol}] 1H Body CLOSED above Sup (Breakout). Waiting for 15M Retest...")
                # ریجکشن (سایه داخل زون، بدنه سبز بالای زون بسته شده)
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
                # سایه بالا داخل زون، بدنه قرمز زیر زون
                if m15_high >= z['low'] and m15_close < z['low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry Triggered!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, h4['atr'])
                    state.update({'state': 'IDLE', 'zone': None})
            elif state['direction'] == 'LONG':
                # سایه پایین داخل زون، بدنه سبز بالای زون
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
            'strategy': 'V51_Fractal_Reaction'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
