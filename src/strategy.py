import asyncio
import pandas as pd
from datetime import datetime, timezone
from logger import get_trading_logger

from market_structure.hydrate import hydrate
from market_structure.atr import _compute_atr

logger = get_trading_logger()

class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.active_setups = {}
        
        # تنظیمات
        self.LOOKBACK_2H = 12   # ۱۲ کندل ۲ ساعته
        self.LOOKBACK_4H = 18   # ۱۸ کندل ۴ ساعته (۳ روز)
        self.ZONE_WIDTH_PCT = 0.00225 # 0.225% (حداکثر عرض)

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V37 Started (market-structure: 2H & 4H Zones)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180)

    def _df_from_data(self, data):
        """تبدیل دیتای خام به DataFrame"""
        return pd.DataFrame({
            'open': data['opens'],
            'high': data['highs'],
            'low': data['lows'],
            'close': data['closes'],
            'volume': [1.0] * len(data['closes']) # حجم ساختگی (لازم نیست)
        })

    def _get_zones_from_df(self, df, atr):
        """استخراج زونها از دیتافریم با market-structure"""
        helper = hydrate(df)
        zones = []
        
        # زونهای حمایت
        for z in helper.get_support_zones(atr_arr=atr):
            body_low, body_high = z.range
            wick_low, wick_high = z.wick_range
            zones.append({
                'type': 'sup',
                'body_low': body_low, 'body_high': body_high,
                'wick_low': wick_low, 'wick_high': wick_high
            })
        
        # زونهای مقاومت (با استفاده از get_resistance_zones)
        for z in helper.get_resistance_zones(atr_arr=atr):
            body_low, body_high = z.range
            wick_low, wick_high = z.wick_range
            zones.append({
                'type': 'res',
                'body_low': body_low, 'body_high': body_high,
                'wick_low': wick_low, 'wick_high': wick_high
            })
        
        return zones

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h2 = await self.market_data.get_tf_data(symbol, "120")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h2, h1, m15]): return

        price = h2['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"

        # دیتای ۲ ساعته (۱۲ کندل آخر)
        df_2h = self._df_from_data(h2).iloc[-self.LOOKBACK_2H:]
        atr_2h = _compute_atr(df_2h['high'].values, df_2h['low'].values, df_2h['close'].values, period=14)
        zones_2h = self._get_zones_from_df(df_2h, atr_2h)

        # دیتای ۴ ساعته (۱۸ کندل آخر = ۳ روز)
        df_4h = self._df_from_data(h4).iloc[-self.LOOKBACK_4H:]
        atr_4h = _compute_atr(df_4h['high'].values, df_4h['low'].values, df_4h['close'].values, period=14)
        zones_4h = self._get_zones_from_df(df_4h, atr_4h)

        # ترکیب همه زونها
        all_zones = zones_2h + zones_4h

        # لاگگیری
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        for z in all_zones[:4]: # فقط ۴ زون اول را نشان بده
            log_msg += f"   ↳ {z['type'].upper()}: body({z['body_low']:.2f}-{z['body_high']:.2f}) wick({z['wick_low']:.2f}-{z['wick_high']:.2f})\n"
        logger.log_system_event(log_msg)

        # ماشین وضعیت (بررسی برخورد قیمت با زونها)
        if symbol not in self.active_setups:
            self.active_setups[symbol] = {'state': 'IDLE', 'zone': None}

        state = self.active_setups[symbol]

        if state['state'] == 'IDLE':
            for z in all_zones:
                # بررسی برخورد قیمت با زون (بر اساس body)
                if z['body_low'] <= price <= z['body_high']:
                    state.update({'state': 'IN_ZONE', 'zone': z})
                    logger.log_system_event(f"🔔 [{symbol}] Price entered {z['type'].upper()} zone. Waiting for 15M Rejection...")
                    break

        elif state['state'] == 'IN_ZONE':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]

            if z['type'] == 'res':
                # سایه وارد زون شده اما بدنه زیر body_low بسته شده
                if m15_high >= z['body_low'] and m15_close < z['body_low'] and m15_close < m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] SHORT Entry (Rejection at Res Zone)!")
                    self.send_signal(symbol, 'SHORT', m15_close, z, atr_2h)
                    state.update({'state': 'IDLE', 'zone': None})
            elif z['type'] == 'sup':
                # سایه وارد زون شده اما بدنه بالای body_high بسته شده
                if m15_low <= z['body_high'] and m15_close > z['body_high'] and m15_close > m15_open:
                    logger.log_system_event(f"🏆 [{symbol}] LONG Entry (Rejection at Sup Zone)!")
                    self.send_signal(symbol, 'LONG', m15_close, z, atr_2h)
                    state.update({'state': 'IDLE', 'zone': None})

            # اگر قیمت بدون ریجکشن از زون رد شد (شکست) -> به حالت انتظار پولبک برو
            if m15_close > z['body_high'] and z['type'] == 'res':
                state.update({'state': 'WAIT_PB_SHORT', 'zone': z})
                logger.log_system_event(f"📝 [{symbol}] Resistance broken. Waiting for pullback...")
            elif m15_close < z['body_low'] and z['type'] == 'sup':
                state.update({'state': 'WAIT_PB_LONG', 'zone': z})
                logger.log_system_event(f"📝 [{symbol}] Support broken. Waiting for pullback...")

        elif state['state'] == 'WAIT_PB_SHORT':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_high = m15['highs'][-1]
            m15_open = m15['opens'][-1]
            if m15_high >= z['body_low'] and m15_close < z['body_low'] and m15_close < m15_open:
                logger.log_system_event(f"🏆 [{symbol}] SHORT Entry (Pullback after Breakout)!")
                self.send_signal(symbol, 'SHORT', m15_close, z, atr_2h)
                state.update({'state': 'IDLE', 'zone': None})

        elif state['state'] == 'WAIT_PB_LONG':
            z = state['zone']
            m15_close = m15['closes'][-1]
            m15_low = m15['lows'][-1]
            m15_open = m15['opens'][-1]
            if m15_low <= z['body_high'] and m15_close > z['body_high'] and m15_close > m15_open:
                logger.log_system_event(f"🏆 [{symbol}] LONG Entry (Pullback after Breakout)!")
                self.send_signal(symbol, 'LONG', m15_close, z, atr_2h)
                state.update({'state': 'IDLE', 'zone': None})

    def send_signal(self, symbol, direction, entry, zone, atr):
        if not self.telegram_bot: return
        sl = zone['wick_high'] if direction == 'SHORT' else zone['wick_low']
        risk = abs(entry - sl)
        if risk < atr * 0.2: risk = atr * 1.0
        
        tp = entry - (risk * 1.5) if direction == 'SHORT' else entry + (risk * 1.5)
        
        signal = {
            'symbol': symbol, 'type': direction, 'entry_price': entry,
            'stop_loss': sl, 'take_profit_1': tp, 'take_profit_2': 0,
            'strategy': 'V37_MarketStructure'
        }
        asyncio.create_task(self.telegram_bot.send_signal(signal))
