import asyncio
import numpy as np
from datetime import datetime, timezone
from logger import get_trading_logger
import pyvsmc as smc

logger = get_trading_logger()

class StrategyManager:
    def __init__(self, telegram_bot=None):
        self.running = False
        self.telegram_bot = telegram_bot
        self.market_data = None
        self.active_setups = {} 
        
        # تنظیمات
        self.LOOKBACK_2H = 12   # ۱۲ کندل ۲ ساعته
        self.LOOKBACK_4H = 18   # ۱۸ کندل ۴ ساعته
        self.MAX_ZONE_WIDTH_PCT = 0.00225 # حداکثر عرض زون 0.225%

    def set_market_data_handler(self, handler):
        self.market_data = handler

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("🚀 V39 Started (pyvsmc Order Blocks Test)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    def _get_obs(self, data, lookback):
        """استخراج اوردر بلاک‌ها با pyvsmc"""
        # تبدیل لیست‌ها به numpy array (چون pyvsmc با numpy کار میکنه)
        o = np.array(data['opens'][-lookback:])
        h = np.array(data['highs'][-lookback:])
        l = np.array(data['lows'][-lookback:])
        c = np.array(data['closes'][-lookback:])
        
        # تشخیص اوردر بلاک (متد پیش‌فرض pyvsmc)
        # خروجی معمولاً یک دیکشنری یا آرایه از سطوح است
        try:
            obs = smc.detect_order_blocks(o, h, l, c)
            return obs
        except Exception as e:
            # اگر کتابخانه ورودی متفاوتی خواست
            return []

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h2 = await self.market_data.get_tf_data(symbol, "120")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h2, h1, m15]): return

        price = h2['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        # استخراج اوردر بلاک‌ها
        obs_2h = self._get_obs(h2, self.LOOKBACK_2H)
        obs_4h = self._get_obs(h4, self.LOOKBACK_4H)
        
        # برای تست، فعلاً فقط لاگ می‌کنیم که آیا pyvsmc چیزی پیدا کرده یا نه
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += f"   ↳ 2H OBs found: {len(obs_2h) if obs_2h is not None else 0}\n"
        log_msg += f"   ↳ 4H OBs found: {len(obs_4h) if obs_4h is not None else 0}\n"
        
        # اگر pyvsmc خروجی استاندارد داد، اینجا نمایش میدیم
        if isinstance(obs_2h, list) and len(obs_2h) > 0:
            log_msg += f"   ↳ First 2H OB: {obs_2h[0]}\n"
            
        logger.log_system_event(log_msg)
        
        # نکته: منطق ورود (State Machine) فعلاً غیرفعال است تا ببینیم pyvsmc چه خروجی می‌دهد.
        # اگر خروجی درست بود، در مرحله بعد منطق ورود را به آن وصل می‌کنیم.
