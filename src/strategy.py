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
        logger.log_system_event("🚀 V39.1 Started (pyvsmc Order Blocks Test)")

        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"]
        
        while self.running:
            for symbol in symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.log_system_event(f"❌ Error in {symbol}: {e}")
                await asyncio.sleep(15)
            
            await asyncio.sleep(180) 

    def _get_obs_info(self, data, lookback):
        """استخراج و شمارش اوردر بلاک‌ها با pyvsmc (اصلاح‌شده)"""
        o = np.array(data['opens'][-lookback:])
        h = np.array(data['highs'][-lookback:])
        l = np.array(data['lows'][-lookback:])
        c = np.array(data['closes'][-lookback:])
        
        try:
            # تشخیص اوردر بلاک با حالت zone_mode="body"
            obs = smc.detect_order_blocks(o, h, l, c, lookback=5, zone_mode="body")
            
            # شمارش تعداد اوردر بلاک‌های صعودی و نزولی با sum روی ماسک بولی
            bull_count = int(np.sum(obs.bullish_ob)) if obs.bullish_ob is not None else 0
            bear_count = int(np.sum(obs.bearish_ob)) if obs.bearish_ob is not None else 0
            
            # استخراج محدوده آخرین اوردر بلاک (اگر وجود داشته باشد)
            last_ob_info = "None"
            if bull_count > 0 or bear_count > 0:
                # در اینجا فقط یک نمونه از ob_high و ob_low را برمی‌داریم
                # برای نمایش در لاگ، اولین عنصر غیر نال را پیدا می‌کنیم
                valid_highs = obs.ob_high[~np.isnan(obs.ob_high)] if obs.ob_high is not None else []
                valid_lows = obs.ob_low[~np.isnan(obs.ob_low)] if obs.ob_low is not None else []
                if len(valid_highs) > 0 and len(valid_lows) > 0:
                    last_ob_info = f"High: {valid_highs[-1]:.2f}, Low: {valid_lows[-1]:.2f}"
            
            return bull_count, bear_count, last_ob_info
            
        except Exception as e:
            logger.log_system_event(f"pyvsmc error: {e}")
            return 0, 0, "Error"

    async def process_symbol(self, symbol):
        h4 = await self.market_data.get_tf_data(symbol, "240")
        h2 = await self.market_data.get_tf_data(symbol, "120")
        h1 = await self.market_data.get_tf_data(symbol, "60")
        m15 = await self.market_data.get_tf_data(symbol, "15")

        if not all([h4, h2, h1, m15]): return

        price = h2['price']
        trend = "UPTREND" if price > h4['ema_200'] else "DOWNTREND"
        
        # استخراج اطلاعات اوردر بلاک‌ها
        bull_2h, bear_2h, info_2h = self._get_obs_info(h2, self.LOOKBACK_2H)
        bull_4h, bear_4h, info_4h = self._get_obs_info(h4, self.LOOKBACK_4H)
        
        # لاگ‌گیری شفاف
        log_msg = f"📊 {symbol} — 4H: {trend} | Price: {price:.2f}\n"
        log_msg += f"   ↳ 2H OBs: Bull={bull_2h}, Bear={bear_2h} | Last OB: {info_2h}\n"
        log_msg += f"   ↳ 4H OBs: Bull={bull_4h}, Bear={bear_4h} | Last OB: {info_4h}\n"
        
        logger.log_system_event(log_msg)
        
        # نکته: منطق ورود (State Machine) فعلاً غیرفعال است تا خروجی pyvsmc را بررسی کنیم.
        # اگر خروجی منطقی بود، در مرحله بعد منطق ورود را به آن وصل می‌کنیم.
