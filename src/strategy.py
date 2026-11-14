import asyncio
from typing import Optional
from datetime import datetime, timezone
from logger import get_trading_logger

logger = get_trading_logger()

class StrategyManager:
    def __init__(self):
        self.running = False
        self.signals_today = 0
        self.max_daily_signals = 2 
        self.telegram_bot = None  
        self.market_data = None   
        self.last_crypto_signal_time = None 
        self.watchlist = {} 
        logger.log_system_event("Strategy Initialized (Golden V28 - Final Simple Strategy)")

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("Strategy Monitoring Started (24/7 Mode)...")
        symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XAUUSDT"] 
        
        while self.running:
            for symbol in symbols:
                try:
                    if self.market_data:
                        data = await self.market_data.get_market_data(symbol)
                        if data:
                            signal = await self.generate_signal(symbol, data)
                            if signal:
                                if self.telegram_bot:
                                    await self.telegram_bot.send_signal(signal)
                except Exception as e:
                    logger.log_system_event(f"Error scanning {symbol}: {e}")
                await asyncio.sleep(30) 
            await asyncio.sleep(60)

    async def stop(self):
        self.running = False
        logger.log_system_event("StrategyManager stopped")

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m15 = market_data.get('m15', {})
        if not h4 or not h1 or not m15: return None

        # ۱. تشخیص روند
        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0)
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0)
        
        now = datetime.now(timezone.utc)
        m15_close = m15.get('close', 0)
        m15_high = m15.get('high', 0)
        m15_low = m15.get('low', 0)
        m15_open = m15.get('open', 0)
        m15_atr = m15.get('atr', 0.0010)
        h1_close = h1.get('close', 0)

        # ۲. پیدا کردن سطوح (بدنه‌ها) در ۲۴ کندل اخیر ۴ ساعته
        recent_closes = h4.get('closes', [])[-24:]
        if not recent_closes: return None
        
        max_close = max(recent_closes)
        min_close = min(recent_closes)

        # ۳. انتخاب نزدیک‌ترین سطح برای شکست
        # در روند صعودی: نزدیک‌ترین سقف (بالاترین بدنه) بالای قیمت
        # در روند نزولی: نزدیک‌ترین کف (پایین‌ترین بدنه) زیر قیمت
        
        target_level = None
        trade_type = None
        
        if is_uptrend:
            valid_highs = [c for c in recent_closes if c > m15_close]
            if valid_highs:
                target_level = min(valid_highs) # نزدیک‌ترین سقف
                trade_type = "BUY"
        elif is_downtrend:
            valid_lows = [c for c in recent_closes if c < m15_close]
            if valid_lows:
                target_level = max(valid_lows) # نزدیک‌ترین کف
                trade_type = "SELL"

        if not target_level: return None

        # ۴. ساخت زون باریک (0.1 ATR)
        buffer = m15_atr * 0.1
        
        # لاگ‌گیری
        logger.log_system_event(f"📡 [V28 RADAR] {symbol} | Price: {m15_close:.2f} | Target: {target_level:.2f} ({trade_type})")

        # ۵. ماشه ورود (شکست، رد شدن، یا پولبک)
        signal_type = None
        stop_loss = 0
        take_profit = 0

        # حالت اول: قیمت به سطح رسید و رد شد (Rejection)
        if trade_type == "SELL":
            if m15_high >= target_level and m15_close < target_level and m15_close < m15_open:
                signal_type = "SELL"
                stop_loss = m15_high + (m15_atr * 0.5)
                take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
        
        elif trade_type == "BUY":
            if m15_low <= target_level and m15_close > target_level and m15_close > m15_open:
                signal_type = "BUY"
                stop_loss = m15_low - (m15_atr * 0.5)
                take_profit = m15_close + ((m15_close - stop_loss) * 2.0)

        # حالت دوم: سطح با بدنه ۱ ساعته شکسته شد و حالا منتظر پولبک هستیم
        # (برای سادگی، فعلاً همون حالت اول رو نگه می‌داریم که تایید پولبک/رد شدن در ۱۵ دقیقه هست)

        # نهایی‌سازی سیگنال
        if signal_type:
            if symbol != "XAUUSDT" and self.last_crypto_signal_time:
                if (now - self.last_crypto_signal_time).total_seconds() / 3600 < 4: return None 

            if symbol != "XAUUSDT": self.last_crypto_signal_time = now
            self.signals_today += 1
            
            signal = {
                'symbol': symbol, 'type': signal_type, 'entry_price': m15_close, 
                'stop_loss': round(stop_loss, 4), 
                'take_profit_1': round(take_profit, 4), 
                'take_profit_2': 0, 
                'take_profit_3': 0,
                'strategy': 'Golden_V28'
            }
            logger.log_system_event(f"🏆 GOLDEN V28 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
