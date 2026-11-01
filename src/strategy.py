import asyncio
from typing import Optional
from datetime import datetime, timezone, time
from logger import get_trading_logger

logger = get_trading_logger()

class StrategyManager:
    def __init__(self):
        self.running = False
        self.signals_today = 0
        self.max_daily_signals = 2 # حداکثر ۲ سیگنال در روز
        self.telegram_bot = None  
        self.market_data = None   
        self.last_crypto_signal_time = None # برای فیلتر همبستگی
        logger.log_system_event("Golden Strategy Manager Initialized")

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("Golden Strategy Started (Scanning 25 Assets...)")
        
        symbols = [
            "XAUUSD", "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
            "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "DOGEUSDT", 
            "LTCUSDT", "BCHUSDT", "TRXUSDT", "UNIUSDT", "ATOMUSDT", 
            "ETCUSDT", "FILUSDT", "APTUSDT", "SUIUSDT", "ARBUSDT", 
            "OPUSDT", "NEARUSDT", "INJUSDT", "MATICUSDT"
        ] 
        
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
                
                await asyncio.sleep(0.5)
            
            await asyncio.sleep(60)

    async def stop(self):
        self.running = False
        logger.log_system_event("StrategyManager stopped")

    def is_trading_session_active(self) -> bool:
        now = datetime.now(timezone.utc)
        current_time = now.time()
        # 11:00 AM to 03:00 AM Iran Time
        start_time = time(7, 30)
        end_time = time(23, 30)
        
        in_window = start_time <= current_time <= end_time
        london_open_block = time(7, 45) <= current_time <= time(8, 15)
        ny_open_block = time(12, 45) <= current_time <= time(13, 15)
        
        if not in_window or london_open_block or ny_open_block: 
            return False
        return True

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: 
            return None
        if not self.is_trading_session_active(): 
            return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m30 = market_data.get('m30', {})
        
        if not h4 or not h1 or not m30: 
            return None

        # 🛡️ لایه اول: فیلتر روند 4 ساعته
        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        if not (is_uptrend or is_downtrend): 
            return None

        # 🎯 لایه دوم: ناحیه کمین 1 ساعته (EMA 20)
        atr_h1 = h1.get('atr', 0)
        entry_price = m30.get('close', 0)
        ema_20_h1 = h1.get('ema_20', 0)
        
        # قیمت باید نزدیک EMA 20 (فاصله کمتر از 1 ATR)
        near_ema_20 = abs(entry_price - ema_20_h1) < atr_h1

        # ⚡ لایه سوم: ماشه ورود 30 دقیقه + تایید حجم 1.2 برابر
        volume_ok = m30.get('volume', 0) > (m30.get('volume_avg_20', 0) * 1.2)
        breakout_up = entry_price > m30.get('high_5', 0)
        breakout_down = entry_price < m30.get('low_5', 0)

        signal_type = None
        if is_uptrend and near_ema_20 and breakout_up and volume_ok: 
            signal_type = "BUY"
        elif is_downtrend and near_ema_20 and breakout_down and volume_ok: 
            signal_type = "SELL"

        if signal_type:
            # 🔗 فیلتر همبستگی (Correlation Filter): 
            # اگر سیگنال کریپتو است و قبلاً سیگنال کریپتو دادیم، صرف نظر کن
            if symbol != "XAUUSD":
                now = datetime.now(timezone.utc)
                if self.last_crypto_signal_time:
                    time_diff = (now - self.last_crypto_signal_time).total_seconds() / 3600
                    if time_diff < 4: # حداقل 4 ساعت فاصله بین سیگنال‌های کریپتو
                        return None
            
            sl_distance = 2.0 * atr_h1
            macro_high = h4.get('high_100', 9999999)
            macro_low = h4.get('low_100', 0)
            
            if signal_type == "BUY":
                # فیلتر ماکرو 1.0 ATR
                if (macro_high - entry_price) < (atr_h1 * 1.0): return None
                
                stop_loss = entry_price - sl_distance
                tp1 = max(h1.get('high_20', entry_price + (sl_distance * 1.5)), entry_price + (sl_distance * 1.5))
                
                # فیلتر RR: حداقل 1.5
                if (tp1 - entry_price) < (sl_distance * 1.5): return None
                
            else: # SELL
                # فیلتر ماکرو 1.0 ATR
                if (entry_price - macro_low) < (atr_h1 * 1.0): return None
                
                stop_loss = entry_price + sl_distance
                tp1 = min(h1.get('low_20', entry_price - (sl_distance * 1.5)), entry_price - (sl_distance * 1.5))
                
                # فیلتر RR: حداقل 1.5
                if (entry_price - tp1) < (sl_distance * 1.5): return None

            # ثبت سیگنال
            if symbol != "XAUUSD":
                self.last_crypto_signal_time = datetime.now(timezone.utc)
                
            self.signals_today += 1
            
            signal = {
                'symbol': symbol, 
                'type': signal_type, 
                'entry_price': entry_price, 
                'stop_loss': stop_loss, 
                'take_profit_1': tp1, 
                'take_profit_2': 0, # برای TP2 در این نسخه باز نمی‌کنیم تا ساده‌تر باشه
                'strategy': 'Golden_Strategy'
            }
            logger.log_system_event(f"🏆 GOLDEN SIGNAL: {symbol} ({signal_type}) | Entry: {entry_price}")
            return signal
        return None
