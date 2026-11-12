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
        logger.log_system_event("Strategy Initialized (Golden V20 - Precise Zones)")

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
                
                await asyncio.sleep(15) 
            
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

        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0)
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0)
        
        now = datetime.now(timezone.utc)
        h1_highs = h1.get('highs', [])
        h1_lows = h1.get('lows', [])
        
        m15_close = m15.get('close', 0)
        m15_high = m15.get('high', 0)
        m15_low = m15.get('low', 0)
        m15_open = m15.get('open', 0)
        atr_15m = m15.get('atr', 0.0010)

        if len(h1_highs) < 16: return None

        # 🟢 ۱. استخراج سقف و کف ۱۵ کندل اخیر
        recent_highs = h1_highs[-16:-1] 
        recent_lows = h1_lows[-16:-1]
        
        range_high = max(recent_highs)
        range_low = min(recent_lows)
        
        # 🟢 ۲. ساخت ناحیه (Zone) با ضخامت باریک بر اساس ATR
        # ضخامت ناحیه رو 0.3 برابر ATR در نظر می‌گیریم (برای اتریوم میشه حدود ۵ تا ۸ دلار)
        zone_buffer = atr_15m * 0.3
        
        # ناحیه مقاومت: از range_high منهای بافر، تا خود range_high (مثلاً 2135 تا 2140)
        res_zone_bottom = range_high - zone_buffer
        res_zone_top = range_high
        
        # ناحیه حمایت: از range_low منهای بافر، تا خود range_low (مثلاً 2028 تا 2033)
        sup_zone_bottom = range_low - zone_buffer
        sup_zone_top = range_low
        
        # 🟢 ۳. لاگ‌گیری رادار (نمایش دقیق زون‌ها)
        logger.log_system_event(f"📡 [V20 RADAR] {symbol} | Price: {m15_close:.2f} | Res Zone: {res_zone_bottom:.2f} - {res_zone_top:.2f} | Sup Zone: {sup_zone_bottom:.2f} - {sup_zone_top:.2f}")

        # 🎯 ۴. تشخیص ستاپ (برخورد با زون‌ها)

        if is_downtrend:
            # آیا سایه کندل ۱۵ دقیقه‌ای وارد ناحیه مقاومت شده؟
            if m15_high >= res_zone_bottom and m15_high <= res_zone_top + (atr_15m * 0.1): 
                # تاییدیه: بدنه باید زیر ناحیه مقاومت (زیر res_zone_bottom) بسته بشه
                if m15_close < res_zone_bottom and m15_close < m15_open:
                    if symbol not in self.watchlist:
                        self.watchlist[symbol] = {'type': 'SELL', 'level': res_zone_bottom, 'time': now}
                        logger.log_system_event(f"📝 [V20] Rejected from Res Zone. Waiting for confirmation...")

            # شکست نزولی کف زون
            elif h1.get('closes', [0])[-1] < sup_zone_bottom and h1.get('opens', [0])[-1] > sup_zone_bottom:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': sup_zone_bottom, 'time': now}
                    logger.log_system_event(f"📝 [V20] 1H Breakout Below Sup Zone. Waiting for retest...")

        elif is_uptrend:
            # آیا سایه کندل ۱۵ دقیقه‌ای وارد ناحیه حمایت شده؟
            if m15_low <= sup_zone_top and m15_low >= sup_zone_bottom - (atr_15m * 0.1):
                # تاییدیه: بدنه باید بالای ناحیه حمایت (بالای sup_zone_top) بسته بشه
                if m15_close > sup_zone_top and m15_close > m15_open:
                    if symbol not in self.watchlist:
                        self.watchlist[symbol] = {'type': 'BUY', 'level': sup_zone_top, 'time': now}
                        logger.log_system_event(f"📝 [V20] Rejected from Sup Zone. Waiting for confirmation...")

            # شکست صعودی سقف زون
            elif h1.get('closes', [0])[-1] > res_zone_top and h1.get('opens', [0])[-1] < res_zone_top:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': res_zone_top, 'time': now}
                    logger.log_system_event(f"📝 [V20] 1H Breakout Above Res Zone. Waiting for retest...")

        # 🎯 ۵. تایید نهایی و صدور سیگنال
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 4: 
                del self.watchlist[symbol]
                return None

            level = watch['level']
            signal_type = None

            if watch['type'] == 'SELL' and is_downtrend:
                if m15_close < level and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'SELL_RETEST' and is_downtrend:
                if m15_high >= level and m15_close < level and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY' and is_uptrend:
                if m15_close > level and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY_RETEST' and is_uptrend:
                if m15_low <= level and m15_close > level and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

        # 🏆 نهایی‌سازی سیگنال
        if 'signal_type' in locals() and signal_type:
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
                'strategy': 'Golden_V20'
            }
            logger.log_system_event(f"🏆 GOLDEN V20 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
