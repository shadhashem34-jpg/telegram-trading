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
        logger.log_system_event("Strategy Initialized (Golden V22 - Multi-Timeframe Levels)")

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

        # ۱. فیلتر روند (دست نخورده)
        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0)
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0)
        
        now = datetime.now(timezone.utc)
        m15_close = m15.get('close', 0)
        m15_high = m15.get('high', 0)
        m15_low = m15.get('low', 0)
        m15_open = m15.get('open', 0)
        atr_15m = m15.get('atr', 0.0010)

        # ۲. سطوح کلان (۴ ساعته)
        major_res = max(h4.get('highs', [m15_high]))
        major_sup = min(h4.get('lows', [m15_low]))

        # ۳. سطوح خرد (۱ ساعته)
        minor_res_list = h1.get('highs', [])
        minor_sup_list = h1.get('lows', [])
        
        # ۴. حذف تداخل (اگر سطح ۱ ساعته به سطح ۴ ساعته نزدیک بود، حذف شود)
        valid_minor_res = [m for m in minor_res_list if abs(m - major_res) >= (atr_15m * 0.5)]
        valid_minor_sup = [m for m in minor_sup_list if abs(m - major_sup) >= (atr_15m * 0.5)]

        # پیدا کردن نزدیک‌ترین سطح خرد معتبر (اگر وجود داشت)
        nearest_minor_res = min([m for m in valid_minor_res if m > m15_close], default=None)
        nearest_minor_sup = max([m for m in valid_minor_sup if m < m15_close], default=None)

        # لاگ‌گیری کامل برای اینکه ببینیم ربات چی می‌بینه
        logger.log_system_event(f"📡 [V22 RADAR] {symbol} | Price: {m15_close:.2f}")
        logger.log_system_event(f"   ↳ 4H Major: Res {major_res:.2f} | Sup {major_sup:.2f}")
        if nearest_minor_res: logger.log_system_event(f"   ↳ 1H Minor Res: {nearest_minor_res:.2f}")
        if nearest_minor_sup: logger.log_system_event(f"   ↳ 1H Minor Sup: {nearest_minor_sup:.2f}")

        # ۵. تشخیص شکست و قفل کردن سطح
        if is_downtrend:
            # بررسی شکست سطح کلان
            if m15_close < major_sup and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': major_sup, 'time': now, 'tf': '4H'}
                logger.log_system_event(f"📝 [V22] Major 4H Breakdown below {major_sup:.2f}. LOCKED. Waiting for Retest...")
            # بررسی شکست سطح خرد
            elif nearest_minor_sup and m15_close < nearest_minor_sup and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': nearest_minor_sup, 'time': now, 'tf': '1H'}
                logger.log_system_event(f"📝 [V22] Minor 1H Breakdown below {nearest_minor_sup:.2f}. LOCKED. Waiting for Retest...")

        elif is_uptrend:
            # بررسی شکست سطح کلان
            if m15_close > major_res and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': major_res, 'time': now, 'tf': '4H'}
                logger.log_system_event(f"📝 [V22] Major 4H Breakout above {major_res:.2f}. LOCKED. Waiting for Retest...")
            # بررسی شکست سطح خرد
            elif nearest_minor_res and m15_close > nearest_minor_res and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': nearest_minor_res, 'time': now, 'tf': '1H'}
                logger.log_system_event(f"📝 [V22] Minor 1H Breakout above {nearest_minor_res:.2f}. LOCKED. Waiting for Retest...")

        # ۶. شکار پولبک در تایم ۱۵ دقیقه (دست نخورده)
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 8: 
                del self.watchlist[symbol]
                return None

            level = watch['level']
            signal_type = None

            if watch['type'] == 'SELL_RETEST' and is_downtrend:
                if m15_high >= level and m15_close < level and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY_RETEST' and is_uptrend:
                if m15_low <= level and m15_close > level and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

        # ۷. نهایی‌سازی سیگنال (دست نخورده)
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
                'strategy': 'Golden_V22'
            }
            logger.log_system_event(f"🏆 GOLDEN V22 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
