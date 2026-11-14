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
        self.watchlist = {} # حافظه برای قفل کردن سطح
        logger.log_system_event("Strategy Initialized (Golden V29 - Full Log & Level Lock)")

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
        
        # ساخت زون باریک (0.1 ATR) برای نمایش در لاگ
        buffer = m15_atr * 0.1
        res_zone = f"{max_close:.2f} - {max_close + buffer:.2f}"
        sup_zone = f"{min_close - buffer:.2f} - {min_close:.2f}"

        # 🟢 لاگ ۱: نمایش قیمت لحظه‌ای و زون‌های ۴ ساعته
        logger.log_system_event(f"📡 [V29 RADAR] {symbol} | Price: {m15_close:.2f}")
        logger.log_system_event(f"   ↳ 4H Res Zone: {res_zone}")
        logger.log_system_event(f"   ↳ 4H Sup Zone: {sup_zone}")

        target_level = None
        trade_type = None
        
        if is_uptrend:
            valid_highs = [c for c in recent_closes if c > m15_close]
            if valid_highs:
                target_level = min(valid_highs)
                trade_type = "BUY"
        elif is_downtrend:
            valid_lows = [c for c in recent_closes if c < m15_close]
            if valid_lows:
                target_level = max(valid_lows)
                trade_type = "SELL"

        if not target_level: return None

        # 🟢 منطق قفل کردن سطح (Level Lock) در صورت شکست با بدنه ۱ ساعته
        if symbol not in self.watchlist:
            # برای فروش: اگر کندل ۱ ساعته با بدنه زیر سطح بسته شد
            if trade_type == "SELL" and h1_close < target_level:
                self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': target_level, 'time': now}
                logger.log_system_event(f"📝 [V29] 1H Body Breakdown below {target_level:.2f}. Level LOCKED. Waiting for 15M retest...")
                return None
            # برای خرید: اگر کندل ۱ ساعته با بدنه بالای سطح بسته شد
            elif trade_type == "BUY" and h1_close > target_level:
                self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': target_level, 'time': now}
                logger.log_system_event(f"📝 [V29] 1H Body Breakout above {target_level:.2f}. Level LOCKED. Waiting for 15M retest...")
                return None
            else:
                # اگر هنوز شکستی رخ نداده، فقط لاگ می‌کنیم که منتظرشه
                logger.log_system_event(f"   ↳ Waiting for 1H Body Break of {target_level:.2f} to LOCK...")
                return None

        # 🎯 شکار پولبک در تایم ۱۵ دقیقه (اگر سطح قفل شده باشد)
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 8: # انقضای ۸ ساعته
                del self.watchlist[symbol]
                return None

            level = watch['level']
            signal_type = None

            if watch['type'] == 'SELL_RETEST' and is_downtrend:
                # سایه بالا به سطح خورده و بدنه زیر سطح قرمز بسته شده
                if m15_high >= level and m15_close < level and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (m15_atr * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY_RETEST' and is_uptrend:
                if m15_low <= level and m15_close > level and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (m15_atr * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

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
                    'strategy': 'Golden_V29'
                }
                logger.log_system_event(f"🏆 GOLDEN V29 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
                return signal
        return None
