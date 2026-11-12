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
        logger.log_system_event("Strategy Initialized (Golden V16 - Nearest Swing Levels)")

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

        if len(h1_highs) < 5: return None

        # 🟢 ۱. پیدا کردن سقف‌ها و کف‌های نوسانی معتبر در ۴۸ کندل اخیر
        valid_swing_highs = []
        valid_swing_lows = []

        for i in range(2, len(h1_highs) - 2):
            # سقف نوسانی: از ۲ کندل چپ و ۲ کندل راست بالاتر باشد
            if h1_highs[i] > h1_highs[i-1] and h1_highs[i] > h1_highs[i-2] and \
               h1_highs[i] > h1_highs[i+1] and h1_highs[i] > h1_highs[i+2]:
                # آیا بعد از تشکیل این سقف، قیمت ازش رد شده؟ (مصرف شده)
                is_swept = any(h1_highs[j] > h1_highs[i] for j in range(i + 1, len(h1_highs)))
                if not is_swept:
                    valid_swing_highs.append(h1_highs[i])

            # کف نوسانی: از ۲ کندل چپ و ۲ کندل راست پایین‌تر باشد
            if h1_lows[i] < h1_lows[i-1] and h1_lows[i] < h1_lows[i-2] and \
               h1_lows[i] < h1_lows[i+1] and h1_lows[i] < h1_lows[i+2]:
                is_swept = any(h1_lows[j] < h1_lows[i] for j in range(i + 1, len(h1_lows)))
                if not is_swept:
                    valid_swing_lows.append(h1_lows[i])

        # 🟢 ۲. پیدا کردن نزدیک‌ترین سطح معتبر به قیمت فعلی
        # نزدیک‌ترین مقاومت (پایین‌ترین سقف معتبری که بالای قیمت فعلی است)
        nearest_resistance = min([h for h in valid_swing_highs if h > m15_close], default=None)
        
        # نزدیک‌ترین حمایت (بالاترین کف معتبری که زیر قیمت فعلی است)
        nearest_support = max([l for l in valid_swing_lows if l < m15_close], default=None)

        # 🟢 ۳. لاگ‌گیری دقیق (بدون پیچیدگی)
        if nearest_resistance:
            logger.log_system_event(f"🔍 [V16] {symbol} Down. Nearest Fresh Resistance: {nearest_resistance:.2f} | Price: {m15_close:.2f}")
        if nearest_support:
            logger.log_system_event(f"🔍 [V16] {symbol} Up. Nearest Fresh Support: {nearest_support:.2f} | Price: {m15_close:.2f}")

        # 🎯 ۴. تشخیص ستاپ (شکست با بدنه ۱ ساعته یا سویپ ۱۵ دقیقه‌ای روی سطح نزدیک)
        if is_downtrend and nearest_resistance:
            # شکست با بدنه ۱ ساعته (Breakout)
            if h1_closes[-1] > nearest_resistance and h1_opens[-1] < nearest_resistance:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': nearest_resistance, 'time': now}
                    logger.log_system_event(f"📝 [V16] 1H Breakout UP at {nearest_resistance:.2f}. Waiting for pullback...")
            # شکار نقدینگی (Sweep)
            elif m15_high > nearest_resistance and m15_close < nearest_resistance and m15_close < m15_open:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'SELL_SWEEP', 'level': nearest_resistance, 'time': now}
                    logger.log_system_event(f"📝 [V16] Liquidity Swept UP at {nearest_resistance:.2f}. Waiting for confirmation...")

        elif is_uptrend and nearest_support:
            # شکست با بدنه ۱ ساعته (Breakout)
            if h1_closes[-1] < nearest_support and h1_opens[-1] > nearest_support:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': nearest_support, 'time': now}
                    logger.log_system_event(f"📝 [V16] 1H Breakout DOWN at {nearest_support:.2f}. Waiting for pullback...")
            # شکار نقدینگی (Sweep)
            elif m15_low < nearest_support and m15_close > nearest_support and m15_close > m15_open:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'BUY_SWEEP', 'level': nearest_support, 'time': now}
                    logger.log_system_event(f"📝 [V16] Liquidity Swept DOWN at {nearest_support:.2f}. Waiting for confirmation...")

        # 🎯 ۵. تایید نهایی
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

            elif watch['type'] == 'SELL_SWEEP' and is_downtrend:
                if m15_high > level and m15_close < level and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY_SWEEP' and is_uptrend:
                if m15_low < level and m15_close > level and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

        # 🏆 نهایی‌سازی
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
                'strategy': 'Golden_V16'
            }
            logger.log_system_event(f"🏆 GOLDEN V16 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
