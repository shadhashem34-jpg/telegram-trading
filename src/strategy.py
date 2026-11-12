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
        logger.log_system_event("Strategy Initialized (Golden V19 - 15 Candle Range)")

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

        # 🟢 ۱. استخراج سقف و کف ۱۵ کندل اخیر (بدون پیچیدگی)
        recent_highs = h1_highs[-16:-1] # ۱۵ کندل قبل از کندل فعلی
        recent_lows = h1_lows[-16:-1]
        
        range_high = max(recent_highs)
        range_low = min(recent_lows)
        
        # 🟢 ۲. لاگ‌گیری رادار (سقف و کف واضح ۱۵ ساعته)
        logger.log_system_event(f"📡 [V19 RADAR] {symbol} | Price: {m15_close:.2f} | 15H High: {range_high:.2f} | 15H Low: {range_low:.2f}")

        # 🎯 ۳. تشخیص ستاپ (برخورد با سقف یا کف ۱۵ ساعته)
        
        # حالت فروش (SELL): اگر روند نزولی است و قیمت به سقف ۱۵ ساعته نزدیک شده
        if is_downtrend:
            # شکار نقدینگی (سایه بالای ۱۵ کندل رفته، اما بدنه زیر آن بسته شده)
            if m15_high > range_high and m15_close < range_high and m15_close < m15_open:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'SELL', 'level': range_high, 'time': now}
                    logger.log_system_event(f"📝 [V19] Swept High {range_high:.2f}. Waiting for confirmation...")
            
            # شکست نزولی (بدنه ۱ ساعته زیر کف ۱۵ ساعته بسته شده -> منتظر پولبک)
            elif h1.get('closes', [0])[-1] < range_low and h1.get('opens', [0])[-1] > range_low:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': range_low, 'time': now}
                    logger.log_system_event(f"📝 [V19] 1H Breakout Below {range_low:.2f}. Waiting for retest...")

        # حالت خرید (BUY): اگر روند صعودی است و قیمت به کف ۱۵ ساعته نزدیک شده
        elif is_uptrend:
            # شکار نقدینگی (سایه زیر ۱۵ کندل رفته، اما بدنه بالای آن بسته شده)
            if m15_low < range_low and m15_close > range_low and m15_close > m15_open:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'BUY', 'level': range_low, 'time': now}
                    logger.log_system_event(f"📝 [V19] Swept Low {range_low:.2f}. Waiting for confirmation...")

            # شکست صعودی (بدنه ۱ ساعته بالای سقف ۱۵ ساعته بسته شده -> منتظر پولبک)
            elif h1.get('closes', [0])[-1] > range_high and h1.get('opens', [0])[-1] < range_high:
                if symbol not in self.watchlist:
                    self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': range_high, 'time': now}
                    logger.log_system_event(f"📝 [V19] 1H Breakout Above {range_high:.2f}. Waiting for retest...")

        # 🎯 ۴. تایید نهایی و صدور سیگنال
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 4: # انقضای ۴ ساعته
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
                'strategy': 'Golden_V19'
            }
            logger.log_system_event(f"🏆 GOLDEN V19 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
