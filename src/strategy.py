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
        logger.log_system_event("Strategy Initialized (Golden V14 - Pure Liquidity & 1TP)")

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

        # ۱. پیدا کردن پیوت‌های ۲۴ کندل اخیر و فیلتر کردن سطوح مصرف شده
        fresh_highs = []
        fresh_lows = []
        
        for i in range(2, len(h1_highs) - 2):
            if len(h1_highs) - i > 24: continue 
            
            # سقف معتبر
            if h1_highs[i] > h1_highs[i-1] and h1_highs[i] > h1_highs[i-2] and \
               h1_highs[i] > h1_highs[i+1] and h1_highs[i] > h1_highs[i+2]:
                is_swept = False
                for j in range(i + 1, len(h1_highs)):
                    if h1_highs[j] > h1_highs[i]:
                        is_swept = True
                        break
                if not is_swept:
                    fresh_highs.append(h1_highs[i])
                    
            # کف معتبر
            if h1_lows[i] < h1_lows[i-1] and h1_lows[i] < h1_lows[i-2] and \
               h1_lows[i] < h1_lows[i+1] and h1_lows[i] < h1_lows[i+2]:
                is_swept = False
                for j in range(i + 1, len(h1_lows)):
                    if h1_lows[j] < h1_lows[i]:
                        is_swept = True
                        break
                if not is_swept:
                    fresh_lows.append(h1_lows[i])

        # ۲. تشخیص شکار نقدینگی با نزدیک‌ترین سطح تمیز
        if is_downtrend:
            valid_highs = [h for h in fresh_highs if h > m15_close]
            if valid_highs:
                nearest_bsl = min(valid_highs)
                logger.log_system_event(f"🔍 [V14] {symbol} Down. Fresh BSL: {nearest_bsl:.2f} | 15M H:{m15_high:.2f} C:{m15_close:.2f}")
                
                if m15_high > nearest_bsl and m15_close < nearest_bsl and m15_close < m15_open:
                    self.watchlist[symbol] = {'type': 'SELL', 'level': nearest_bsl, 'time': now}
                    logger.log_system_event(f"📝 [V14] BSL Swept for {symbol} at {nearest_bsl}. Waiting for confirmation...")
                    return None

        elif is_uptrend:
            valid_lows = [l for l in fresh_lows if l < m15_close]
            if valid_lows:
                nearest_ssl = max(valid_lows)
                logger.log_system_event(f"🔍 [V14] {symbol} Up. Fresh SSL: {nearest_ssl:.2f} | 15M L:{m15_low:.2f} C:{m15_close:.2f}")

                if m15_low < nearest_ssl and m15_close > nearest_ssl and m15_close > m15_open:
                    self.watchlist[symbol] = {'type': 'BUY', 'level': nearest_ssl, 'time': now}
                    logger.log_system_event(f"📝 [V14] SSL Swept for {symbol} at {nearest_ssl}. Waiting for confirmation...")
                    return None

        # ۳. تایید نهایی و صدور سیگنال (فقط یک تی پی با RR=2)
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 4: 
                del self.watchlist[symbol]
                return None

            level = watch['level']
            
            if watch['type'] == 'SELL' and is_downtrend:
                if m15_high >= level and m15_close < level and m15_close < m15_open:
                    signal_type = "SELL"
                    del self.watchlist[symbol]
                    
                    stop_loss = m15_high + (atr_15m * 0.5) 
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0) # همون تی پی قبلی (RR=2)
                else: return None

            elif watch['type'] == 'BUY' and is_uptrend:
                if m15_low <= level and m15_close > level and m15_close > m15_open:
                    signal_type = "BUY"
                    del self.watchlist[symbol]
                    
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0) # همون تی پی قبلی (RR=2)
                else: return None
            else: return None
        else: return None

        # 🏆 نهایی‌سازی سیگنال (فقط TP1)
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
                'strategy': 'Golden_V14'
            }
            logger.log_system_event(f"🏆 GOLDEN V14 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
