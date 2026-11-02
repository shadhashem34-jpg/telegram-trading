import asyncio
from typing import Optional
from datetime import datetime, timezone, time
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
        logger.log_system_event("Strategy Initialized (Pullback Logic Fixed)")

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("Strategy Monitoring Started...")
        
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
        start_time = time(7, 30)
        end_time = time(23, 30)
        in_window = start_time <= current_time <= end_time
        london_open_block = time(7, 45) <= current_time <= time(8, 15)
        ny_open_block = time(12, 45) <= current_time <= time(13, 15)
        if not in_window or london_open_block or ny_open_block: return False
        return True

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: return None
        if not self.is_trading_session_active(): return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m30 = market_data.get('m30', {})
        
        if not h4 or not h1 or not m30: return None

        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        if not (is_uptrend or is_downtrend): return None

        m30_open = m30.get('open', 0)
        m30_close = m30.get('close', 0)
        m30_high = m30.get('high', 0)
        m30_low = m30.get('low', 0)
        atr_h1 = h1.get('atr', 0.0010)
        ema_20_h1 = h1.get('ema_20', 0)
        now = datetime.now(timezone.utc)

        # ۱. تشخیص شکست با بدنه (Body Breakout)
        if is_uptrend and m30_close > m30_open and m30_close > m30.get('high_5', 0):
            self.watchlist[symbol] = {'type': 'BUY', 'level': m30.get('high_5', 0), 'time': now}
            logger.log_system_event(f"📝 Breakout UP registered for {symbol}. Waiting for pullback...")
            return None

        elif is_downtrend and m30_close < m30_open and m30_close < m30.get('low_5', 0):
            self.watchlist[symbol] = {'type': 'SELL', 'level': m30.get('low_5', 0), 'time': now}
            logger.log_system_event(f"📝 Breakout DOWN registered for {symbol}. Waiting for pullback...")
            return None

        # ۲. بررسی پولبک و ورود
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 4:
                del self.watchlist[symbol]
                return None

            if watch['type'] == 'BUY' and is_uptrend:
                if m30_low <= watch['level'] and m30_close > watch['level'] and m30_close < m30_open:
                    if abs(m30_close - ema_20_h1) < (atr_h1 * 1.5):
                        signal_type = "BUY"
                        del self.watchlist[symbol]
                    else: return None
                else: return None

            elif watch['type'] == 'SELL' and is_downtrend:
                if m30_high >= watch['level'] and m30_close < watch['level'] and m30_close < m30_open:
                    if abs(m30_close - ema_20_h1) < (atr_h1 * 1.5):
                        signal_type = "SELL"
                        del self.watchlist[symbol]
                    else: return None
                else: return None
            else: return None
        else: return None

        # ۳. نهایی‌سازی سیگنال و مدیریت ریسک
        if signal_type:
            if symbol != "XAUUSD":
                if self.last_crypto_signal_time:
                    c_time_diff = (now - self.last_crypto_signal_time).total_seconds() / 3600
                    if c_time_diff < 4: return None
            
            sl_distance = 2.0 * atr_h1
            macro_high = h4.get('high_100', 9999999)
            macro_low = h4.get('low_100', 0)
            atr_h4 = h4.get('atr', atr_h1)
            
            if signal_type == "BUY":
                if (macro_high - m30_close) < atr_h4: return None
                stop_loss = m30_close - sl_distance
                tp1 = m30_close + (sl_distance * 1.5)
            else:
                if (m30_close - macro_low) < atr_h4: return None
                stop_loss = m30_close + sl_distance
                tp1 = m30_close - (sl_distance * 1.5)

            if symbol != "XAUUSD": self.last_crypto_signal_time = now
            self.signals_today += 1
            
            signal = {
                'symbol': symbol, 'type': signal_type, 'entry_price': m30_close, 
                'stop_loss': stop_loss, 'take_profit_1': tp1, 'take_profit_2': 0, 
                'strategy': 'Golden_Pullback_Valid'
            }
            logger.log_system_event(f"🏆 PULLBACK ENTRY: {symbol} ({signal_type}) | Entry: {m30_close}")
            return signal
        return None
