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
        logger.log_system_event("Strategy Initialized (Golden V6 - Pivots & 48H)")

    async def start_monitoring(self):
        self.running = True
        logger.log_system_event("Strategy Monitoring Started...")
        
        symbols = [
            "XAUUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
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
                
                await asyncio.sleep(10) 
            
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

        h2 = market_data.get('h2', {})
        m30 = market_data.get('m30', {})
        m15 = market_data.get('m15', {})
        
        if not h2 or not m30 or not m15: return None

        # Layer 1: 2H Trend
        is_uptrend = h2.get('close', 0) > h2.get('ema_200', 0) and h2.get('adx', 0) > 20
        is_downtrend = h2.get('close', 0) < h2.get('ema_200', 0) and h2.get('adx', 0) > 20
        if not (is_uptrend or is_downtrend): return None

        m30_close = m30.get('close', 0)
        now = datetime.now(timezone.utc)

        # Layer 2: Pivot Levels (48H)
        pivot_highs = sorted(h2.get('pivot_highs', []))
        pivot_lows = sorted(h2.get('pivot_lows', []), reverse=True)

        nearest_resistance = next((level for level in pivot_highs if level > m30_close), None)
        nearest_support = next((level for level in pivot_lows if level < m30_close), None)

        if is_uptrend and nearest_resistance:
            if m30_close > nearest_resistance:
                self.watchlist[symbol] = {'type': 'BUY', 'level': nearest_resistance, 'time': now}
                logger.log_system_event(f"📝 Valid 30M Breakout UP for {symbol} (Resistance: {nearest_resistance}). Waiting for 15M retest...")
                return None

        elif is_downtrend and nearest_support:
            if m30_close < nearest_support:
                self.watchlist[symbol] = {'type': 'SELL', 'level': nearest_support, 'time': now}
                logger.log_system_event(f"📝 Valid 30M Breakout DOWN for {symbol} (Support: {nearest_support}). Waiting for 15M retest...")
                return None

        # Layer 3: 15M Trigger
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 72: 
                del self.watchlist[symbol]
                return None

            m15_close = m15.get('close', 0)
            m15_high = m15.get('high', 0)
            m15_low = m15.get('low', 0)
            atr_15m = m15.get('atr', 0.0010)
            
            adx_2h = h2.get('adx', 0)
            m15_ema20 = m30.get('ema_20', 0) 
            
            if adx_2h > 30:
                ema_filter_ok = True 
            else:
                ema_filter_ok = abs(m15_close - m15_ema20) < (atr_15m * 1.5)

            tolerance = atr_15m * 0.3
            level = watch['level']

            volume_ok = m15.get('volume', 0) > (m15.get('volume_avg_20', 1) * 1.3)

            if watch['type'] == 'BUY' and is_uptrend:
                if m15_low <= (level + tolerance) and m15_close > level and ema_filter_ok and volume_ok:
                    signal_type = "BUY"
                    del self.watchlist[symbol]
                    
                    stop_loss = min(m15_low, level) - (atr_15m * 0.5) 
                    take_profit = m15_close + ((m15_close - stop_loss) * 1.5)
                else: return None

            elif watch['type'] == 'SELL' and is_downtrend:
                if m15_high >= (level - tolerance) and m15_close < level and ema_filter_ok and volume_ok:
                    signal_type = "SELL"
                    del self.watchlist[symbol]
                    
                    stop_loss = max(m15_high, level) + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 1.5)
                else: return None
            else: return None
        else: return None

        # Finalize Signal
        if signal_type:
            if symbol != "XAUUSDT":
                if self.last_crypto_signal_time:
                    c_time_diff = (now - self.last_crypto_signal_time).total_seconds() / 3600
                    if c_time_diff < 4: return None 
            
            if signal_type == "BUY":
                rr = (take_profit - m15_close) / (m15_close - stop_loss) if (m15_close - stop_loss) > 0 else 0
            else:
                rr = (m15_close - take_profit) / (stop_loss - m15_close) if (stop_loss - m15_close) > 0 else 0

            if rr < 1.5: return None

            if symbol != "XAUUSDT": self.last_crypto_signal_time = now
            self.signals_today += 1
            
            signal = {
                'symbol': symbol, 'type': signal_type, 'entry_price': m15_close, 
                'stop_loss': round(stop_loss, 4), 'take_profit_1': round(take_profit, 4), 
                'take_profit_2': 0, 'strategy': 'Golden_V6'
            }
            logger.log_system_event(f"🏆 GOLDEN V6 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss}")
            return signal
        return None
