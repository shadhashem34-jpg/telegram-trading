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
        logger.log_system_event("Strategy Initialized (Golden V30.2 - Full Logging)")

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
                        else:
                            logger.log_system_event(f"❌ [V30.2] {symbol} | Data is None (API issue).")
                except Exception as e:
                    logger.log_system_event(f"Error scanning {symbol}: {e}")
                await asyncio.sleep(30) 
            await asyncio.sleep(60)

    async def stop(self):
        self.running = False
        logger.log_system_event("StrategyManager stopped")

    def _find_levels(self, highs, lows, closes, current_price, is_uptrend):
        if len(highs) < 5 or len(lows) < 5 or len(closes) < 5:
            return None, None

        levels = []
        for i in range(2, len(highs) - 2):
            if is_uptrend:
                if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                    levels.append(closes[i])
            else:
                if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                    levels.append(closes[i])
        
        if not levels: 
            return None, None
        
        if is_uptrend:
            valid_levels = sorted([l for l in levels if l > current_price])
            if len(valid_levels) >= 2:
                return valid_levels[0], valid_levels[1]
            elif len(valid_levels) == 1:
                return valid_levels[0], None
        else:
            valid_levels = sorted([l for l in levels if l < current_price], reverse=True)
            if len(valid_levels) >= 2:
                return valid_levels[0], valid_levels[1]
            elif len(valid_levels) == 1:
                return valid_levels[0], None
                
        return None, None

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: 
            return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m15 = market_data.get('m15', {})
        
        if not h4 or not h1 or not m15: 
            return None

        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0)
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0)
        
        now = datetime.now(timezone.utc)
        m15_close = m15.get('close', 0)
        m15_high = m15.get('high', 0)
        m15_low = m15.get('low', 0)
        m15_open = m15.get('open', 0)
        m15_atr = m15.get('atr', 0.0010)
        h1_close = h1.get('close', 0)

        logger.log_system_event(f"📡 [V30.2 RADAR] {symbol} | Price: {m15_close:.2f}")

        lvl1, lvl2 = self._find_levels(h4.get('highs', []), h4.get('lows', []), h4.get('closes', []), m15_close, is_uptrend)

        if not lvl1: 
            logger.log_system_event(f"   ↳ No valid levels found for {symbol}.")
            return None
        
        if is_downtrend:
            logger.log_system_event(f"   ↳ Nearest Sup (1): {lvl1:.2f} | Next Sup (2): {lvl2 if lvl2 else 'None'}")
        else:
            logger.log_system_event(f"   ↳ Nearest Res (1): {lvl1:.2f} | Next Res (2): {lvl2 if lvl2 else 'None'}")

        signal_type = None
        stop_loss = 0
        take_profit = 0

        if symbol not in self.watchlist:
            if is_downtrend:
                if lvl2 and m15_high >= lvl2 and m15_close < lvl2 and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (m15_atr * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                
                elif h1_close < lvl1:
                    self.watchlist[symbol] = {'type': 'SELL_RETEST', 'level': lvl1, 'time': now}
                    logger.log_system_event(f"📝 [V30.2] 1H Body Breakdown below {lvl1:.2f}. Level LOCKED. Waiting for Retest...")
                    return None

            elif is_uptrend:
                if lvl2 and m15_low <= lvl2 and m15_close > lvl2 and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (m15_atr * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                
                elif h1_close > lvl1:
                    self.watchlist[symbol] = {'type': 'BUY_RETEST', 'level': lvl1, 'time': now}
                    logger.log_system_event(f"📝 [V30.2] 1H Body Breakout above {lvl1:.2f}. Level LOCKED. Waiting for Retest...")
                    return None
        else:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            if time_diff > 8:
                del self.watchlist[symbol]
                return None

            if watch['type'] == 'SELL_RETEST' and is_downtrend:
                if m15_high >= watch['level'] and m15_close < watch['level'] and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (m15_atr * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY_RETEST' and is_uptrend:
                if m15_low <= watch['level'] and m15_close > watch['level'] and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (m15_atr * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

        if signal_type:
            if symbol != "XAUUSDT" and self.last_crypto_signal_time:
                if (now - self.last_crypto_signal_time).total_seconds() / 3600 < 4: 
                    return None 

            if symbol != "XAUUSDT": 
                self.last_crypto_signal_time = now
                
            self.signals_today += 1
            
            signal = {
                'symbol': symbol, 'type': signal_type, 'entry_price': m15_close, 
                'stop_loss': round(stop_loss, 4), 
                'take_profit_1': round(take_profit, 4), 
                'take_profit_2': 0, 
                'take_profit_3': 0,
                'strategy': 'Golden_V30.2'
            }
            logger.log_system_event(f"🏆 GOLDEN V30.2 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        
        return None
