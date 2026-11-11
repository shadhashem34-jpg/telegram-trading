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
        logger.log_system_event("Strategy Initialized (Golden V12.2 - 24/7 Liquidity Sweep)")

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
                
                # افزایش زمان استراحت به 30 ثانیه
                await asyncio.sleep(30) 
            
            await asyncio.sleep(60)

    async def stop(self):
        self.running = False
        logger.log_system_event("StrategyManager stopped")

    def is_trading_session_active(self) -> bool:
        # 🟢 محدودیت زمانی کاملاً برداشته شد. ربات 24 ساعته کار می‌کند.
        return True

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: return None
        if not self.is_trading_session_active(): return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m15 = market_data.get('m15', {})
        
        if not h4 or not h1 or not m15: return None

        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0)
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0)
        
        swing_highs = sorted(h1.get('swing_highs', []))
        swing_lows = sorted(h1.get('swing_lows', []), reverse=True)
        
        now = datetime.now(timezone.utc)
        m15_close = m15.get('close', 0)
        m15_high = m15.get('high', 0)
        m15_low = m15.get('low', 0)
        m15_open = m15.get('open', 0)
        atr_15m = m15.get('atr', 0.0010)

        if is_downtrend:
            valid_liquidity_highs = [sh for sh in swing_highs if sh > m15_close]
            if valid_liquidity_highs:
                closest_level = min(valid_liquidity_highs)
                # لاگ تشخیصی برای اینکه ببینیم ربات چه سطحی را زیر نظر دارد
                logger.log_system_event(f"🔍 [V12.2] {symbol} Down. Nearest Liq: {closest_level:.2f} | 15M C:{m15_close:.2f} H:{m15_high:.2f}")
                
                for lvl in valid_liquidity_highs:
                    if m15_high > lvl and m15_close < lvl and m15_close < m15_open:
                        self.watchlist[symbol] = {'type': 'SELL', 'level': lvl, 'time': now}
                        logger.log_system_event(f"📝 [V12.2] Liquidity Sweep UP for {symbol} at {lvl}. Waiting for confirmation...")
                        return None

        elif is_uptrend:
            valid_liquidity_lows = [sl for sl in swing_lows if sl < m15_close]
            if valid_liquidity_lows:
                closest_level = max(valid_liquidity_lows)
                logger.log_system_event(f"🔍 [V12.2] {symbol} Up. Nearest Liq: {closest_level:.2f} | 15M C:{m15_close:.2f} L:{m15_low:.2f}")

                for lvl in valid_liquidity_lows:
                    if m15_low < lvl and m15_close > lvl and m15_close > m15_open:
                        self.watchlist[symbol] = {'type': 'BUY', 'level': lvl, 'time': now}
                        logger.log_system_event(f"📝 [V12.2] Liquidity Sweep DOWN for {symbol} at {lvl}. Waiting for confirmation...")
                        return None

        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 4:
                del self.watchlist[symbol]
                return None

            level = watch['level']
            vol_ok = m15.get('volume', 0) < (m15.get('volume_avg_20', 1) * 1.5)

            if watch['type'] == 'SELL' and is_downtrend:
                if m15_high >= level and m15_close < level and m15_close < m15_open and vol_ok:
                    signal_type = "SELL"
                    del self.watchlist[symbol]
                    stop_loss = m15_high + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                else: return None

            elif watch['type'] == 'BUY' and is_uptrend:
                if m15_low <= level and m15_close > level and m15_close > m15_open and vol_ok:
                    signal_type = "BUY"
                    del self.watchlist[symbol]
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                else: return None
            else: return None
        else: return None

        if signal_type:
            if symbol != "XAUUSDT" and self.last_crypto_signal_time:
                if (now - self.last_crypto_signal_time).total_seconds() / 3600 < 4: return None 
            
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
                'take_profit_2': 0, 'strategy': 'Golden_V12.2'
            }
            logger.log_system_event(f"🏆 GOLDEN V12.2 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
