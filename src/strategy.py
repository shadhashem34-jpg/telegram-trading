import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timezone, time
from logger import get_trading_logger

logger = get_trading_logger()

class StrategyManager:
    def __init__(self):
        self.running = False
        self.active_strategy = "3-Layer_Strategy"
        self.last_signal_time = None
        self.signals_today = 0
        self.max_daily_signals = 2
        logger.log_system_event("StrategyManager initialized")

    async def start_monitoring(self):
        self.running = True
        while self.running:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            if now.hour == 0 and now.minute == 0:
                self.signals_today = 0

    async def stop(self):
        self.running = False

    def is_trading_session_active(self) -> bool:
        now = datetime.now(timezone.utc)
        current_time = now.time()
        start_time = time(11, 30)
        end_time = time(22, 30)
        if not (start_time <= current_time <= end_time):
            return False
        ny_open_block = time(12, 45) <= current_time <= time(13, 15)
        return not ny_open_block

    def is_high_impact_news(self) -> bool:
        return False

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: return None
        if not self.is_trading_session_active() or self.is_high_impact_news(): return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m15 = market_data.get('m15', {})
        if not h4 or not h1 or not m15: return None

        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        if not (is_uptrend or is_downtrend): return None

        near_zone = h1.get('near_ema_50', False) or h1.get('near_level', False)
        if not near_zone: return None

        volume_ok = m15.get('volume', 0) > m15.get('volume_avg_20', 0)
        breakout_up = m15.get('close', 0) > m15.get('high_5', 0)
        breakout_down = m15.get('close', 0) < m15.get('low_5', 0)

        signal_type = None
        if is_uptrend and breakout_up and volume_ok: signal_type = "BUY"
        elif is_downtrend and breakout_down and volume_ok: signal_type = "SELL"

        if signal_type:
            entry_price = m15.get('close', 1.0000)
            atr_h1 = h1.get('atr', 0.0010) 
            sl_distance = 2.0 * atr_h1
            macro_high = h4.get('high_100', 9999999)
            macro_low = h4.get('low_100', 0)
            
            if signal_type == "BUY":
                if (macro_high - entry_price) < (atr_h1 * 1.0): return None
                stop_loss = entry_price - sl_distance
                tp1 = min(h1.get('high_20', entry_price + (sl_distance * 1.5)), macro_high)
                tp2 = min(h4.get('high_20', entry_price + (sl_distance * 3.0)), macro_high)
                if (tp1 - entry_price) < (sl_distance * 1.5): return None
            else:
                if (entry_price - macro_low) < (atr_h1 * 1.0): return None
                stop_loss = entry_price + sl_distance
                tp1 = max(h1.get('low_20', entry_price - (sl_distance * 1.5)), macro_low)
                tp2 = max(h4.get('low_20', entry_price - (sl_distance * 3.0)), macro_low)
                if (entry_price - tp1) < (sl_distance * 1.5): return None

            signal = {'symbol': symbol, 'type': signal_type, 'entry_price': entry_price, 'stop_loss': stop_loss, 'take_profit_1': tp1, 'take_profit_2': tp2, 'strategy': '3-Layer_Strategy', 'session': 'Custom_Iran_Time', 'timestamp': datetime.now(timezone.utc)}
            self.signals_today += 1
            return signal
        return None
