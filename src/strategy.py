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
        logger.log_system_event("Strategy Initialized (Golden V7.1 - Zones & Sweeps)")

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

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m15 = market_data.get('m15', {})
        
        if not h4 or not h1 or not m15: return None

        # 🛡️ لایه اول: فیلتر روند ۴ ساعته (مکرو)
        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0) and h4.get('adx', 0) > 20
        if not (is_uptrend or is_downtrend): return None

        h1_close = h1.get('close', 0)
        now = datetime.now(timezone.utc)

        # ⚡ لایه دوم: پیدا کردن رنج‌های ۱ ساعته و شکست آن‌ها
        pivot_highs = sorted(h1.get('pivot_highs', []))
        pivot_lows = sorted(h1.get('pivot_lows', []), reverse=True)

        # پیدا کردن نزدیک‌ترین سطحی که قیمت به تازگی آن را به سمت بالا شکسته است
        broken_resistances = [level for level in pivot_highs if level < h1_close]
        broken_supports = [level for level in pivot_lows if level > h1_close]

        if is_uptrend and broken_resistances:
            level = max(broken_resistances) 
            if symbol not in self.watchlist or self.watchlist[symbol]['level'] != level:
                self.watchlist[symbol] = {'type': 'BUY', 'level': level, 'time': now}
                logger.log_system_event(f"📝 Valid 1H Breakout UP for {symbol} (Level: {level}). Waiting for 15M retest zone...")
                return None

        elif is_downtrend and broken_supports:
            level = min(broken_supports) 
            if symbol not in self.watchlist or self.watchlist[symbol]['level'] != level:
                self.watchlist[symbol] = {'type': 'SELL', 'level': level, 'time': now}
                logger.log_system_event(f"📝 Valid 1H Breakout DOWN for {symbol} (Level: {level}). Waiting for 15M retest zone...")
                return None

        # 🎯 لایه سوم: ماشه در تایم ۱۵ دقیقه (ریتست ناحیه‌ای + شکار استاپ)
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            # انقضای ستاپ بعد از ۱۲ ساعت (چون رنج ۱ ساعته دیگه اعتبار نداره)
            if time_diff > 12: 
                del self.watchlist[symbol]
                return None

            m15_close = m15.get('close', 0)
            m15_high = m15.get('high', 0)
            m15_low = m15.get('low', 0)
            atr_15m = m15.get('atr', 0.0010)
            
            # فیلتر حجم (۱.۳ برابر میانگین ۲۰ کندل)
            volume_ok = m15.get('volume', 0) > (m15.get('volume_avg_20', 1) * 1.3)
            
            # تعریف ناحیه (Zone) با ضخامت ۰.۵ ATR
            zone_buffer = atr_15m * 0.5
            zone_top = watch['level'] + zone_buffer
            zone_bottom = watch['level'] - zone_buffer
            level = watch['level']

            if watch['type'] == 'BUY' and is_uptrend:
                # ۱. سایه پایین باید وارد ناحیه شده باشد (m15_low <= zone_top)
                # ۲. بدنه باید بالای خط اصلی بسته شده باشد (m15_close > level)
                # ۳. حجم تاییدیه داشته باشد
                if m15_low <= zone_top and m15_close > level and volume_ok:
                    signal_type = "BUY"
                    del self.watchlist[symbol]
                    
                    stop_loss = min(m15_low, zone_bottom) - (atr_15m * 0.5) 
                    
                    rr_tp = m15_close + ((m15_close - stop_loss) * 1.5)
                    h4_pivot_highs = sorted([p for p in h4.get('pivot_highs', []) if p > m15_close])
                    
                    if h4_pivot_highs and (h4_pivot_highs[0] - m15_close) < (rr_tp - m15_close):
                        take_profit = h4_pivot_highs[0]
                    else:
                        take_profit = rr_tp
                else: return None

            elif watch['type'] == 'SELL' and is_downtrend:
                # ۱. سایه بالا باید وارد ناحیه شده باشد (m15_high >= zone_bottom)
                # ۲. بدنه باید زیر خط اصلی بسته شده باشد (m15_close < level)
                # ۳. حجم تاییدیه داشته باشد
                if m15_high >= zone_bottom and m15_close < level and volume_ok:
                    signal_type = "SELL"
                    del self.watchlist[symbol]
                    
                    stop_loss = max(m15_high, zone_top) + (atr_15m * 0.5)
                    
                    rr_tp = m15_close - ((stop_loss - m15_close) * 1.5)
                    h4_pivot_lows = sorted([p for p in h4.get('pivot_lows', []) if p < m15_close], reverse=True)
                    
                    if h4_pivot_lows and (m15_close - h4_pivot_lows[0]) < (m15_close - rr_tp):
                        take_profit = h4_pivot_lows[0]
                    else:
                        take_profit = rr_tp
                else: return None
            else: return None
        else: return None

        # 🏆 نهایی‌سازی سیگنال و مدیریت ریسک
        if signal_type:
            if symbol != "XAUUSDT":
                if self.last_crypto_signal_time:
                    c_time_diff = (now - self.last_crypto_signal_time).total_seconds() / 3600
                    if c_time_diff < 4: return None 
            
            # بررسی RR (حداقل 1.5)
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
                'take_profit_2': 0, 'strategy': 'Golden_V7.1'
            }
            logger.log_system_event(f"🏆 GOLDEN V7.1 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
