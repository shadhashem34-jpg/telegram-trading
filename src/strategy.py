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
        logger.log_system_event("Strategy Initialized (Golden V26 - Dynamic Levels)")

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

    def _find_nearest_levels(self, highs, lows, closes, current_price, atr):
        """
        پیدا کردن تمام سقف و کف‌های واضح (Swing Points) و انتخاب نزدیک‌ترین‌ها
        """
        swing_points = []
        
        # پیدا کردن پیوت‌ها (۲ کندل چپ و راست)
        for i in range(2, len(highs) - 2):
            # سقف واضح
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_points.append({'type': 'high', 'level': closes[i]}) # استفاده از بدنه
            # کف واضح
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_points.append({'type': 'low', 'level': closes[i]})

        if not swing_points:
            return None, None

        # ضخامت زون خیلی باریک (0.1 ATR)
        buffer = atr * 0.1

        # پیدا کردن نزدیک‌ترین مقاومت (هر سطحی که بالای قیمت فعلی است - شامل کف‌های شکسته شده)
        res_candidates = [p for p in swing_points if p['level'] > current_price]
        nearest_res = None
        if res_candidates:
            nearest_res = min(res_candidates, key=lambda x: x['level'])
            nearest_res['zone_bottom'] = nearest_res['level']
            nearest_res['zone_top'] = nearest_res['level'] + buffer
        
        # پیدا کردن نزدیک‌ترین حمایت (هر سطحی که زیر قیمت فعلی است)
        sup_candidates = [p for p in swing_points if p['level'] < current_price]
        nearest_sup = None
        if sup_candidates:
            nearest_sup = max(sup_candidates, key=lambda x: x['level'])
            nearest_sup['zone_top'] = nearest_sup['level']
            nearest_sup['zone_bottom'] = nearest_sup['level'] - buffer
            
        return nearest_res, nearest_sup

    async def generate_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        if self.signals_today >= self.max_daily_signals: return None

        h4 = market_data.get('h4', {})
        h1 = market_data.get('h1', {})
        m15 = market_data.get('m15', {})
        if not h4 or not h1 or not m15: return None

        is_uptrend = h4.get('close', 0) > h4.get('ema_200', 0)
        is_downtrend = h4.get('close', 0) < h4.get('ema_200', 0)
        
        now = datetime.now(timezone.utc)
        m15_close = m15.get('close', 0)
        m15_high = m15.get('high', 0)
        m15_low = m15.get('low', 0)
        m15_open = m15.get('open', 0)
        h1_atr = h1.get('atr', 0.0010) # استفاده از ATR 1H برای زون‌ها

        # پیدا کردن سطوح از تایم ۱ ساعته (برای دقت بیشتر در ترید روزانه)
        nearest_res, nearest_sup = self._find_nearest_levels(
            h1.get('highs', []), h1.get('lows', []), h1.get('closes', []), m15_close, h1_atr
        )

        # لاگ‌گیری دقیق (نمایش نزدیک‌ترین سطح در هر لحظه)
        res_str = f"{nearest_res['level']:.2f} (Zone: {nearest_res['zone_bottom']:.2f}-{nearest_res['zone_top']:.2f})" if nearest_res else "None"
        sup_str = f"{nearest_sup['level']:.2f} (Zone: {nearest_sup['zone_bottom']:.2f}-{nearest_sup['zone_top']:.2f})" if nearest_sup else "None"
        
        logger.log_system_event(f"📡 [V26 RADAR] {symbol} | Price: {m15_close:.2f}")
        logger.log_system_event(f"   ↳ Nearest Res: {res_str}")
        logger.log_system_event(f"   ↳ Nearest Sup: {sup_str}")

        # 🎯 منطق ورود (ترکیب سویپ و پولبک روی نزدیک‌ترین سطح)
        signal_type = None
        
        if is_downtrend and nearest_res:
            # اگر قیمت به زون مقاومت رسید و کندل 15 دقیقه‌ای سایه بالا زد و قرمز بسته شد
            if m15_high >= nearest_res['zone_bottom'] and m15_close < nearest_res['zone_bottom'] and m15_close < m15_open:
                signal_type = "SELL"
                stop_loss = m15_high + (m15.get('atr', 0.0010) * 0.5)
                take_profit = m15_close - ((stop_loss - m15_close) * 2.0)

        elif is_uptrend and nearest_sup:
            # اگر قیمت به زون حمایت رسید و کندل 15 دقیقه‌ای سایه پایین زد و سبز بسته شد
            if m15_low <= nearest_sup['zone_top'] and m15_close > nearest_sup['zone_top'] and m15_close > m15_open:
                signal_type = "BUY"
                stop_loss = m15_low - (m15.get('atr', 0.0010) * 0.5)
                take_profit = m15_close + ((m15_close - stop_loss) * 2.0)

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
                'strategy': 'Golden_V26'
            }
            logger.log_system_event(f"🏆 GOLDEN V26 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
