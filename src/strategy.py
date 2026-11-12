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
        logger.log_system_event("Strategy Initialized (Golden V24 - Body Zones)")

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

    def _get_body_zones(self, highs, lows, closes, atr):
        """پیدا کردن پیوت‌ها و ساخت زون بر اساس بدنه به سمت سایه"""
        res_zones = []
        sup_zones = []
        buffer = atr * 0.5
        
        for i in range(2, len(highs) - 2):
            # پیوت سقف
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                base_close = closes[i]
                # زون مقاومت: از بدنه به سمت بالا (سایه)
                res_zones.append({'base': base_close, 'bottom': base_close, 'top': base_close + buffer})
                
            # پیوت کف
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                base_close = closes[i]
                # زون حمایت: از بدنه به سمت پایین (سایه)
                sup_zones.append({'base': base_close, 'top': base_close, 'bottom': base_close - buffer})
                
        return res_zones, sup_zones

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
        atr_15m = m15.get('atr', 0.0010)

        # پیدا کردن زون‌ها (۴ ساعته و ۱ ساعته)
        h4_res, h4_sup = self._get_body_zones(h4.get('highs', []), h4.get('lows', []), h4.get('closes', []), atr_15m)
        h1_res, h1_sup = self._get_body_zones(h1.get('highs', []), h1.get('lows', []), h1.get('closes', []), atr_15m)

        # انتخاب نزدیک‌ترین زون‌ها
        valid_h4_res = [z for z in h4_res if z['base'] > m15_close]
        valid_h4_sup = [z for z in h4_sup if z['base'] < m15_close]
        nearest_h4_res = min(valid_h4_res, key=lambda x: x['base']) if valid_h4_res else None
        nearest_h4_sup = max(valid_h4_sup, key=lambda x: x['base']) if valid_h4_sup else None

        valid_h1_res = [z for z in h1_res if z['base'] > m15_close]
        valid_h1_sup = [z for z in h1_sup if z['base'] < m15_close]
        nearest_h1_res = min(valid_h1_res, key=lambda x: x['base']) if valid_h1_res else None
        nearest_h1_sup = max(valid_h1_sup, key=lambda x: x['base']) if valid_h1_sup else None

        # لاگ‌گیری
        logger.log_system_event(f"📡 [V24 RADAR] {symbol} | Price: {m15_close:.2f}")
        if nearest_h4_res: logger.log_system_event(f"   ↳ 4H Res: {nearest_h4_res['bottom']:.2f} - {nearest_h4_res['top']:.2f} (Body: {nearest_h4_res['base']:.2f})")
        if nearest_h4_sup: logger.log_system_event(f"   ↳ 4H Sup: {nearest_h4_sup['bottom']:.2f} - {nearest_h4_sup['top']:.2f} (Body: {nearest_h4_sup['base']:.2f})")
        if nearest_h1_res: logger.log_system_event(f"   ↳ 1H Res: {nearest_h1_res['bottom']:.2f} - {nearest_h1_res['top']:.2f} (Body: {nearest_h1_res['base']:.2f})")
        if nearest_h1_sup: logger.log_system_event(f"   ↳ 1H Sup: {nearest_h1_sup['bottom']:.2f} - {nearest_h1_sup['top']:.2f} (Body: {nearest_h1_sup['base']:.2f})")

        # تشخیص شکست با بدنه ۱ ساعته
        h1_close = h1.get('close', 0)
        
        if is_downtrend:
            if nearest_h4_sup and h1_close < nearest_h4_sup['bottom'] and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'SELL_RETEST', 'zone': nearest_h4_sup, 'time': now, 'tf': '4H'}
                logger.log_system_event(f"📝 [V24] 4H Zone Breakdown below {nearest_h4_sup['bottom']:.2f}. LOCKED.")
            elif nearest_h1_sup and h1_close < nearest_h1_sup['bottom'] and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'SELL_RETEST', 'zone': nearest_h1_sup, 'time': now, 'tf': '1H'}
                logger.log_system_event(f"📝 [V24] 1H Zone Breakdown below {nearest_h1_sup['bottom']:.2f}. LOCKED.")

        elif is_uptrend:
            if nearest_h4_res and h1_close > nearest_h4_res['top'] and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'BUY_RETEST', 'zone': nearest_h4_res, 'time': now, 'tf': '4H'}
                logger.log_system_event(f"📝 [V24] 4H Zone Breakout above {nearest_h4_res['top']:.2f}. LOCKED.")
            elif nearest_h1_res and h1_close > nearest_h1_res['top'] and symbol not in self.watchlist:
                self.watchlist[symbol] = {'type': 'BUY_RETEST', 'zone': nearest_h1_res, 'time': now, 'tf': '1H'}
                logger.log_system_event(f"📝 [V24] 1H Zone Breakout above {nearest_h1_res['top']:.2f}. LOCKED.")

        # شکار پولبک
        if symbol in self.watchlist:
            watch = self.watchlist[symbol]
            time_diff = (now - watch['time']).total_seconds() / 3600
            
            if time_diff > 8: 
                del self.watchlist[symbol]
                return None

            zone = watch['zone']
            signal_type = None

            if watch['type'] == 'SELL_RETEST' and is_downtrend:
                # سایه وارد زون بشه (بالای بدنه)، اما بدنه زیر بدنه بسته بشه
                if m15_high >= zone['base'] and m15_close < zone['base'] and m15_close < m15_open:
                    signal_type = "SELL"
                    stop_loss = m15_high + (atr_15m * 0.5)
                    take_profit = m15_close - ((stop_loss - m15_close) * 2.0)
                    del self.watchlist[symbol]

            elif watch['type'] == 'BUY_RETEST' and is_uptrend:
                # سایه وارد زون بشه (زیر بدنه)، اما بدنه بالای بدنه بسته بشه
                if m15_low <= zone['base'] and m15_close > zone['base'] and m15_close > m15_open:
                    signal_type = "BUY"
                    stop_loss = m15_low - (atr_15m * 0.5)
                    take_profit = m15_close + ((m15_close - stop_loss) * 2.0)
                    del self.watchlist[symbol]

        # نهایی‌سازی سیگنال
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
                'strategy': 'Golden_V24'
            }
            logger.log_system_event(f"🏆 GOLDEN V24 ENTRY: {symbol} ({signal_type}) | Entry: {m15_close} | SL: {stop_loss} | TP: {take_profit}")
            return signal
        return None
