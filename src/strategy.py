    def _find_levels(self, highs, lows, closes, current_price, is_uptrend):
        """پیدا کردن نزدیک‌ترین کف‌ها (در روند نزولی) یا سقف‌ها (در روند صعودی)"""
        
        # 🟢 شرط ایمنی: اگر تعداد کندل‌ها کافی نبود، هیچ سطحی برنگردان
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
        
        if not levels: return None, None
        
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
