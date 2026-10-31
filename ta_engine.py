"""
Technical Analysis Engine for Delta Exchange India Breakout Alerts System.
Calculates RSI, EMA, Volume Ratio, ATR (Average True Range), Consolidation Squeeze,
Automated Stop-Loss/Take-Profit targets, and Setup Quality Scores (0-100 Rating).
"""

from typing import List, Dict, Any, Optional, Tuple


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """Calculates Relative Strength Index (RSI) for a price series."""
    if len(prices) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calculate_ema(prices: List[float], period: int = 20) -> Optional[float]:
    """Calculates Exponential Moving Average (EMA) for a price series."""
    if len(prices) < period:
        return None

    multiplier = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period  # Initialize with SMA

    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


def calculate_volume_ratio(volumes: List[float], period: int = 20) -> Optional[float]:
    """
    Calculates current volume relative to 20-period Volume Simple Moving Average (SMA).
    Returns ratio (e.g. 1.8 = 1.8x average volume).
    """
    if len(volumes) < period + 1:
        return None

    current_vol = volumes[-1]
    hist_volumes = volumes[-period - 1 : -1]
    avg_vol = sum(hist_volumes) / len(hist_volumes)

    if avg_vol <= 0:
        return 1.0

    return current_vol / avg_vol


def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    """Calculates Average True Range (ATR) to measure volatility expansion."""
    if not highs or not lows or not closes:
        return None

    min_len = min(len(highs), len(lows), len(closes))
    if min_len < period + 1:
        return None

    tr_list = []
    for i in range(1, min_len):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        tr_list.append(tr)

    if len(tr_list) < period:
        return None

    atr = sum(tr_list[:period]) / period
    for i in range(period, len(tr_list)):
        atr = (atr * (period - 1) + tr_list[i]) / period

    return atr


def detect_consolidation_squeeze(highs: List[float], lows: List[float], period: int = 6) -> Tuple[bool, str]:
    """
    Detects if price was in a tight consolidation squeeze prior to breakout.
    """
    if not highs or not lows or len(highs) < period or len(lows) < period:
        return False, "N/A"

    recent_highs = highs[-period:]
    recent_lows = lows[-period:]

    if not recent_highs or not recent_lows:
        return False, "N/A"

    range_max = max(recent_highs)
    range_min = min(recent_lows)

    if range_min <= 0:
        return False, "N/A"

    range_spread_pct = ((range_max - range_min) / range_min) * 100.0

    if range_spread_pct <= 2.5:
        return True, f"Tight Squeeze ({period} Candles Compressed - {range_spread_pct:.1f}% Range) 🎯"
    elif range_spread_pct <= 4.0:
        return True, f"Moderate Consolidation ({period} Candles - {range_spread_pct:.1f}% Range)"
    else:
        return False, f"Normal Range ({range_spread_pct:.1f}% Range)"


def calculate_trade_targets(
    direction: str,
    current_price: float,
    boundary_level: float,
    atr: Optional[float],
    currency_symbol: str = "$",
    rr1: float = 1.5,
    rr2: float = 3.0,
) -> Dict[str, Any]:
    """
    Calculates entry, stop loss, and take profit targets based on customizable Risk-to-Reward ratios.
    """
    # Fallback risk buffer if ATR is not available (1.0% buffer)
    risk_buffer = atr * 0.8 if (atr and atr > 0) else current_price * 0.01

    if direction.upper() in ["HIGH", "BULLISH"]:
        sl_price = max(boundary_level - risk_buffer, current_price * 0.95)
        risk = max(current_price - sl_price, current_price * 0.005)
        tp1 = current_price + (rr1 * risk)
        tp2 = current_price + (rr2 * risk)

        sl_pct = ((current_price - sl_price) / current_price) * 100.0
        tp1_pct = ((tp1 - current_price) / current_price) * 100.0
        tp2_pct = ((tp2 - current_price) / current_price) * 100.0

        return {
            "entry": current_price,
            "sl": sl_price,
            "sl_pct": sl_pct,
            "tp1": tp1,
            "tp1_pct": tp1_pct,
            "tp2": tp2,
            "tp2_pct": tp2_pct,
            "formatted": (
                f"🎯 <b>TRADE PLAN & TARGETS</b>\n"
                f"• <b>Entry:</b> {currency_symbol}{current_price:,.2f}\n"
                f"• <b>Stop Loss:</b> {currency_symbol}{sl_price:,.2f} (-{sl_pct:.2f}%)\n"
                f"• <b>Target 1 (1:{rr1:.1f} RR):</b> {currency_symbol}{tp1:,.2f} (+{tp1_pct:.2f}%)\n"
                f"• <b>Target 2 (1:{rr2:.1f} RR):</b> {currency_symbol}{tp2:,.2f} (+{tp2_pct:.2f}%)"
            ),
        }
    else:  # Bearish
        sl_price = min(boundary_level + risk_buffer, current_price * 1.05)
        risk = max(sl_price - current_price, current_price * 0.005)
        tp1 = current_price - (rr1 * risk)
        tp2 = current_price - (rr2 * risk)

        sl_pct = ((sl_price - current_price) / current_price) * 100.0
        tp1_pct = ((current_price - tp1) / current_price) * 100.0
        tp2_pct = ((current_price - tp2) / current_price) * 100.0

        return {
            "entry": current_price,
            "sl": sl_price,
            "sl_pct": sl_pct,
            "tp1": tp1,
            "tp1_pct": tp1_pct,
            "tp2": tp2,
            "tp2_pct": tp2_pct,
            "formatted": (
                f"🎯 <b>TRADE PLAN & TARGETS</b>\n"
                f"• <b>Entry:</b> {currency_symbol}{current_price:,.2f}\n"
                f"• <b>Stop Loss:</b> {currency_symbol}{sl_price:,.2f} (+{sl_pct:.2f}%)\n"
                f"• <b>Target 1 (1:{rr1:.1f} RR):</b> {currency_symbol}{tp1:,.2f} (-{tp1_pct:.2f}%)\n"
                f"• <b>Target 2 (1:{rr2:.1f} RR):</b> {currency_symbol}{tp2:,.2f} (-{tp2_pct:.2f}%)"
            ),
        }


def calculate_trade_setup_score(
    direction: str,
    current_price: float,
    boundary_level: float,
    rsi: Optional[float],
    ema20: Optional[float],
    vol_ratio: Optional[float],
    atr: Optional[float],
    is_squeeze: bool,
    is_confluent: bool,
) -> Tuple[int, str]:
    """
    Computes a 0-100 Confluence Score and Setup Grade.
    Base score: 50
    +15: Trend Alignment (EMA 20)
    +15: RSI Momentum Alignment
    +10: Volume Ratio >= 1.2x
    +10: ATR Volatility Expansion
    +10: Consolidation Squeeze
    +10: Multi-Timeframe Confluence
    """
    score = 50

    is_bullish = direction.upper() in ["HIGH", "BULLISH"]

    # 1. Trend Alignment
    if ema20:
        if (is_bullish and current_price >= ema20) or (not is_bullish and current_price <= ema20):
            score += 15

    # 2. RSI Momentum
    if rsi:
        if is_bullish and (55 <= rsi <= 75):
            score += 15
        elif not is_bullish and (25 <= rsi <= 45):
            score += 15
        elif (is_bullish and rsi > 75) or (not is_bullish and rsi < 25):
            score += 5  # Strong but slightly overextended

    # 3. Volume Confirmation
    if vol_ratio and vol_ratio >= 1.2:
        score += 10
    elif vol_ratio and vol_ratio >= 1.0:
        score += 5

    # 4. ATR Volatility Expansion
    if atr and atr > 0:
        breakout_dist = abs(current_price - boundary_level)
        if breakout_dist >= (0.5 * atr):
            score += 10

    # 5. Consolidation Squeeze
    if is_squeeze:
        score += 10

    # 6. Multi-Timeframe Confluence
    if is_confluent:
        score += 10

    score = min(100, score)

    if score >= 88:
        grade = f"🌟 Setup Score: {score}/100 (A+ Grade Institutional Quality Setup)"
    elif score >= 75:
        grade = f"🟢 Setup Score: {score}/100 (A Grade High Probability Setup)"
    elif score >= 60:
        grade = f"🟡 Setup Score: {score}/100 (B Grade Moderate Setup)"
    else:
        grade = f"🔴 Setup Score: {score}/100 (C Grade Weak Setup)"

    return score, grade


def get_rsi_status(rsi: Optional[float]) -> str:
    """Returns human-readable RSI momentum status."""
    if rsi is None:
        return "N/A"
    if rsi >= 70:
        return f"{rsi:.1f} (Overbought / Strong Bullish)"
    elif rsi >= 55:
        return f"{rsi:.1f} (Bullish Momentum)"
    elif rsi <= 30:
        return f"{rsi:.1f} (Oversold / Strong Bearish)"
    elif rsi <= 45:
        return f"{rsi:.1f} (Bearish Momentum)"
    else:
        return f"{rsi:.1f} (Neutral)"


def get_trend_status(current_price: float, ema20: Optional[float]) -> str:
    """Returns human-readable trend status based on EMA 20."""
    if ema20 is None:
        return "N/A"
    if current_price >= ema20:
        return f"🟢 Bullish (Price > 20 EMA ${ema20:,.2f})"
    else:
        return f"🔻 Bearish (Price &lt; 20 EMA ${ema20:,.2f})"


def format_oi_metrics(oi_data: Optional[Dict[str, Any]], direction: str) -> str:
    """Formats Open Interest and Funding Rate data for Telegram alerts."""
    if not oi_data:
        return "N/A"

    change_pct = oi_data.get("oi_change_pct_6h", 0.0)
    funding_rate = oi_data.get("funding_rate_pct", 0.0)

    dir_upper = direction.upper()
    is_bullish = dir_upper in ["HIGH", "BULLISH"]

    # Sentiment Tag
    if is_bullish:
        if change_pct >= 2.0:
            sentiment = f"↗️ +{change_pct:.1f}% (Institutional Inflow 🔥)"
        elif change_pct <= -2.0:
            sentiment = f"↘️ {change_pct:.1f}% (Short Squeeze Trap ⚠️)"
        else:
            sentiment = f"↔️ {change_pct:+.1f}% (Stable)"
    else:
        if change_pct >= 2.0:
            sentiment = f"↗️ +{change_pct:.1f}% (Aggressive Short Build-up 💥)"
        elif change_pct <= -2.0:
            sentiment = f"↘️ {change_pct:.1f}% (Long Liquidation Dump ⚠️)"
        else:
            sentiment = f"↔️ {change_pct:+.1f}% (Stable)"

    funding_str = f" | Funding: {funding_rate:+.4f}%" if funding_rate != 0 else ""
    return f"{sentiment}{funding_str}"

