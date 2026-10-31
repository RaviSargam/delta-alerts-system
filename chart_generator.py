"""
Lightweight in-memory candlestick chart generator for Delta Exchange breakout alerts.
Renders dark-themed charts (< 50 KB) with EMA, broken boundaries, and volume for Telegram.
"""

import io
import logging
from typing import List, Dict, Any, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches

logger = logging.getLogger("DeltaAlerts.Chart")


def generate_breakout_chart(
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    boundary_level: float,
    direction: str,
    current_price: float,
    currency_symbol: str = "$",
    num_candles: int = 24,
) -> Optional[io.BytesIO]:
    """
    Renders an in-memory dark-themed candlestick chart highlighting the breakout boundary level.
    Returns a BytesIO stream containing the PNG image.
    """
    if not candles or len(candles) < 3:
        return None

    try:
        # Take the most recent completed candles
        display_candles = candles[-num_candles:]
        n = len(display_candles)

        fig, (ax_main, ax_vol) = plt.subplots(
            2, 1, figsize=(8, 4.5), dpi=100, gridspec_kw={"height_ratios": [3.2, 1.0]}
        )

        bg_color = "#131722"
        panel_color = "#1E222D"
        grid_color = "#2A2E39"
        text_color = "#D1D4DC"
        up_color = "#089981"
        down_color = "#F23645"

        fig.patch.set_facecolor(bg_color)
        ax_main.set_facecolor(bg_color)
        ax_vol.set_facecolor(bg_color)

        closes = []
        volumes = []

        # Draw candlesticks
        for i, c in enumerate(display_candles):
            open_p = float(c.get("open", 0))
            close_p = float(c.get("close", 0))
            high_p = float(c.get("high", 0))
            low_p = float(c.get("low", 0))
            vol = float(c.get("volume", 0))

            closes.append(close_p)
            volumes.append(vol)

            is_up = close_p >= open_p
            candle_color = up_color if is_up else down_color

            # Wick
            ax_main.vlines(i, low_p, high_p, color=candle_color, linewidth=1.2, zorder=2)

            # Candle Body
            height = abs(close_p - open_p)
            bottom = min(open_p, close_p)
            # Ensure minimum height for doji candles
            if height == 0:
                height = (high_p - low_p) * 0.05 or 0.01

            rect = patches.Rectangle(
                (i - 0.35, bottom), 0.7, height, facecolor=candle_color, edgecolor=candle_color, zorder=3
            )
            ax_main.add_patch(rect)

            # Volume bar
            vol_rect = patches.Rectangle(
                (i - 0.35, 0), 0.7, vol, facecolor=candle_color, alpha=0.6, edgecolor="none", zorder=2
            )
            ax_vol.add_patch(vol_rect)

        # 20 EMA calculation for chart overlay
        if len(closes) >= 5:
            ema_values = []
            multiplier = 2 / (20 + 1)
            ema = closes[0]
            for p in closes:
                ema = (p - ema) * multiplier + ema
                ema_values.append(ema)
            ax_main.plot(range(n), ema_values, color="#2962FF", linewidth=1.5, label="20 EMA", zorder=4)

        # Highlight Broken Boundary Line
        is_bullish = direction.upper() in ["HIGH", "BULLISH"]
        boundary_color = "#00E676" if is_bullish else "#FF5252"
        boundary_label = f"Broken High ({currency_symbol}{boundary_level:,.2f})" if is_bullish else f"Broken Low ({currency_symbol}{boundary_level:,.2f})"
        ax_main.axhline(boundary_level, color=boundary_color, linestyle="--", linewidth=1.4, label=boundary_label, zorder=5)

        # Mark Current Price
        ax_main.axhline(current_price, color="#FFEB3B", linestyle=":", linewidth=1.0, alpha=0.8, label=f"Live: {currency_symbol}{current_price:,.2f}")

        # Formatting Main Axis
        title_tag = "[BULLISH BREAKOUT]" if is_bullish else "[BEARISH BREAKOUT]"
        ax_main.set_title(
            f"{title_tag} #{symbol} ({timeframe}) | Live: {currency_symbol}{current_price:,.2f}",
            color=text_color,
            fontsize=11,
            fontweight="bold",
            pad=8,
        )

        ax_main.tick_params(colors="#787B86", labelsize=8)
        ax_main.grid(True, color=grid_color, linestyle=":", alpha=0.5)
        ax_main.set_xlim(-1, n)
        ax_main.set_xticklabels([])  # Hide x labels on main chart

        legend = ax_main.legend(
            loc="upper left",
            facecolor=panel_color,
            edgecolor=grid_color,
            labelcolor=text_color,
            fontsize=8,
            framealpha=0.9,
        )

        # Formatting Volume Axis
        ax_vol.tick_params(colors="#787B86", labelsize=7)
        ax_vol.grid(True, color=grid_color, linestyle=":", alpha=0.4)
        ax_vol.set_xlim(-1, n)
        ax_vol.set_ylabel("Volume", color="#787B86", fontsize=7)
        ax_vol.yaxis.get_offset_text().set_color("#787B86")

        plt.subplots_adjust(left=0.08, right=0.95, top=0.90, bottom=0.08, hspace=0.08)

        buf = io.BytesIO()
        plt.savefig(buf, format="png", facecolor=fig.get_facecolor(), edgecolor="none", dpi=100)
        plt.close(fig)
        buf.seek(0)
        return buf

    except Exception as e:
        logger.error(f"Error generating chart snapshot for {symbol} {timeframe}: {e}")
        return None
