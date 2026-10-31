"""
Main Event Loop & Execution Engine for Delta Exchange India Breakout Alerts System.
Handles REST Sync, Persistent WebSocket Listener, Breakout Detection,
Telegram Dispatcher, Config Hot-Reloading, and System Heartbeats.
"""

import asyncio
import json
import logging
import logging.handlers
import os
import random
import signal
import sys
import time
from typing import Dict, List, Optional, Set

import aiohttp
from aiohttp import web
import websockets

from config import (
    ConfigManager,
    get_delta_chart_url,
    get_previous_candle_timestamps,
    parse_timeframe_to_seconds,
    seconds_until_next_candle_close,
)
from state_manager import (
    BoundaryState,
    CooldownTracker,
    HealthMonitor,
    MuteState,
    ConfluenceTracker,
)
from ta_engine import (
    calculate_rsi,
    calculate_ema,
    calculate_volume_ratio,
    calculate_atr,
    detect_consolidation_squeeze,
    calculate_trade_targets,
    calculate_trade_setup_score,
    get_rsi_status,
    get_trend_status,
)
from alert_auditor import AlertAuditor

# ------------------------------------------------------------------
# Global Instances & Logging Initialization
# ------------------------------------------------------------------
config_mgr = ConfigManager("config.json")
boundary_state = BoundaryState()
cooldown_tracker = CooldownTracker()
health_monitor = HealthMonitor()
mute_state = MuteState()
confluence_tracker = ConfluenceTracker()
alert_auditor = AlertAuditor(config_mgr)

logger = logging.getLogger("DeltaAlerts")


def normalize_symbol(symbol: str) -> str:
    """Normalizes symbol string to official Delta Exchange India symbol format."""
    sym = symbol.strip().upper()
    if sym == "BTCINR":
        return "BTC_INR"
    if sym == "ETHINR":
        return "ETH_INR"
    if sym in ["GOLD", "GOLDUSD", "XAUT"]:
        return "XAUTUSD"
    return sym


def setup_logging(log_file_path: str):
    """Sets up log rotation to local file and stdout."""
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Rotating File Handler
    try:
        log_dir = os.path.dirname(log_file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logging initialized at: {log_file_path}")
    except Exception as e:
        fallback_path = os.path.abspath(os.path.join("logs", "app.log"))
        os.makedirs(os.path.dirname(fallback_path), exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            fallback_path,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.warning(
            f"Could not open primary log path ({log_file_path}): {e}. Fallback to {fallback_path}"
        )


def create_async_session() -> aiohttp.ClientSession:
    """Creates an aiohttp ClientSession with a ThreadedResolver for robust Windows DNS resolution."""
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    return aiohttp.ClientSession(connector=connector)


# ------------------------------------------------------------------
# Telegram Notification Gateway
# ------------------------------------------------------------------
class TelegramNotifier:
    """Async Telegram Bot Notification Gateway."""

    def __init__(self, config: ConfigManager):
        self.config = config

    async def send_message(self, text: str, retries: int = 3, bypass_mute: bool = False) -> bool:
        if not bypass_mute and mute_state.is_muted():
            logger.info("Alert suppressed (System is currently muted).")
            return False

        token = self.config.telegram_bot_token
        chat_id = self.config.telegram_chat_id

        if not token or not chat_id or "YOUR_TELEGRAM" in token:
            logger.warning(
                "Telegram credentials not configured properly. Skipping alert dispatch."
            )
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        async with create_async_session() as session:
            for attempt in range(1, retries + 1):
                try:
                    async with session.post(url, json=payload, timeout=10) as resp:
                        if resp.status == 200:
                            health_monitor.record_alert_sent()
                            logger.info("Telegram notification sent successfully.")
                            return True
                        else:
                            resp_text = await resp.text()
                            logger.error(
                                f"Telegram API error HTTP {resp.status} (Attempt {attempt}/{retries}): {resp_text}"
                            )
                except Exception as e:
                    logger.error(
                        f"Failed sending Telegram alert (Attempt {attempt}/{retries}): {e}"
                    )

                if attempt < retries:
                    await asyncio.sleep(2 ** attempt)

        return False


# ------------------------------------------------------------------
# ntfy.sh Mobile Push Notification Gateway
# ------------------------------------------------------------------
class NtfyNotifier:
    """Async ntfy.sh Mobile Push Notification Gateway."""

    def __init__(self, config: ConfigManager):
        self.config = config

    async def send_notification(self, title: str, text: str, priority: str = "high", tags: str = "chart_with_upwards_trend") -> bool:
        if not self.config.enable_ntfy_alerts or not self.config.ntfy_topic:
            return False

        if mute_state.is_muted():
            logger.info("ntfy alert suppressed (System is currently muted).")
            return False

        topic = self.config.ntfy_topic.strip()
        url = f"https://ntfy.sh/{topic}"
        headers = {
            "Title": title,
            "Priority": priority,
            "Tags": tags,
        }

        async with create_async_session() as session:
            try:
                async with session.post(url, data=text.encode("utf-8"), headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        logger.info(f"ntfy notification sent to topic '{topic}' successfully.")
                        return True
                    else:
                        resp_text = await resp.text()
                        logger.error(f"ntfy API error HTTP {resp.status}: {resp_text}")
            except Exception as e:
                logger.error(f"Failed sending ntfy notification: {e}")
        return False


# ------------------------------------------------------------------
# REST Candle Boundary Fetcher
# ------------------------------------------------------------------
class DeltaRESTClient:
    """REST Client for fetching historical candlestick boundaries from Delta Exchange India."""

    def __init__(self, config: ConfigManager):
        self.config = config

    async def fetch_symbol_timeframe_boundary(
        self, session: aiohttp.ClientSession, symbol: str, timeframe: str, semaphore: asyncio.Semaphore
    ) -> bool:
        async with semaphore:
            norm_symbol = normalize_symbol(symbol)
            base_url = self.config.delta_rest_url.rstrip("/")
            endpoint = f"{base_url}/v2/history/candles"

            period_sec = parse_timeframe_to_seconds(timeframe)
            start_ts, end_ts = get_previous_candle_timestamps(timeframe)
            params = {
                "symbol": norm_symbol,
                "resolution": timeframe,
                "start": str(start_ts - (30 * period_sec)),  # Fetch 30 periods for TA metrics
                "end": str(end_ts + 60),
            }

            for attempt in range(1, 4):
                try:
                    async with session.get(endpoint, params=params, timeout=15) as resp:
                        health_monitor.record_rest_fetch()
                        if resp.status != 200:
                            err_body = await resp.text()
                            logger.error(
                                f"REST fetch failed [{norm_symbol} {timeframe}] HTTP {resp.status}: {err_body}"
                            )
                            return False

                        data = await resp.json()
                        candles = data.get("result", [])
                        if not candles or not isinstance(candles, list):
                            logger.warning(f"No candle data returned for [{norm_symbol} {timeframe}]")
                            return False

                        # Ensure candles are in strict chronological order (oldest to newest)
                        candles.sort(key=lambda x: int(x.get("time", 0)) if isinstance(x, dict) else 0)

                        # Select target completed candle matching start_ts (latest completed candle)
                        completed_candles = [c for c in candles if isinstance(c, dict) and int(c.get("time", 0)) <= start_ts]
                        target_candle = completed_candles[-1] if completed_candles else candles[-1]

                        high = float(target_candle["high"])
                        low = float(target_candle["low"])
                        candle_time = int(target_candle["time"])

                        close_prices = [float(c.get("close", c.get("close_price", 0.0))) for c in candles]
                        volumes = [float(c.get("volume", c.get("vol", 0.0))) for c in candles]
                        high_prices = [float(c.get("high", 0.0)) for c in candles]
                        low_prices = [float(c.get("low", 0.0)) for c in candles]

                        boundary_state.update_boundary(
                            norm_symbol,
                            timeframe,
                            high,
                            low,
                            candle_time,
                            close_prices=close_prices,
                            volumes=volumes,
                            high_prices=high_prices,
                            low_prices=low_prices,
                        )
                        return True

                except Exception as e:
                    logger.error(
                        f"Exception fetching REST boundary [{norm_symbol} {timeframe}] (Attempt {attempt}/3): {e}"
                    )
                    if attempt < 3:
                        await asyncio.sleep(1.5 * attempt)

            return False

    async def sync_all_boundaries(self):
        """Fetches boundaries for all configured symbol and timeframe pairs concurrently."""
        pairs = config_mgr.get_active_pairs()
        semaphore = asyncio.Semaphore(self.config.rest_concurrency_limit)

        # Purge boundaries for any inactive symbol/timeframe pairs
        boundary_state.purge_inactive(self.config.symbols, self.config.timeframes, active_pairs=set(pairs))

        logger.info(
            f"Syncing REST boundaries for {len(pairs)} active (symbol, timeframe) pairs..."
        )

        async with create_async_session() as session:
            tasks = [
                self.fetch_symbol_timeframe_boundary(session, symbol, tf, semaphore)
                for (symbol, tf) in pairs
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            logger.info(
                f"Completed boundary sync: {success_count}/{len(tasks)} boundaries updated."
            )


# ------------------------------------------------------------------
# Breakout Detection Logic
# ------------------------------------------------------------------
async def evaluate_price_tick(
    symbol: str,
    current_price: float,
    notifier: TelegramNotifier,
    ntfy_notifier: Optional[NtfyNotifier] = None,
):
    """Evaluates live trade tick against all active boundaries for the symbol."""
    norm_symbol = normalize_symbol(symbol)
    active_symbols = [normalize_symbol(s) for s in config_mgr.symbols]

    # Ignore symbols that are not currently enabled in config.json
    if norm_symbol not in active_symbols:
        return

    if not config_mgr.is_symbol_allowed_at_time(norm_symbol):
        return

    timeframes = config_mgr.get_timeframes_for_symbol(norm_symbol)

    for tf in timeframes:
        boundary = boundary_state.get_boundary(norm_symbol, tf)
        if not boundary:
            continue

        high_level = boundary.high
        low_level = boundary.low
        candle_time = boundary.candle_time

        currency_symbol = "₹" if "INR" in norm_symbol else "$"

        # 1. Bullish Breakout Check
        if current_price > high_level:
            breakout_pct = ((current_price - high_level) / high_level) * 100.0

            # Filter out micro-wick breakouts if min_breakout_percent is configured
            if config_mgr.min_breakout_percent > 0 and breakout_pct < config_mgr.min_breakout_percent:
                continue

            if cooldown_tracker.can_alert(
                norm_symbol, tf, "HIGH", candle_time, cooldown_minutes=config_mgr.alert_cooldown_minutes, current_price=current_price
            ):
                # Calculate Technical Analysis Metrics
                rsi = calculate_rsi(boundary.close_prices)
                ema20 = calculate_ema(boundary.close_prices)
                vol_ratio = calculate_volume_ratio(boundary.volumes)
                atr = calculate_atr(boundary.high_prices, boundary.low_prices, boundary.close_prices)
                is_squeeze, squeeze_text = detect_consolidation_squeeze(boundary.high_prices, boundary.low_prices)

                # Optional Volume Filter check
                if config_mgr.volume_confirmation_enabled and vol_ratio is not None and vol_ratio < config_mgr.min_volume_ratio:
                    logger.info(
                        f"Skipping alert [{norm_symbol} {tf}]: Low volume ratio ({vol_ratio:.2f}x < {config_mgr.min_volume_ratio:.2f}x threshold)."
                    )
                    continue

                confluent_tfs = confluence_tracker.record_breakout(norm_symbol, tf, "HIGH")
                is_confluent = len(confluent_tfs) >= 2

                # Calculate 0-100 Setup Quality Score
                setup_score, grade_text = calculate_trade_setup_score(
                    "HIGH", current_price, high_level, rsi, ema20, vol_ratio, atr, is_squeeze, is_confluent
                )

                # Apply Setup Quality Filter if enabled
                if config_mgr.enable_quality_filter and setup_score < config_mgr.min_setup_score:
                    logger.info(
                        f"Skipping alert [{norm_symbol} {tf}]: Setup score {setup_score} < min threshold {config_mgr.min_setup_score}."
                    )
                    continue

                cooldown_tracker.record_alert(norm_symbol, tf, "HIGH", candle_time, current_price=current_price)
                chart_url = get_delta_chart_url(norm_symbol)

                rsi_text = get_rsi_status(rsi)
                trend_text = get_trend_status(current_price, ema20)
                vol_text = f"{vol_ratio:.1f}x Avg Volume 💥" if vol_ratio and vol_ratio >= 1.2 else (f"{vol_ratio:.1f}x Avg Volume" if vol_ratio else "Normal")
                atr_text = f"{(atr / current_price * 100):.2f}% (Volatility Expansion 💥)" if atr else "N/A"

                targets = calculate_trade_targets("HIGH", current_price, high_level, atr, currency_symbol)

                if is_confluent:
                    title_header = f"🔥 <b>MULTI-TIMEFRAME CONFLUENCE BULLISH BREAKOUT</b> 🔥\n<b>Confluence:</b> {', '.join(confluent_tfs).upper()}"
                else:
                    title_header = "🚀 <b>BULLISH BREAKOUT DETECTED</b> 🚀"

                msg_parts = [
                    f"{title_header}\n",
                    f"<b>Symbol:</b> #{norm_symbol}",
                    f"<b>Timeframe:</b> {tf}",
                    f"<b>Live Price:</b> {currency_symbol}{current_price:,.2f}",
                    f"<b>Broken High:</b> {currency_symbol}{high_level:,.2f}",
                    f"<b>Breakout:</b> +{breakout_pct:.2f}%\n",
                    f"{grade_text}",
                ]

                if config_mgr.enable_trade_targets:
                    msg_parts.append(f"\n{targets['formatted']}")

                if config_mgr.enable_technical_metrics:
                    msg_parts.append(
                        f"\n📊 <b>TECHNICAL METRICS</b>\n"
                        f"• <b>RSI (14):</b> {rsi_text}\n"
                        f"• <b>Trend (20 EMA):</b> {trend_text}\n"
                        f"• <b>Volume:</b> {vol_text}\n"
                        f"• <b>ATR (14):</b> {atr_text}\n"
                        f"• <b>Consolidation:</b> {squeeze_text}"
                    )

                msg_parts.append(
                    f"\n<b>Candle Period:</b> {time.strftime('%Y-%m-%d %H:%M IST', time.localtime(candle_time))}\n"
                    f"<b>Timestamp:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}\n\n"
                    f"📈 <a href='{chart_url}'>View on Delta Exchange</a>"
                )

                alert_text = "\n".join(msg_parts)
                logger.warning(
                    f"ALERT: Bullish Breakout [{norm_symbol} {tf}] Price {current_price} > High {high_level} (+{breakout_pct:.2f}%) Score={setup_score}"
                )
                await notifier.send_message(alert_text)

                if ntfy_notifier:
                    ntfy_title = f"🚀 BULLISH BREAKOUT: #{norm_symbol} ({tf})"
                    if config_mgr.enable_trade_targets:
                        ntfy_body = (
                            f"Live Price: {currency_symbol}{current_price:,.2f} | High: {currency_symbol}{high_level:,.2f} (+{breakout_pct:.2f}%)\n"
                            f"Score: {setup_score}/100 | Target 1: {currency_symbol}{targets['tp1']:,.2f} | SL: {currency_symbol}{targets['sl']:,.2f}"
                        )
                    else:
                        ntfy_body = (
                            f"Live Price: {currency_symbol}{current_price:,.2f} | High: {currency_symbol}{high_level:,.2f} (+{breakout_pct:.2f}%)\n"
                            f"Score: {setup_score}/100"
                        )
                    await ntfy_notifier.send_notification(
                        ntfy_title, ntfy_body, priority="high", tags="rocket,chart_with_upwards_trend"
                    )

        # 2. Bearish Breakout Check
        elif current_price < low_level:
            breakout_pct = ((low_level - current_price) / low_level) * 100.0

            # Filter out micro-wick breakouts if min_breakout_percent is configured
            if config_mgr.min_breakout_percent > 0 and breakout_pct < config_mgr.min_breakout_percent:
                continue

            if cooldown_tracker.can_alert(
                norm_symbol, tf, "LOW", candle_time, cooldown_minutes=config_mgr.alert_cooldown_minutes, current_price=current_price
            ):
                # Calculate Technical Analysis Metrics
                rsi = calculate_rsi(boundary.close_prices)
                ema20 = calculate_ema(boundary.close_prices)
                vol_ratio = calculate_volume_ratio(boundary.volumes)
                atr = calculate_atr(boundary.high_prices, boundary.low_prices, boundary.close_prices)
                is_squeeze, squeeze_text = detect_consolidation_squeeze(boundary.high_prices, boundary.low_prices)

                # Optional Volume Filter check
                if config_mgr.volume_confirmation_enabled and vol_ratio is not None and vol_ratio < config_mgr.min_volume_ratio:
                    logger.info(
                        f"Skipping alert [{norm_symbol} {tf}]: Low volume ratio ({vol_ratio:.2f}x < {config_mgr.min_volume_ratio:.2f}x threshold)."
                    )
                    continue

                confluent_tfs = confluence_tracker.record_breakout(norm_symbol, tf, "LOW")
                is_confluent = len(confluent_tfs) >= 2

                # Calculate 0-100 Setup Quality Score
                setup_score, grade_text = calculate_trade_setup_score(
                    "LOW", current_price, low_level, rsi, ema20, vol_ratio, atr, is_squeeze, is_confluent
                )

                # Apply Setup Quality Filter if enabled
                if config_mgr.enable_quality_filter and setup_score < config_mgr.min_setup_score:
                    logger.info(
                        f"Skipping alert [{norm_symbol} {tf}]: Setup score {setup_score} < min threshold {config_mgr.min_setup_score}."
                    )
                    continue

                cooldown_tracker.record_alert(norm_symbol, tf, "LOW", candle_time, current_price=current_price)
                chart_url = get_delta_chart_url(norm_symbol)

                rsi_text = get_rsi_status(rsi)
                trend_text = get_trend_status(current_price, ema20)
                vol_text = f"{vol_ratio:.1f}x Avg Volume 💥" if vol_ratio and vol_ratio >= 1.2 else (f"{vol_ratio:.1f}x Avg Volume" if vol_ratio else "Normal")
                atr_text = f"{(atr / current_price * 100):.2f}% (Volatility Expansion 💥)" if atr else "N/A"

                targets = calculate_trade_targets("LOW", current_price, low_level, atr, currency_symbol)

                if is_confluent:
                    title_header = f"🔥 <b>MULTI-TIMEFRAME CONFLUENCE BEARISH BREAKOUT</b> 🔥\n<b>Confluence:</b> {', '.join(confluent_tfs).upper()}"
                else:
                    title_header = "🔻 <b>BEARISH BREAKOUT DETECTED</b> 🔻"

                msg_parts = [
                    f"{title_header}\n",
                    f"<b>Symbol:</b> #{norm_symbol}",
                    f"<b>Timeframe:</b> {tf}",
                    f"<b>Live Price:</b> {currency_symbol}{current_price:,.2f}",
                    f"<b>Broken Low:</b> {currency_symbol}{low_level:,.2f}",
                    f"<b>Breakout:</b> -{breakout_pct:.2f}%\n",
                    f"{grade_text}",
                ]

                if config_mgr.enable_trade_targets:
                    msg_parts.append(f"\n{targets['formatted']}")

                if config_mgr.enable_technical_metrics:
                    msg_parts.append(
                        f"\n📊 <b>TECHNICAL METRICS</b>\n"
                        f"• <b>RSI (14):</b> {rsi_text}\n"
                        f"• <b>Trend (20 EMA):</b> {trend_text}\n"
                        f"• <b>Volume:</b> {vol_text}\n"
                        f"• <b>ATR (14):</b> {atr_text}\n"
                        f"• <b>Consolidation:</b> {squeeze_text}"
                    )

                msg_parts.append(
                    f"\n<b>Candle Period:</b> {time.strftime('%Y-%m-%d %H:%M IST', time.localtime(candle_time))}\n"
                    f"<b>Timestamp:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}\n\n"
                    f"📉 <a href='{chart_url}'>View on Delta Exchange</a>"
                )

                alert_text = "\n".join(msg_parts)
                logger.warning(
                    f"ALERT: Bearish Breakout [{norm_symbol} {tf}] Price {current_price} < Low {low_level} (-{breakout_pct:.2f}%) Score={setup_score}"
                )
                await notifier.send_message(alert_text)

                if ntfy_notifier:
                    ntfy_title = f"🔻 BEARISH BREAKOUT: #{norm_symbol} ({tf})"
                    if config_mgr.enable_trade_targets:
                        ntfy_body = (
                            f"Live Price: {currency_symbol}{current_price:,.2f} | Low: {currency_symbol}{low_level:,.2f} (-{breakout_pct:.2f}%)\n"
                            f"Score: {setup_score}/100 | Target 1: {currency_symbol}{targets['tp1']:,.2f} | SL: {currency_symbol}{targets['sl']:,.2f}"
                        )
                    else:
                        ntfy_body = (
                            f"Live Price: {currency_symbol}{current_price:,.2f} | Low: {currency_symbol}{low_level:,.2f} (-{breakout_pct:.2f}%)\n"
                            f"Score: {setup_score}/100"
                        )
                    await ntfy_notifier.send_notification(
                        ntfy_title, ntfy_body, priority="high", tags="small_red_triangle,chart_with_downwards_trend"
                    )


# ------------------------------------------------------------------
# Resilient WebSocket Engine with Ping Keep-Alive
# ------------------------------------------------------------------
class DeltaWSClient:
    """Persistent WebSocket Engine for Delta Exchange India."""

    def __init__(self, config: ConfigManager, notifier: TelegramNotifier, ntfy_notifier: Optional[NtfyNotifier] = None):
        self.config = config
        self.notifier = notifier
        self.ntfy_notifier = ntfy_notifier
        self.running = True

    async def connect_and_listen(self):
        backoff = 1.0
        max_backoff = 60.0

        while self.running:
            ws_url = self.config.delta_ws_url
            symbols = [normalize_symbol(s) for s in self.config.symbols]

            logger.info(f"Connecting to Delta WS endpoint: {ws_url}")
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=15,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    health_monitor.set_ws_connected(True)
                    backoff = 1.0
                    logger.info("WebSocket connection established.")

                    # Send subscription payload for ticker and trades channels
                    sub_payload = {
                        "type": "subscribe",
                        "payload": {
                            "channels": [
                                {"name": "ticker", "symbols": symbols},
                                {"name": "trades", "symbols": symbols},
                            ]
                        },
                    }
                    await ws.send(json.dumps(sub_payload))
                    logger.info(f"Subscribed WS channels for symbols: {symbols}")

                    while self.running:
                        try:
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            health_monitor.record_ws_message()
                            msg = json.loads(msg_raw)

                            # Extract Symbol & Price from WS payload
                            symbol = msg.get("sy") or msg.get("symbol") or msg.get("contract_symbol")
                            price = None

                            if "p" in msg:
                                price = float(msg["p"])
                            elif "close" in msg:
                                price = float(msg["close"])
                            elif "price" in msg:
                                price = float(msg["price"])
                            elif "mark_price" in msg:
                                price = float(msg["mark_price"])
                            elif "d" in msg and isinstance(msg["d"], list) and len(msg["d"]) > 0:
                                item = msg["d"][0]
                                if isinstance(item, dict):
                                    if not symbol:
                                        symbol = item.get("s")
                                    if "m" in item and item["m"] is not None:
                                        price = float(item["m"])
                                    elif "ohlc" in item and isinstance(item["ohlc"], list) and len(item["ohlc"]) >= 4:
                                        price = float(item["ohlc"][3])

                            if symbol and price and price > 0:
                                await evaluate_price_tick(symbol, price, self.notifier, self.ntfy_notifier)

                        except asyncio.TimeoutError:
                            await ws.ping()
                        except websockets.ConnectionClosed as cc:
                            logger.warning(f"WebSocket connection closed: {cc}")
                            break

            except Exception as e:
                health_monitor.set_ws_connected(False)
                health_monitor.record_reconnect()
                logger.error(f"WebSocket connection error: {e}")

            if not self.running:
                break

            jitter = random.uniform(0.5, 1.5)
            sleep_time = min(max_backoff, backoff * jitter)
            logger.info(f"Reconnecting to WebSocket in {sleep_time:.2f} seconds...")
            await asyncio.sleep(sleep_time)
            backoff = min(max_backoff, backoff * 2.0)

    def stop(self):
        self.running = False


# ------------------------------------------------------------------
# Background Scheduler Tasks
# ------------------------------------------------------------------
async def candle_close_scheduler_task(rest_client: DeltaRESTClient):
    """Schedules REST boundary updates exactly on candle completion for active timeframes."""
    logger.info("Candle close scheduler task started.")
    while True:
        try:
            timeframes = config_mgr.get_all_active_timeframes()
            if not timeframes:
                await asyncio.sleep(10)
                continue

            wait_times = [
                (tf, seconds_until_next_candle_close(tf)) for tf in timeframes
            ]
            wait_times.sort(key=lambda x: x[1])

            next_tf, shortest_wait = wait_times[0]
            buffer_wait = shortest_wait + 1.5

            await asyncio.sleep(buffer_wait)

            now = time.time()
            closing_tfs = [
                tf for tf in timeframes if seconds_until_next_candle_close(tf, now) > 5.0
            ]
            if not closing_tfs:
                closing_tfs = [next_tf]

            logger.info(f"Candle closed for timeframes {closing_tfs}. Refreshing REST boundaries...")
            active_pairs = [
                (s, tf) for (s, tf) in config_mgr.get_active_pairs() if tf in closing_tfs
            ]
            semaphore = asyncio.Semaphore(config_mgr.rest_concurrency_limit)

            async with create_async_session() as session:
                tasks = [
                    rest_client.fetch_symbol_timeframe_boundary(session, s, tf, semaphore)
                    for (s, tf) in active_pairs
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in candle close scheduler: {e}")
            await asyncio.sleep(5)


async def heartbeat_task(notifier: TelegramNotifier):
    """Periodic Telegram health status heartbeat."""
    logger.info("Heartbeat scheduler task started.")
    while True:
        try:
            interval_hours = config_mgr.heartbeat_interval_hours
            await asyncio.sleep(interval_hours * 3600)

            summary = health_monitor.get_summary()
            heartbeat_msg = (
                f"💓 <b>DELTA ALERTS SYSTEM HEARTBEAT</b> 💓\n\n"
                f"<b>Status:</b> {'🟢 Online' if summary['is_ws_connected'] else '🔴 Disconnected'}\n"
                f"<b>Uptime:</b> {summary['uptime']}\n"
                f"<b>Active Symbols:</b> {', '.join(config_mgr.symbols)}\n"
                f"<b>WS Messages Processed:</b> {summary['total_ws_messages']:,}\n"
                f"<b>REST Boundary Fetches:</b> {summary['total_rest_fetches']:,}\n"
                f"<b>Total Alerts Fired:</b> {summary['total_alerts_sent']}\n"
                f"<b>Reconnections:</b> {summary['reconnect_count']}\n"
                f"<b>Timestamp:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}"
            )
            await notifier.send_message(heartbeat_msg)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in heartbeat task: {e}")
            await asyncio.sleep(60)


async def config_reload_task(notifier: TelegramNotifier):
    """Checks for config.json modifications dynamically on the fly."""
    logger.info("Config hot-reload monitoring task started.")
    while True:
        try:
            check_interval = config_mgr.config_check_interval_seconds
            await asyncio.sleep(check_interval)
            updated = config_mgr.load_config()
            if updated:
                logger.info("Config reloaded dynamically. Active settings updated.")
                active_pairs = config_mgr.get_active_pairs()
                boundary_state.purge_inactive(config_mgr.symbols, config_mgr.timeframes, active_pairs=set(active_pairs))

                # Re-sync boundaries for new symbol/timeframe pairs immediately
                rest_client = DeltaRESTClient(config_mgr)
                await rest_client.sync_all_boundaries()
                
                reload_msg = (
                    f"⚙️ <b>CONFIG HOT-RELOADED</b> ⚙️\n\n"
                    f"<b>Active Symbols:</b> {', '.join(config_mgr.symbols)}\n"
                    f"<b>Active Pairs:</b> {len(active_pairs)} configured\n"
                    f"<b>Timestamp:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}"
                )
                await notifier.send_message(reload_msg)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error checking config reload: {e}")
            await asyncio.sleep(10)


async def telegram_command_listener_task(notifier: TelegramNotifier):
    """Polls Telegram getUpdates API for incoming bot commands (/status, /symbols, /mute, /unmute, /help)."""
    logger.info("Telegram command listener task started.")
    offset = 0
    while True:
        try:
            token = config_mgr.telegram_bot_token
            if not token or "YOUR_TELEGRAM" in token:
                await asyncio.sleep(15)
                continue

            url = f"https://api.telegram.org/bot{token}/getUpdates"
            params = {"offset": offset, "timeout": 10}

            async with create_async_session() as session:
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        updates = data.get("result", [])
                        for u in updates:
                            if not isinstance(u, dict) or "update_id" not in u:
                                continue
                            offset = u["update_id"] + 1
                            msg = u.get("message") or u.get("edited_message")
                            if not isinstance(msg, dict):
                                continue
                            chat_dict = msg.get("chat")
                            chat_id = str(chat_dict.get("id", "")) if isinstance(chat_dict, dict) else ""
                            text = str(msg.get("text") or "").strip()

                            # Only process commands from configured chat_id
                            if chat_id != config_mgr.telegram_chat_id or not text.startswith("/"):
                                continue

                            cmd_parts = text.split()
                            if not cmd_parts:
                                continue
                            cmd = cmd_parts[0].lower()

                            if cmd == "/status":
                                summary = health_monitor.get_summary()
                                mute_str = f"🔕 Muted ({mute_state.remaining_minutes()}m remaining)" if mute_state.is_muted() else "🔔 Active"
                                sched_active = config_mgr.is_within_operating_schedule()
                                sched_str = "🟢 Active (Operating Hours)" if sched_active else "🔕 Quiet (Outside Operating Hours)"
                                us_window_str = f"{config_mgr.us_stocks_active_hours_ist.get('START', '18:30')} - {config_mgr.us_stocks_active_hours_ist.get('END', '23:59')} IST" if config_mgr.us_stocks_filter_enabled else "24/7"
                                reply = (
                                    f"📊 <b>DELTA SYSTEM STATUS</b> 📊\n\n"
                                    f"<b>Service Status:</b> {'🟢 Online' if summary['is_ws_connected'] else '🔴 Disconnected'}\n"
                                    f"<b>Alert Mode:</b> {mute_str}\n"
                                    f"<b>Schedule Status:</b> {sched_str}\n"
                                    f"<b>US Stocks Window:</b> {us_window_str}\n"
                                    f"<b>Breakout Mode:</b> {config_mgr.breakout_mode}\n"
                                    f"<b>ntfy Topic:</b> {config_mgr.ntfy_topic}\n"
                                    f"<b>Uptime:</b> {summary['uptime']}\n"
                                    f"<b>Active Symbols:</b> {len(config_mgr.symbols)}\n"
                                    f"<b>Active Pairs:</b> {len(config_mgr.get_active_pairs())}\n"
                                    f"<b>WS Messages:</b> {summary['total_ws_messages']:,}\n"
                                    f"<b>Alerts Sent:</b> {summary['total_alerts_sent']}\n"
                                    f"<b>Timestamp:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}"
                                )
                                await notifier.send_message(reply, bypass_mute=True)

                            elif cmd == "/symbols":
                                lines = ["📋 <b>MONITORED SYMBOLS & TIMEFRAMES</b> 📋\n"]
                                for sym in config_mgr.symbols:
                                    tfs = config_mgr.get_timeframes_for_symbol(sym)
                                    lines.append(f"• <b>#{sym}:</b> {', '.join(tfs)}")
                                reply = "\n".join(lines)
                                await notifier.send_message(reply, bypass_mute=True)

                            elif cmd == "/mute":
                                mins = 30
                                if len(cmd_parts) > 1 and cmd_parts[1].isdigit():
                                    mins = int(cmd_parts[1])
                                mute_state.mute(mins)
                                reply = f"🔕 <b>Alerts muted for {mins} minutes.</b> Use /unmute to resume anytime."
                                await notifier.send_message(reply, bypass_mute=True)

                            elif cmd == "/unmute":
                                mute_state.unmute()
                                reply = "🔔 <b>Alerts unmuted!</b> Live breakout notifications resumed."
                                await notifier.send_message(reply, bypass_mute=True)

                            elif cmd == "/audit":
                                hours = 24
                                title_prefix = "DAILY BREAKOUT ACCURACY AUDIT"
                                if len(cmd_parts) > 1:
                                    arg = cmd_parts[1].lower().strip()
                                    if arg in ["7d", "week", "weekly", "7"]:
                                        hours = 168
                                        title_prefix = "WEEKLY BREAKOUT ACCURACY AUDIT"
                                    elif arg in ["30d", "month", "monthly", "30"]:
                                        hours = 720
                                        title_prefix = "MONTHLY BREAKOUT ACCURACY AUDIT"
                                    elif arg in ["all", "alltime"]:
                                        hours = None
                                        title_prefix = "ALL-TIME BREAKOUT ACCURACY AUDIT"
                                    elif arg.endswith("h") and arg[:-1].isdigit():
                                        hours = int(arg[:-1])
                                        title_prefix = f"{hours}H BREAKOUT ACCURACY AUDIT"
                                    elif arg.endswith("d") and arg[:-1].isdigit():
                                        hours = int(arg[:-1]) * 24
                                        title_prefix = f"{arg.upper()} BREAKOUT ACCURACY AUDIT"

                                wait_msg = f"⏳ <i>Analyzing breakout accuracy for {title_prefix.lower()}... Please wait.</i>"
                                await notifier.send_message(wait_msg, bypass_mute=True)
                                try:
                                    res = await alert_auditor.run_audit(hours=hours)
                                    report = alert_auditor.format_telegram_report(res, title_prefix=title_prefix)
                                    await notifier.send_message(report, bypass_mute=True)
                                except Exception as e:
                                    logger.error(f"Error executing on-demand audit command: {e}")
                                    await notifier.send_message(f"❌ <b>Audit Error:</b> {e}", bypass_mute=True)

                            elif cmd == "/help":
                                reply = (
                                    "🤖 <b>DELTA ALERT BOT COMMANDS</b> 🤖\n\n"
                                    "• <b>/status</b> - Show system health & uptime\n"
                                    "• <b>/symbols</b> - List all active symbols and timeframes\n"
                                    "• <b>/audit [24h|7d|30d|all]</b> - Breakout accuracy & win-rate audit\n"
                                    "• <b>/mute [mins]</b> - Mute alerts for X minutes (default 30m)\n"
                                    "• <b>/unmute</b> - Resume alert notifications\n"
                                    "• <b>/help</b> - Show this help menu"
                                )
                                await notifier.send_message(reply, bypass_mute=True)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in telegram command listener: {e}")
            await asyncio.sleep(5)


async def alert_audit_scheduler_task(notifier: TelegramNotifier):
    """Periodically dispatches daily and weekly breakout accuracy audit reports to Telegram."""
    logger.info("Alert audit background scheduler started.")
    last_daily_date = ""
    last_weekly_date = ""

    while True:
        try:
            now = time.time()
            current_hm = time.strftime("%H:%M", time.localtime(now))
            current_ymd = time.strftime("%Y-%m-%d", time.localtime(now))
            current_day = time.strftime("%a", time.localtime(now)).upper()

            # 1. Daily Audit Dispatch
            if config_mgr.enable_daily_audit_report:
                target_hm = config_mgr.daily_audit_time_ist
                if current_hm == target_hm and current_ymd != last_daily_date:
                    last_daily_date = current_ymd
                    logger.info(f"Triggering scheduled daily audit report at {current_hm} IST...")
                    res = await alert_auditor.run_audit(hours=24)
                    report = alert_auditor.format_telegram_report(res, title_prefix="DAILY BREAKOUT ACCURACY AUDIT")
                    await notifier.send_message(report, bypass_mute=True)

            # 2. Weekly Audit Dispatch
            if config_mgr.enable_weekly_audit_report:
                target_day = config_mgr.weekly_audit_day_ist
                target_hm = config_mgr.weekly_audit_time_ist
                if current_day == target_day and current_hm == target_hm and current_ymd != last_weekly_date:
                    last_weekly_date = current_ymd
                    logger.info(f"Triggering scheduled weekly audit report on {current_day} {current_hm} IST...")
                    res = await alert_auditor.run_audit(hours=168)
                    report = alert_auditor.format_telegram_report(res, title_prefix="WEEKLY BREAKOUT ACCURACY AUDIT")
                    await notifier.send_message(report, bypass_mute=True)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in alert audit scheduler: {e}")

        await asyncio.sleep(45)


async def health_server_task():
    """Runs a lightweight HTTP server for Render/cloud deployment health checks and keep-alive."""
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()

    async def handle_root(request):
        summary = health_monitor.get_summary()
        return web.json_response({
            "status": "online",
            "system": "Delta Exchange India Breakout Alert System",
            "ws_connected": summary.get("is_ws_connected", False),
            "uptime": summary.get("uptime", ""),
            "active_pairs": len(config_mgr.get_active_pairs()),
            "alerts_sent": summary.get("total_alerts_sent", 0),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S IST")
        })

    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_root)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    try:
        await site.start()
        logger.info(f"Render health HTTP server listening on port {port}")
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await runner.cleanup()
    except Exception as e:
        logger.error(f"Health HTTP server error: {e}")


# ------------------------------------------------------------------
# Main Service Application Runner
# ------------------------------------------------------------------
async def async_main():
    setup_logging(config_mgr.log_file_path)
    logger.info("Starting Delta Exchange India Breakout Alert System...")

    notifier = TelegramNotifier(config_mgr)
    ntfy_notifier = NtfyNotifier(config_mgr)
    rest_client = DeltaRESTClient(config_mgr)
    ws_client = DeltaWSClient(config_mgr, notifier, ntfy_notifier)

    # Initial REST sync of all boundaries
    await rest_client.sync_all_boundaries()

    # Launch Background Async Tasks
    tasks = [
        asyncio.create_task(ws_client.connect_and_listen()),
        asyncio.create_task(candle_close_scheduler_task(rest_client)),
        asyncio.create_task(heartbeat_task(notifier)),
        asyncio.create_task(config_reload_task(notifier)),
        asyncio.create_task(telegram_command_listener_task(notifier)),
        asyncio.create_task(alert_audit_scheduler_task(notifier)),
        asyncio.create_task(health_server_task()),
    ]

    # Startup Telegram Notification
    active_pairs = config_mgr.get_active_pairs()
    service_mode = "Cloud Container (Render)" if os.environ.get("RENDER") else "Continuous Background Windows Service"
    startup_msg = (
        f"⚡ <b>DELTA ALERTS SERVICE STARTED</b> ⚡\n\n"
        f"<b>Monitored Symbols:</b> {', '.join(config_mgr.symbols)}\n"
        f"<b>Active Symbol-Timeframe Pairs:</b> {len(active_pairs)}\n"
        f"<b>Service Mode:</b> {service_mode}\n"
        f"<b>Timestamp:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}"
    )
    await notifier.send_message(startup_msg)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Main tasks cancelled. Shutting down...")
    finally:
        ws_client.stop()


def main():
    """Synchronous entry point with signal handling."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal():
        logger.info("Received termination signal. Cancelling tasks...")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(async_main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Keyboard interrupt received. Terminating.")
    finally:
        loop.close()
        logger.info("Service shut down cleanly.")


if __name__ == "__main__":
    main()
