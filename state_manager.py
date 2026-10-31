"""
State Manager for tracking historical high/low boundaries, alert cooldown locks,
and service health metrics.
"""

import time
import threading
import logging
import os
import json
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Any, Set, List

logger = logging.getLogger("DeltaAlerts.StateManager")


@dataclass
class CandleBoundary:
    symbol: str
    timeframe: str
    high: float
    low: float
    candle_time: int  # Unix timestamp of the completed boundary candle start
    fetched_at: float
    close_prices: List[float] = None
    volumes: List[float] = None
    high_prices: List[float] = None
    low_prices: List[float] = None


class BoundaryState:
    """
    Thread-safe storage for active candlestick high/low boundaries.
    Keyed by (symbol, timeframe).
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._boundaries: Dict[Tuple[str, str], CandleBoundary] = {}

    def update_boundary(
        self,
        symbol: str,
        timeframe: str,
        high: float,
        low: float,
        candle_time: int,
        close_prices: Optional[List[float]] = None,
        volumes: Optional[List[float]] = None,
        high_prices: Optional[List[float]] = None,
        low_prices: Optional[List[float]] = None,
    ):
        with self._lock:
            key = (symbol.upper(), timeframe.lower())
            self._boundaries[key] = CandleBoundary(
                symbol=symbol.upper(),
                timeframe=timeframe.lower(),
                high=high,
                low=low,
                candle_time=candle_time,
                fetched_at=time.time(),
                close_prices=close_prices or [],
                volumes=volumes or [],
                high_prices=high_prices or [],
                low_prices=low_prices or [],
            )
            logger.info(
                f"Updated boundary [{symbol} {timeframe}]: High={high}, Low={low}, CandleTime={candle_time}"
            )

    def get_boundary(self, symbol: str, timeframe: str) -> Optional[CandleBoundary]:
        with self._lock:
            key = (symbol.upper(), timeframe.lower())
            return self._boundaries.get(key)

    def purge_inactive(
        self,
        active_symbols: List[str],
        active_timeframes: List[str],
        active_pairs: Optional[Set[Tuple[str, str]]] = None,
    ):
        """Purges boundaries for symbols, timeframes, or specific pairs no longer in active configuration."""
        with self._lock:
            if active_pairs is not None:
                valid_pairs = {(s.upper(), t.lower()) for (s, t) in active_pairs}
                to_delete = [key for key in self._boundaries if key not in valid_pairs]
            else:
                valid_symbols = {s.upper() for s in active_symbols}
                valid_timeframes = {t.lower() for t in active_timeframes}
                to_delete = [
                    key for key in self._boundaries
                    if key[0] not in valid_symbols or key[1] not in valid_timeframes
                ]

            for key in to_delete:
                del self._boundaries[key]
                logger.info(f"Purged boundary for inactive symbol/timeframe pair: {key}")

    def get_all_boundaries(self) -> Dict[Tuple[str, str], CandleBoundary]:
        with self._lock:
            return self._boundaries.copy()


COOLDOWN_STORAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cooldown_state.json")


class CooldownTracker:
    """
    Thread-safe tracker to prevent duplicate alerts for a specific symbol/timeframe/direction.
    Allows new alerts when a new candle boundary opens OR when ALERT_COOLDOWN_MINUTES has elapsed AND price makes a new peak.
    Persists alert locks to disk (cooldown_state.json) so service restarts do not cause re-alerts.
    """

    def __init__(self, storage_path: str = COOLDOWN_STORAGE_PATH):
        self._lock = threading.RLock()
        self.storage_path = storage_path
        # Set of (symbol, timeframe, direction, candle_time)
        self._alerted_boundaries: Set[Tuple[str, str, str, int]] = set()
        # Dict of (symbol, timeframe, direction) -> timestamp of last alert
        self._last_alert_time: Dict[Tuple[str, str, str], float] = {}
        # Dict of (symbol, timeframe, direction) -> price of last alert
        self._last_alert_price: Dict[Tuple[str, str, str], float] = {}
        self._load_state()

    def _load_state(self):
        """Loads alerted boundaries, last alert timestamps, and prices from JSON disk cache."""
        with self._lock:
            if not os.path.exists(self.storage_path):
                return
            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                raw_boundaries = data.get("boundaries", [])
                for item in raw_boundaries:
                    if isinstance(item, list) and len(item) == 4:
                        self._alerted_boundaries.add(
                            (str(item[0]).upper(), str(item[1]).lower(), str(item[2]).upper(), int(item[3]))
                        )

                raw_last_time = data.get("last_alert_time", {})
                for k, v in raw_last_time.items():
                    parts = k.split("|")
                    if len(parts) == 3:
                        self._last_alert_time[(parts[0].upper(), parts[1].lower(), parts[2].upper())] = float(v)

                raw_last_price = data.get("last_alert_price", {})
                for k, v in raw_last_price.items():
                    parts = k.split("|")
                    if len(parts) == 3:
                        self._last_alert_price[(parts[0].upper(), parts[1].lower(), parts[2].upper())] = float(v)

                logger.info(
                    f"Loaded {len(self._alerted_boundaries)} alerted boundaries from disk cache: {self.storage_path}"
                )
            except Exception as e:
                logger.error(f"Failed to load cooldown state from {self.storage_path}: {e}")

    def _save_state(self):
        """Saves alerted boundaries, timestamps, and prices to JSON disk cache atomically."""
        with self._lock:
            try:
                boundaries_list = [list(key) for key in self._alerted_boundaries]
                last_time_dict = {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in self._last_alert_time.items()}
                last_price_dict = {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in self._last_alert_price.items()}

                data = {
                    "boundaries": boundaries_list,
                    "last_alert_time": last_time_dict,
                    "last_alert_price": last_price_dict,
                }

                temp_path = f"{self.storage_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

                if os.path.exists(self.storage_path):
                    os.replace(temp_path, self.storage_path)
                else:
                    os.rename(temp_path, self.storage_path)
            except Exception as e:
                logger.error(f"Failed to save cooldown state to {self.storage_path}: {e}")

    def can_alert(
        self,
        symbol: str,
        timeframe: str,
        direction: str,
        candle_time: int,
        cooldown_minutes: int = 15,
        current_price: Optional[float] = None,
    ) -> bool:
        """
        Checks if an alert can be sent.
        Returns True if:
        1. This candle_time has not alerted yet (New Candle Boundary), OR
        2. Elapsed time since the last alert >= cooldown_minutes AND price makes a new peak.
        """
        with self._lock:
            key_boundary = (symbol.upper(), timeframe.lower(), direction.upper(), candle_time)
            if key_boundary not in self._alerted_boundaries:
                return True

            key_pair = (symbol.upper(), timeframe.lower(), direction.upper())
            last_ts = self._last_alert_time.get(key_pair, 0.0)
            elapsed_sec = time.time() - last_ts
            cooldown_sec = max(1, cooldown_minutes) * 60.0

            if elapsed_sec < cooldown_sec:
                return False

            if current_price is not None and key_pair in self._last_alert_price:
                last_px = self._last_alert_price[key_pair]
                is_bullish = direction.upper() in ["HIGH", "BULLISH"]
                if is_bullish and current_price <= last_px:
                    return False
                elif not is_bullish and current_price >= last_px:
                    return False

            return True

    def record_alert(
        self,
        symbol: str,
        timeframe: str,
        direction: str,
        candle_time: int,
        current_price: Optional[float] = None,
    ):
        """Marks this candle boundary as alerted and updates last alert timestamp and price."""
        with self._lock:
            key_boundary = (symbol.upper(), timeframe.lower(), direction.upper(), candle_time)
            key_pair = (symbol.upper(), timeframe.lower(), direction.upper())
            self._alerted_boundaries.add(key_boundary)
            self._last_alert_time[key_pair] = time.time()
            if current_price is not None:
                self._last_alert_price[key_pair] = current_price
            self._save_state()
            logger.info(f"Recorded breakout alert for {key_pair} (CandleTime={candle_time}, Price={current_price})")

    def has_alerted_for_candle(self, symbol: str, timeframe: str, direction: str, candle_time: int) -> bool:
        """Legacy helper maintained for compatibility."""
        with self._lock:
            key = (symbol.upper(), timeframe.lower(), direction.upper(), candle_time)
            return key in self._alerted_boundaries

    def record_candle_alert(self, symbol: str, timeframe: str, direction: str, candle_time: int):
        """Legacy helper redirecting to record_alert."""
        self.record_alert(symbol, timeframe, direction, candle_time)

    def clean_expired(self, max_age_hours: float = 48.0):
        """Purges old boundary alert keys."""
        with self._lock:
            now = int(time.time())
            max_age_sec = int(max_age_hours * 3600)
            expired = [
                key for key in self._alerted_boundaries
                if (now - key[3]) > max_age_sec
            ]
            for key in expired:
                self._alerted_boundaries.remove(key)
            if expired:
                self._save_state()


class HealthMonitor:
    """
    Service health & diagnostic counters for Heartbeat reporting.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.start_time: float = time.time()
        self.total_ws_messages: int = 0
        self.total_rest_fetches: int = 0
        self.total_alerts_sent: int = 0
        self.last_ws_message_time: float = 0.0
        self.is_ws_connected: bool = False
        self.reconnect_count: int = 0

    def record_ws_message(self):
        with self._lock:
            self.total_ws_messages += 1
            self.last_ws_message_time = time.time()

    def record_rest_fetch(self):
        with self._lock:
            self.total_rest_fetches += 1

    def record_alert_sent(self):
        with self._lock:
            self.total_alerts_sent += 1

    def set_ws_connected(self, connected: bool):
        with self._lock:
            self.is_ws_connected = connected

    def record_reconnect(self):
        with self._lock:
            self.reconnect_count += 1

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            uptime_seconds = int(time.time() - self.start_time)
            days, remainder = divmod(uptime_seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"

            seconds_since_last_ws = int(time.time() - self.last_ws_message_time) if self.last_ws_message_time > 0 else -1

            return {
                "uptime": uptime_str,
                "is_ws_connected": self.is_ws_connected,
                "reconnect_count": self.reconnect_count,
                "total_ws_messages": self.total_ws_messages,
                "total_rest_fetches": self.total_rest_fetches,
                "total_alerts_sent": self.total_alerts_sent,
                "seconds_since_last_ws": seconds_since_last_ws,
            }


class MuteState:
    """
    Thread-safe alert muting manager for Telegram /mute and /unmute commands.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._mute_until: float = 0.0

    def mute(self, minutes: int):
        with self._lock:
            self._mute_until = time.time() + max(1, minutes) * 60.0
            logger.info(f"Alerts muted for {minutes} minutes (until {time.strftime('%H:%M:%S', time.localtime(self._mute_until))})")

    def unmute(self):
        with self._lock:
            self._mute_until = 0.0
            logger.info("Alerts unmuted.")

    def is_muted(self) -> bool:
        with self._lock:
            return time.time() < self._mute_until

    def remaining_minutes(self) -> int:
        with self._lock:
            if not self.is_muted():
                return 0
            return int((self._mute_until - time.time()) / 60.0) + 1


class ConfluenceTracker:
    """
    Tracks recent breakouts per symbol to detect multi-timeframe confluence.
    """

    def __init__(self):
        self._lock = threading.RLock()
        # Map of (symbol, direction) -> Dict[timeframe, timestamp]
        self._recent_breakouts: Dict[Tuple[str, str], Dict[str, float]] = {}

    def record_breakout(self, symbol: str, timeframe: str, direction: str) -> List[str]:
        """
        Records a breakout and returns a list of timeframes that broke out
        in the same direction within the confluence window (e.g., 5 minutes).
        """
        with self._lock:
            key = (symbol.upper(), direction.upper())
            now = time.time()

            if key not in self._recent_breakouts:
                self._recent_breakouts[key] = {}

            self._recent_breakouts[key][timeframe.lower()] = now

            # Find timeframes for this symbol/direction within 300s (5 min) window
            confluent_tfs = [
                tf for tf, ts in self._recent_breakouts[key].items()
                if (now - ts) <= 300.0
            ]
            return confluent_tfs
