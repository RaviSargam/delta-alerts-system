"""
Configuration loader, validator, and timeframe calculation utilities for
Delta Exchange India Breakout Alerts System.
"""

import json
import os
import sys
import time
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger("DeltaAlerts.Config")


def _load_env_file():
    """Loads key-value pairs from .env into os.environ if present."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip().strip("'\""))
        except Exception:
            pass


_load_env_file()

SUPPORTED_TIMEFRAMES: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "6h": 21600,
    "1d": 86400,
}


def parse_timeframe_to_seconds(tf: str) -> int:
    """Converts timeframe string to seconds."""
    tf_lower = tf.lower()
    if tf_lower not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{tf}'. Supported: {list(SUPPORTED_TIMEFRAMES.keys())}")
    return SUPPORTED_TIMEFRAMES[tf_lower]


def get_previous_candle_timestamps(timeframe: str, reference_time: Optional[float] = None) -> tuple[int, int]:
    """
    Computes start and end Unix timestamps (in seconds) for fetching the latest completed candle
    from Delta Exchange REST API for a given timeframe.
    """
    now = reference_time if reference_time is not None else time.time()
    period_sec = parse_timeframe_to_seconds(timeframe)
    current_candle_start = int(now // period_sec) * period_sec
    previous_candle_start = current_candle_start - period_sec
    # End timestamp is exclusive or up to current_candle_start
    return previous_candle_start, current_candle_start


def seconds_until_next_candle_close(timeframe: str, reference_time: Optional[float] = None) -> float:
    """Calculates remaining seconds until the current candle closes."""
    now = reference_time if reference_time is not None else time.time()
    period_sec = parse_timeframe_to_seconds(timeframe)
    current_candle_start = int(now // period_sec) * period_sec
    next_candle_start = current_candle_start + period_sec
    remaining = next_candle_start - now
    return max(0.1, remaining)


def get_delta_chart_url(symbol: str) -> str:
    """Generates direct URL to Delta Exchange India chart for symbol."""
    return f"https://india.delta.exchange/app/analytics/trade/{symbol}"


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


class ConfigManager:
    """
    Thread-safe Configuration Manager with auto-reloading support.
    """

    def __init__(self, config_path: str = None):
        if config_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(base_dir, "config.json")
        self.config_path = os.path.abspath(config_path)
        self._last_mtime: float = 0
        self.config_data: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> bool:
        """Loads configuration from JSON file. Returns True if updated."""
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file not found at {self.config_path}. Using fallback defaults.")
            self._set_defaults()
            return True

        try:
            mtime = os.path.getmtime(self.config_path)
            if mtime <= self._last_mtime and self.config_data:
                return False

            with open(self.config_path, "r", encoding="utf-8") as f:
                content = f.read()
                # Strip single-line comments starting with // or #
                lines = [
                    line for line in content.splitlines()
                    if not line.strip().startswith("//") and not line.strip().startswith("#")
                ]
                data = json.loads("\n".join(lines))

            self.config_data = self._validate_and_sanitize(data)
            self._last_mtime = mtime
            logger.info(f"Loaded config successfully from {self.config_path}")
            return True
        except Exception as e:
            logger.error(f"Error reading config file {self.config_path}: {e}")
            if not self.config_data:
                self._set_defaults()
            return False

    def _validate_and_sanitize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validates and applies default fallbacks for missing keys."""
        symbols = data.get("SYMBOLS", ["BTCUSD", "ETHUSD"])
        if not isinstance(symbols, list) or not symbols:
            symbols = ["BTCUSD", "ETHUSD"]

        default_timeframes = data.get("TIMEFRAMES", ["1h", "4h"])
        valid_default_tf = [tf for tf in default_timeframes if tf.lower() in SUPPORTED_TIMEFRAMES]
        if not valid_default_tf:
            valid_default_tf = ["1h", "4h"]

        # Parse per-symbol timeframes dict
        raw_symbol_tf = data.get("SYMBOL_TIMEFRAMES", {})
        sanitized_symbol_tf: Dict[str, List[str]] = {}
        if isinstance(raw_symbol_tf, dict):
            for sym, tfs in raw_symbol_tf.items():
                if isinstance(tfs, list):
                    valid_sym_tfs = [tf for tf in tfs if tf.lower() in SUPPORTED_TIMEFRAMES]
                    if valid_sym_tfs:
                        sanitized_symbol_tf[str(sym).upper()] = valid_sym_tfs

        cooldown = int(data.get("ALERT_COOLDOWN_MINUTES", 5))
        cooldown = max(1, cooldown)

        breakout_mode = str(data.get("BREAKOUT_MODE", "INSTANT")).upper()
        if breakout_mode not in ["INSTANT", "CANDLE_CLOSE"]:
            breakout_mode = "INSTANT"

        min_breakout_pct = float(data.get("MIN_BREAKOUT_PERCENT", 0.05))
        min_breakout_pct = max(0.0, min_breakout_pct)

        vol_confirm = bool(data.get("VOLUME_CONFIRMATION_ENABLED", True))
        min_vol_ratio = float(data.get("MIN_VOLUME_RATIO", 0.70))
        min_vol_ratio = max(0.0, min_vol_ratio)

        enable_quality_filter = bool(data.get("ENABLE_QUALITY_FILTER", True))
        min_setup_score = int(data.get("MIN_SETUP_SCORE", 70))
        min_setup_score = max(0, min(100, min_setup_score))

        ntfy_topic = str(data.get("NTFY_TOPIC", "delta_breakouts_ravi")).strip()
        enable_ntfy = bool(data.get("ENABLE_NTFY_ALERTS", True))

        enable_trade_targets = bool(data.get("ENABLE_TRADE_TARGETS", False))
        enable_technical_metrics = bool(data.get("ENABLE_TECHNICAL_METRICS", True))

        schedule_enabled = bool(data.get("SCHEDULE_ENABLED", True))
        raw_schedule = data.get("OPERATING_SCHEDULE_IST")
        if not isinstance(raw_schedule, dict):
            raw_schedule = {
                "MON": [{"START": "00:00", "END": "23:59"}],
                "TUE": [{"START": "00:00", "END": "23:59"}],
                "WED": [{"START": "00:00", "END": "23:59"}],
                "THU": [{"START": "00:00", "END": "23:59"}],
                "FRI": [{"START": "00:00", "END": "23:59"}],
                "SAT": [],
                "SUN": [{"START": "18:00", "END": "23:59"}],
            }

        us_stocks_filter = bool(data.get("US_STOCKS_FILTER_ENABLED", True))
        us_stocks_hours = data.get("US_STOCKS_ACTIVE_HOURS_IST")
        if not isinstance(us_stocks_hours, dict):
            us_stocks_hours = {"START": "18:30", "END": "23:59"}
        raw_us_stocks = data.get("US_STOCKS", [
            "NVDAXUSD", "AAPLXUSD", "GOOGLXUSD", "TSLAXUSD",
            "METAXUSD", "AMZNXUSD", "COINXUSD", "AMDBUSD", "MSTRBUSD"
        ])
        us_stocks_list = [str(s).upper() for s in raw_us_stocks] if isinstance(raw_us_stocks, list) else []

        enable_daily_audit = bool(data.get("ENABLE_DAILY_AUDIT_REPORT", True))
        daily_audit_time = str(data.get("DAILY_AUDIT_TIME_IST", "08:00")).strip()
        enable_weekly_audit = bool(data.get("ENABLE_WEEKLY_AUDIT_REPORT", True))
        weekly_audit_day = str(data.get("WEEKLY_AUDIT_DAY_IST", "SUN")).strip().upper()
        weekly_audit_time = str(data.get("WEEKLY_AUDIT_TIME_IST", "18:00")).strip()

        return {
            "SYMBOLS": [str(s).upper() for s in symbols],
            "TIMEFRAMES": valid_default_tf,
            "SYMBOL_TIMEFRAMES": sanitized_symbol_tf,
            "ALERT_COOLDOWN_MINUTES": cooldown,
            "BREAKOUT_MODE": breakout_mode,
            "MIN_BREAKOUT_PERCENT": min_breakout_pct,
            "VOLUME_CONFIRMATION_ENABLED": vol_confirm,
            "MIN_VOLUME_RATIO": min_vol_ratio,
            "ENABLE_QUALITY_FILTER": enable_quality_filter,
            "MIN_SETUP_SCORE": min_setup_score,
            "NTFY_TOPIC": ntfy_topic,
            "ENABLE_NTFY_ALERTS": enable_ntfy,
            "ENABLE_TRADE_TARGETS": enable_trade_targets,
            "ENABLE_TECHNICAL_METRICS": enable_technical_metrics,
            "ENABLE_RETEST_ALERTS": bool(data.get("ENABLE_RETEST_ALERTS", True)),
            "ALERT_ON_RETEST_ONLY": bool(data.get("ALERT_ON_RETEST_ONLY", false if "false" in str(data) else False)),
            "RETEST_TOLERANCE_PCT": float(data.get("RETEST_TOLERANCE_PCT", 0.40)),
            "ENABLE_LIFECYCLE_TRACKING": bool(data.get("ENABLE_LIFECYCLE_TRACKING", True)),
            "TARGET_1_RR": float(data.get("TARGET_1_RR", 1.5)),
            "TARGET_2_RR": float(data.get("TARGET_2_RR", 3.0)),
            "ENABLE_OI_METRICS": bool(data.get("ENABLE_OI_METRICS", True)),
            "ENABLE_CHART_SNAPSHOTS": bool(data.get("ENABLE_CHART_SNAPSHOTS", True)),
            "SCHEDULE_ENABLED": schedule_enabled,
            "OPERATING_SCHEDULE_IST": raw_schedule,
            "US_STOCKS_FILTER_ENABLED": us_stocks_filter,
            "US_STOCKS_ACTIVE_HOURS_IST": us_stocks_hours,
            "US_STOCKS": us_stocks_list,
            "ENABLE_DAILY_AUDIT_REPORT": enable_daily_audit,
            "DAILY_AUDIT_TIME_IST": daily_audit_time,
            "ENABLE_WEEKLY_AUDIT_REPORT": enable_weekly_audit,
            "WEEKLY_AUDIT_DAY_IST": weekly_audit_day,
            "WEEKLY_AUDIT_TIME_IST": weekly_audit_time,
            "TELEGRAM_BOT_TOKEN": str(os.environ.get("TELEGRAM_BOT_TOKEN") or data.get("TELEGRAM_BOT_TOKEN", "")),
            "TELEGRAM_CHAT_ID": str(os.environ.get("TELEGRAM_CHAT_ID") or data.get("TELEGRAM_CHAT_ID", "")),
            "DELTA_REST_URL": str(data.get("DELTA_REST_URL", "https://api.india.delta.exchange")),
            "DELTA_WS_URL": str(data.get("DELTA_WS_URL", "wss://public-socket.india.delta.exchange")),
            "LOG_FILE_PATH": (
                "app.log"
                if sys.platform != "win32" and ("ProgramData" in str(data.get("LOG_FILE_PATH", "")) or str(data.get("LOG_FILE_PATH", "")).startswith("C:"))
                else str(data.get("LOG_FILE_PATH", r"C:\ProgramData\DeltaAlerts\app.log"))
            ),
            "HEARTBEAT_INTERVAL_HOURS": int(data.get("HEARTBEAT_INTERVAL_HOURS", 12)),
            "REST_CONCURRENCY_LIMIT": int(data.get("REST_CONCURRENCY_LIMIT", 5)),
            "CONFIG_CHECK_INTERVAL_SECONDS": int(data.get("CONFIG_CHECK_INTERVAL_SECONDS", 30)),
        }

    def _set_defaults(self):
        """Sets internal default values."""
        self.config_data = {
            "SYMBOLS": ["BTCUSD", "ETHUSD"],
            "TIMEFRAMES": ["1h", "4h"],
            "SYMBOL_TIMEFRAMES": {},
            "ALERT_COOLDOWN_MINUTES": 5,
            "TELEGRAM_BOT_TOKEN": "",
            "TELEGRAM_CHAT_ID": "",
            "DELTA_REST_URL": "https://api.india.delta.exchange",
            "DELTA_WS_URL": "wss://public-socket.india.delta.exchange",
            "LOG_FILE_PATH": r"C:\ProgramData\DeltaAlerts\app.log",
            "HEARTBEAT_INTERVAL_HOURS": 12,
            "REST_CONCURRENCY_LIMIT": 5,
            "CONFIG_CHECK_INTERVAL_SECONDS": 30,
        }

    @property
    def symbols(self) -> List[str]:
        return self.config_data.get("SYMBOLS", [])

    @property
    def timeframes(self) -> List[str]:
        return self.config_data.get("TIMEFRAMES", [])

    @property
    def symbol_timeframes(self) -> Dict[str, List[str]]:
        return self.config_data.get("SYMBOL_TIMEFRAMES", {})

    def get_timeframes_for_symbol(self, symbol: str) -> List[str]:
        """Returns the specific list of timeframes configured for a symbol, falling back to global TIMEFRAMES."""
        sym_upper = symbol.upper()
        if sym_upper in self.symbol_timeframes:
            return self.symbol_timeframes[sym_upper]
        return self.timeframes

    def get_active_pairs(self) -> List[tuple[str, str]]:
        """Returns a list of all active (symbol, timeframe) pairs."""
        pairs = []
        for symbol in self.symbols:
            tfs = self.get_timeframes_for_symbol(symbol)
            for tf in tfs:
                pairs.append((symbol, tf))
        return pairs

    def get_all_active_timeframes(self) -> List[str]:
        """Returns a unique sorted list of all timeframes active across any symbol."""
        active_tfs = set()
        for symbol in self.symbols:
            for tf in self.get_timeframes_for_symbol(symbol):
                active_tfs.add(tf)
        return sorted(list(active_tfs), key=lambda x: parse_timeframe_to_seconds(x))

    @property
    def alert_cooldown_minutes(self) -> int:
        return self.config_data.get("ALERT_COOLDOWN_MINUTES", 5)

    @property
    def breakout_mode(self) -> str:
        return self.config_data.get("BREAKOUT_MODE", "INSTANT")

    @property
    def min_breakout_percent(self) -> float:
        return self.config_data.get("MIN_BREAKOUT_PERCENT", 0.05)

    @property
    def volume_confirmation_enabled(self) -> bool:
        return self.config_data.get("VOLUME_CONFIRMATION_ENABLED", True)

    @property
    def min_volume_ratio(self) -> float:
        return self.config_data.get("MIN_VOLUME_RATIO", 0.70)

    @property
    def enable_quality_filter(self) -> bool:
        return self.config_data.get("ENABLE_QUALITY_FILTER", True)

    @property
    def min_setup_score(self) -> int:
        return self.config_data.get("MIN_SETUP_SCORE", 70)

    @property
    def ntfy_topic(self) -> str:
        return self.config_data.get("NTFY_TOPIC", "delta_breakout_ravi")

    @property
    def enable_ntfy_alerts(self) -> bool:
        return self.config_data.get("ENABLE_NTFY_ALERTS", True)

    @property
    def enable_trade_targets(self) -> bool:
        return self.config_data.get("ENABLE_TRADE_TARGETS", False)

    @property
    def enable_technical_metrics(self) -> bool:
        return self.config_data.get("ENABLE_TECHNICAL_METRICS", True)

    @property
    def schedule_enabled(self) -> bool:
        return self.config_data.get("SCHEDULE_ENABLED", True)

    @property
    def operating_schedule_ist(self) -> Dict[str, Any]:
        return self.config_data.get("OPERATING_SCHEDULE_IST", {})

    def is_within_operating_schedule(self, reference_time: Optional[float] = None) -> bool:
        """Checks if the system is currently within active operating schedule hours (IST)."""
        if not self.schedule_enabled:
            return True

        schedule = self.operating_schedule_ist
        if not schedule or not isinstance(schedule, dict):
            return True

        now = reference_time if reference_time is not None else time.time()
        day_abbr = time.strftime("%a", time.localtime(now)).upper()

        day_ranges = schedule.get(day_abbr, [])
        if not day_ranges or not isinstance(day_ranges, list):
            return False

        current_hm = time.strftime("%H:%M", time.localtime(now))

        for r in day_ranges:
            if isinstance(r, dict):
                start_time = str(r.get("START", "00:00"))
                end_time = str(r.get("END", "23:59"))
                if start_time <= current_hm <= end_time:
                    return True

    @property
    def us_stocks_filter_enabled(self) -> bool:
        return self.config_data.get("US_STOCKS_FILTER_ENABLED", True)

    @property
    def us_stocks_active_hours_ist(self) -> Dict[str, str]:
        return self.config_data.get("US_STOCKS_ACTIVE_HOURS_IST", {"START": "18:30", "END": "23:59"})

    @property
    def us_stocks(self) -> List[str]:
        return self.config_data.get("US_STOCKS", [
            "NVDAXUSD", "AAPLXUSD", "GOOGLXUSD", "TSLAXUSD",
            "METAXUSD", "AMZNXUSD", "COINXUSD", "AMDBUSD", "MSTRBUSD"
        ])

    def is_symbol_allowed_at_time(self, symbol: str, reference_time: Optional[float] = None) -> bool:
        """Checks if a symbol is allowed to issue alerts based on global operating schedule & US stock active hours."""
        if not self.is_within_operating_schedule(reference_time):
            return False

        sym_upper = symbol.upper()
        if self.us_stocks_filter_enabled and sym_upper in [s.upper() for s in self.us_stocks]:
            now = reference_time if reference_time is not None else time.time()
            current_hm = time.strftime("%H:%M", time.localtime(now))
            start_time = str(self.us_stocks_active_hours_ist.get("START", "18:30"))
            end_time = str(self.us_stocks_active_hours_ist.get("END", "23:59"))

            if start_time <= end_time:
                if not (start_time <= current_hm <= end_time):
                    return False
            else:
                if not (current_hm >= start_time or current_hm <= end_time):
                    return False

        return True

    @property
    def telegram_bot_token(self) -> str:
        return os.environ.get("TELEGRAM_BOT_TOKEN") or self.config_data.get("TELEGRAM_BOT_TOKEN", "")

    @property
    def telegram_chat_id(self) -> str:
        return os.environ.get("TELEGRAM_CHAT_ID") or self.config_data.get("TELEGRAM_CHAT_ID", "")

    @property
    def delta_rest_url(self) -> str:
        return self.config_data.get("DELTA_REST_URL", "https://api.india.delta.exchange")

    @property
    def delta_ws_url(self) -> str:
        return self.config_data.get("DELTA_WS_URL", "wss://public-socket.india.delta.exchange")

    @property
    def log_file_path(self) -> str:
        path = self.config_data.get("LOG_FILE_PATH", r"C:\ProgramData\DeltaAlerts\app.log")
        if sys.platform != "win32" and ("ProgramData" in path or path.startswith("C:")):
            return "app.log"
        return path

    @property
    def heartbeat_interval_hours(self) -> int:
        return self.config_data.get("HEARTBEAT_INTERVAL_HOURS", 12)

    @property
    def rest_concurrency_limit(self) -> int:
        return self.config_data.get("REST_CONCURRENCY_LIMIT", 5)

    @property
    def config_check_interval_seconds(self) -> int:
        return self.config_data.get("CONFIG_CHECK_INTERVAL_SECONDS", 30)

    @property
    def enable_daily_audit_report(self) -> bool:
        return self.config_data.get("ENABLE_DAILY_AUDIT_REPORT", True)

    @property
    def daily_audit_time_ist(self) -> str:
        return self.config_data.get("DAILY_AUDIT_TIME_IST", "08:00")

    @property
    def enable_weekly_audit_report(self) -> bool:
        return self.config_data.get("ENABLE_WEEKLY_AUDIT_REPORT", True)

    @property
    def weekly_audit_day_ist(self) -> str:
        return self.config_data.get("WEEKLY_AUDIT_DAY_IST", "SUN")

    @property
    def enable_retest_alerts(self) -> bool:
        return self.config_data.get("ENABLE_RETEST_ALERTS", True)

    @property
    def alert_on_retest_only(self) -> bool:
        return self.config_data.get("ALERT_ON_RETEST_ONLY", False)

    @property
    def retest_tolerance_pct(self) -> float:
        return self.config_data.get("RETEST_TOLERANCE_PCT", 0.40)

    @property
    def enable_lifecycle_tracking(self) -> bool:
        return self.config_data.get("ENABLE_LIFECYCLE_TRACKING", True)

    @property
    def target_1_rr(self) -> float:
        return self.config_data.get("TARGET_1_RR", 1.5)

    @property
    def target_2_rr(self) -> float:
        return self.config_data.get("TARGET_2_RR", 3.0)

    @property
    def enable_oi_metrics(self) -> bool:
        return self.config_data.get("ENABLE_OI_METRICS", True)

    @property
    def enable_chart_snapshots(self) -> bool:
        return self.config_data.get("ENABLE_CHART_SNAPSHOTS", True)

    @property
    def weekly_audit_time_ist(self) -> str:
        return self.config_data.get("WEEKLY_AUDIT_TIME_IST", "18:00")
