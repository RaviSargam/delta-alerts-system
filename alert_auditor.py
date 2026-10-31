"""
Alert Performance Auditor & Backtest Engine for Delta Exchange India Breakout System.
Analyzes historical alerts from application logs, evaluates subsequent price action via Delta REST API,
and computes Win Rate, Max Profit Potential (MFE), Fakeout/Reversal Rate, and Setup Score Accuracy.
"""

import asyncio
import datetime
import glob
import logging
import os
import re
import time
from typing import Dict, List, Optional, Any
import aiohttp

from config import ConfigManager, parse_timeframe_to_seconds, normalize_symbol

logger = logging.getLogger("DeltaAlerts.Auditor")

ALERT_LOG_REGEX = re.compile(
    r"\[(?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\].*?ALERT:\s+(?P<dir>Bullish|Bearish)\s+Breakout\s+\[(?P<sym>[A-Z0-9]+)\s+(?P<tf>[a-z0-9]+)\]\s+Price\s+(?P<price>[\d\.]+)\s+[><]\s+(?:High|Low)\s+(?P<level>[\d\.]+)\s+\((?P<pct>[+-]?[\d\.]+)%\)(?:\s+Score=(?P<score>\d+))?"
)


class AlertAuditor:
    """Evaluates the market accuracy of triggered breakout alerts against actual Delta Exchange candle data."""

    def __init__(self, config: ConfigManager):
        self.config = config
        self.candle_cache: Dict[str, List[Dict[str, Any]]] = {}

    def extract_alerts_from_logs(self, hours: Optional[int] = 24) -> List[Dict[str, Any]]:
        """
        Parses all alerts from log files within the specified lookback window (in hours).
        If hours is None, extracts all available historical alerts.
        """
        log_dir = os.path.dirname(self.config.log_file_path) or "."
        log_files = glob.glob(os.path.join(log_dir, "app.log*"))
        if not log_files:
            log_files = [self.config.log_file_path]

        now = time.time()
        cutoff_epoch = (now - (hours * 3600)) if hours is not None else 0

        raw_alerts = []
        for file_path in log_files:
            if not os.path.isfile(file_path):
                continue
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as fp:
                    for line in fp:
                        m = ALERT_LOG_REGEX.search(line)
                        if m:
                            d = m.groupdict()
                            try:
                                dt_obj = datetime.datetime.strptime(d["dt"], "%Y-%m-%d %H:%M:%S")
                                epoch = dt_obj.timestamp()
                                if epoch < cutoff_epoch:
                                    continue
                                raw_alerts.append({
                                    "dt": d["dt"],
                                    "epoch": epoch,
                                    "dir": d["dir"],
                                    "sym": d["sym"],
                                    "tf": d["tf"],
                                    "price": float(d["price"]),
                                    "level": float(d["level"]),
                                    "score": int(d["score"]) if d.get("score") else 50,
                                })
                            except Exception:
                                continue
            except Exception as e:
                logger.error(f"Error reading log file {file_path} for audit: {e}")

        # Deduplicate same breakout within 5 minutes (same continuation wave)
        raw_alerts.sort(key=lambda x: x["epoch"])
        deduped: List[Dict[str, Any]] = []
        last_a = None
        for a in raw_alerts:
            if (
                last_a
                and a["sym"] == last_a["sym"]
                and a["tf"] == last_a["tf"]
                and a["dir"] == last_a["dir"]
                and abs(a["epoch"] - last_a["epoch"]) < 300
            ):
                continue
            deduped.append(a)
            last_a = a

        return deduped

    async def fetch_candles_for_symbol(
        self, session: aiohttp.ClientSession, symbol: str, timeframe: str, start_ts: int, end_ts: int
    ) -> List[Dict[str, Any]]:
        """Fetches candles from Delta Exchange with caching."""
        cache_key = f"{symbol}_{timeframe}_{start_ts // 86400}_{end_ts // 86400}"
        if cache_key in self.candle_cache:
            return self.candle_cache[cache_key]

        base_url = self.config.delta_rest_url.rstrip("/")
        endpoint = f"{base_url}/v2/history/candles"
        params = {
            "symbol": normalize_symbol(symbol),
            "resolution": timeframe,
            "start": str(int(start_ts)),
            "end": str(int(end_ts)),
        }

        try:
            async with session.get(endpoint, params=params, timeout=12) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = data.get("result", [])
                    if isinstance(candles, list):
                        candles.sort(key=lambda x: int(x.get("time", 0)))
                        self.candle_cache[cache_key] = candles
                        return candles
        except Exception as e:
            logger.debug(f"Audit candle fetch failed [{symbol} {timeframe}]: {e}")

        return []

    async def evaluate_alerts(self, alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Evaluates price performance and outcomes for a list of alerts."""
        if not alerts:
            return {
                "total_alerts": 0,
                "completed_alerts": 0,
                "active_alerts": 0,
                "wins": 0,
                "fakeouts": 0,
                "win_rate": 0.0,
                "fakeout_rate": 0.0,
                "avg_mfe": 0.0,
                "avg_mae": 0.0,
                "best_performer": None,
                "worst_performer": None,
                "high_score_win_rate": 0.0,
                "low_score_win_rate": 0.0,
                "symbol_stats": {},
            }

        now = time.time()
        evaluated_records = []

        async with aiohttp.ClientSession() as session:
            for a in alerts:
                tf_sec = parse_timeframe_to_seconds(a["tf"])
                alert_epoch = a["epoch"]

                # Fetch candles from alert candle start up to 2-3 candles forward
                start_fetch = alert_epoch - tf_sec
                end_fetch = alert_epoch + (tf_sec * 3)

                candles = await self.fetch_candles_for_symbol(session, a["sym"], a["tf"], start_fetch, end_fetch)

                # Relevant candles covering or after alert
                relevant = [c for c in candles if int(c.get("time", 0)) + tf_sec >= alert_epoch]
                if not relevant:
                    continue

                breakout_candle = relevant[0]
                candle_close_time = int(breakout_candle.get("time", 0)) + tf_sec

                # If current candle is still active
                if now < candle_close_time:
                    evaluated_records.append({
                        **a,
                        "status": "ACTIVE",
                        "win": None,
                        "fakeout": False,
                        "mfe": 0.0,
                        "mae": 0.0,
                    })
                    continue

                # Evaluation window: breakout candle + next candle (up to 2 candles)
                eval_candles = relevant[:2]
                max_h = max(float(c.get("high", 0)) for c in eval_candles)
                min_l = min(float(c.get("low", 0)) for c in eval_candles)
                final_close = float(eval_candles[-1].get("close", 0))

                if a["dir"] == "Bullish":
                    mfe = ((max_h - a["price"]) / a["price"]) * 100.0
                    mae = ((a["price"] - min_l) / a["price"]) * 100.0
                    # Win: Peak move was at least +0.3% and candle closed at or above broken level
                    win = mfe >= 0.3 and final_close >= a["level"]
                    fakeout = final_close < a["level"]
                else:
                    mfe = ((a["price"] - min_l) / a["price"]) * 100.0
                    mae = ((max_h - a["price"]) / a["price"]) * 100.0
                    # Win: Peak move was at least +0.3% and candle closed at or below broken level
                    win = mfe >= 0.3 and final_close <= a["level"]
                    fakeout = final_close > a["level"]

                evaluated_records.append({
                    **a,
                    "status": "CLOSED",
                    "win": win,
                    "fakeout": fakeout,
                    "mfe": mfe,
                    "mae": mae,
                    "final_close": final_close,
                })

        # Aggregation
        completed = [r for r in evaluated_records if r["status"] == "CLOSED"]
        active = [r for r in evaluated_records if r["status"] == "ACTIVE"]

        total_completed = len(completed)
        wins = sum(1 for r in completed if r["win"] is True)
        fakeouts = sum(1 for r in completed if r["fakeout"] is True)

        win_rate = (wins / total_completed * 100.0) if total_completed > 0 else 0.0
        fakeout_rate = (fakeouts / total_completed * 100.0) if total_completed > 0 else 0.0
        avg_mfe = (sum(r["mfe"] for r in completed) / total_completed) if total_completed > 0 else 0.0
        avg_mae = (sum(r["mae"] for r in completed) / total_completed) if total_completed > 0 else 0.0

        # Score performance breakdown (Score >= 70 vs Score < 70)
        high_score_completed = [r for r in completed if r["score"] >= 70]
        low_score_completed = [r for r in completed if r["score"] < 70]

        high_score_wins = sum(1 for r in high_score_completed if r["win"] is True)
        low_score_wins = sum(1 for r in low_score_completed if r["win"] is True)

        high_score_win_rate = (
            (high_score_wins / len(high_score_completed) * 100.0) if high_score_completed else 0.0
        )
        low_score_win_rate = (
            (low_score_wins / len(low_score_completed) * 100.0) if low_score_completed else 0.0
        )

        # Best / Worst performers by MFE
        best_performer = max(completed, key=lambda x: x["mfe"]) if completed else None
        worst_performer = max(completed, key=lambda x: x["mae"]) if completed else None

        # Per-symbol breakdown
        sym_stats: Dict[str, Dict[str, Any]] = {}
        for r in completed:
            sym = r["sym"]
            if sym not in sym_stats:
                sym_stats[sym] = {"total": 0, "wins": 0, "fakeouts": 0, "mfes": []}
            sym_stats[sym]["total"] += 1
            if r["win"]:
                sym_stats[sym]["wins"] += 1
            if r["fakeout"]:
                sym_stats[sym]["fakeouts"] += 1
            sym_stats[sym]["mfes"].append(r["mfe"])

        for sym, d in sym_stats.items():
            d["win_rate"] = (d["wins"] / d["total"] * 100.0) if d["total"] > 0 else 0.0
            d["avg_mfe"] = (sum(d["mfes"]) / len(d["mfes"])) if d["mfes"] else 0.0

        return {
            "total_alerts": len(evaluated_records),
            "completed_alerts": total_completed,
            "active_alerts": len(active),
            "wins": wins,
            "fakeouts": fakeouts,
            "win_rate": win_rate,
            "fakeout_rate": fakeout_rate,
            "avg_mfe": avg_mfe,
            "avg_mae": avg_mae,
            "best_performer": best_performer,
            "worst_performer": worst_performer,
            "high_score_count": len(high_score_completed),
            "high_score_wins": high_score_wins,
            "high_score_win_rate": high_score_win_rate,
            "low_score_count": len(low_score_completed),
            "low_score_wins": low_score_wins,
            "low_score_win_rate": low_score_win_rate,
            "symbol_stats": sym_stats,
            "evaluated_records": evaluated_records,
        }

    async def run_audit(self, hours: Optional[int] = 24) -> Dict[str, Any]:
        """Full pipeline: extracts alerts, fetches candle data, and calculates metrics."""
        alerts = self.extract_alerts_from_logs(hours=hours)
        metrics = await self.evaluate_alerts(alerts)
        metrics["hours_window"] = hours
        return metrics

    def format_telegram_report(self, audit: Dict[str, Any], title_prefix: str = "BREAKOUT ACCURACY AUDIT") -> str:
        """Formats the audit metrics into a rich Telegram HTML report."""
        hours = audit.get("hours_window")
        if hours is None:
            time_label = "All Time"
        elif hours == 24:
            time_label = "Last 24 Hours"
        elif hours == 168:
            time_label = "Last 7 Days"
        elif hours == 720:
            time_label = "Last 30 Days"
        else:
            time_label = f"Last {hours} Hours"

        total = audit.get("total_alerts", 0)
        completed = audit.get("completed_alerts", 0)
        active = audit.get("active_alerts", 0)

        if total == 0:
            return (
                f"📊 <b>{title_prefix} ({time_label})</b> 📊\n\n"
                f"ℹ️ <i>No breakout alerts were recorded during this period.</i>\n\n"
                f"⏱ <b>Generated:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}"
            )

        win_rate = audit.get("win_rate", 0.0)
        fakeout_rate = audit.get("fakeout_rate", 0.0)
        avg_mfe = audit.get("avg_mfe", 0.0)
        avg_mae = audit.get("avg_mae", 0.0)

        # Health emoji
        rate_emoji = "🟢" if win_rate >= 60 else ("🟡" if win_rate >= 40 else "🔴")

        lines = [
            f"📊 <b>{title_prefix} ({time_label})</b> 📊\n",
            f"🎯 <b>Total Alerts Triggered:</b> {total}",
            f"✅ <b>Successful Breakouts:</b> {audit.get('wins', 0)} ({rate_emoji} {win_rate:.1f}% Win Rate)",
            f"❌ <b>Fakeouts / Traps:</b> {audit.get('fakeouts', 0)} ({fakeout_rate:.1f}%)",
        ]

        if active > 0:
            lines.append(f"⏳ <b>In Progress / Active:</b> {active} candle(s)")

        lines.append(
            f"\n📈 <b>PROFIT POTENTIAL METRICS</b>\n"
            f"• <b>Avg Peak Gain (MFE):</b> +{avg_mfe:.2f}%\n"
            f"• <b>Avg Drawdown (MAE):</b> -{avg_mae:.2f}%"
        )

        best = audit.get("best_performer")
        if best:
            lines.append(f"• <b>Best Run:</b> #{best['sym']} {best['tf']} (+{best['mfe']:.2f}% Peak Gain 🚀)")

        worst = audit.get("worst_performer")
        if worst:
            lines.append(f"• <b>Max Pullback:</b> #{worst['sym']} {worst['tf']} (-{worst['mae']:.2f}% Drawdown ⚠️)")

        # Setup Quality Score Verification
        h_cnt = audit.get("high_score_count", 0)
        l_cnt = audit.get("low_score_count", 0)
        if h_cnt > 0 or l_cnt > 0:
            lines.append(
                f"\n🧠 <b>SETUP QUALITY SCORE VALIDATION</b>\n"
                f"• <b>Score ≥ 70:</b> {audit.get('high_score_win_rate', 0.0):.1f}% Win Rate ({audit.get('high_score_wins', 0)}/{h_cnt})\n"
                f"• <b>Score &lt; 70:</b> {audit.get('low_score_win_rate', 0.0):.1f}% Win Rate ({audit.get('low_score_wins', 0)}/{l_cnt})"
            )

        # Top Performing Symbols (sorted by win rate, then total)
        sym_stats = audit.get("symbol_stats", {})
        if sym_stats:
            sorted_syms = sorted(sym_stats.items(), key=lambda x: (x[1]["win_rate"], x[1]["total"]), reverse=True)
            top_syms = sorted_syms[:3]
            sym_lines = [f"• #{sym}: {d['win_rate']:.0f}% Win Rate ({d['wins']}/{d['total']})" for sym, d in top_syms]
            lines.append(f"\n🏆 <b>TOP SYMBOLS</b>\n" + "\n".join(sym_lines))

        lines.append(f"\n⏱ <b>Generated:</b> {time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        return "\n".join(lines)
