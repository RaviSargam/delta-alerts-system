# Delta Exchange Breakout Alert System - AI Agent Context & Memory Guide

This file provides comprehensive context for AI Coding Assistants (Antigravity, Gemini, etc.) when loading this codebase on any machine.

---

## 📌 Project Overview
A production-grade, highly resilient asynchronous Python application monitoring breakout price levels (Previous High/Low boundaries) on **Delta Exchange India** (`api.india.delta.exchange`). It dispatches rich, instant notifications to Telegram and ntfy, running as an unattended background Windows Service.

- **Monitored Symbols (12)**: `BTCUSD`, `ETHUSD`, `XAUTUSD`, `NVDAXUSD`, `AAPLXUSD`, `GOOGLXUSD`, `TSLAXUSD`, `METAXUSD`, `AMZNXUSD`, `COINXUSD`, `AMDBUSD`, `MSTRBUSD`
- **Active Symbol-Timeframe Pairs**: 25 configured pairs
- **Service Name**: `DeltaCryptoAlerts` (Windows Background Service managed via NSSM)

---

## ⚙️ Core Technical Rules & Configuration Summary

### 1. Volume Confirmation & Breakout Filtering
- `"VOLUME_CONFIRMATION_ENABLED": true`
- `"MIN_VOLUME_RATIO": 0.20` (Volume ratio threshold: current candle volume must be ≥ 20% of 20-period average volume).

### 2. Alert Cooldown & Peak-Price Continuation Logic
- `"ALERT_COOLDOWN_MINUTES": 15` (15-minute time window).
- **Disk Persistence**: `cooldown_state.json` saves `_alerted_boundaries`, `_last_alert_time`, and `_last_alert_price` to disk so alert states survive service restarts without duplicate alert bursts.
- **Intraday Peak-Price Continuation Verification**: During an active candle period, repeat alerts are suppressed unless the current price makes a **new higher high** (for Bullish) or **new lower low** (for Bearish) relative to `last_alert_price`.

### 3. Time Scheduling & Operational Windows
- **Global Operating Schedule** (`SCHEDULE_ENABLED: true`):
  - Monday – Friday: 24 Hours Active (`00:00 - 23:59 IST`)
  - Saturday: Quiet / Disabled (0 Volume)
  - Sunday: Active after 18:00 IST (`18:00 - 23:59 IST`)
- **US Stock Token Window** (`US_STOCKS_FILTER_ENABLED: true`):
  - US Stocks (`NVDAXUSD`, `AAPLXUSD`, `GOOGLXUSD`, `TSLAXUSD`, `METAXUSD`, `AMZNXUSD`, `COINXUSD`, `AMDBUSD`, `MSTRBUSD`) are restricted to **18:30 to 23:59 IST** (US active market hours).
  - Crypto (`BTCUSD`, `ETHUSD`) and Gold (`XAUTUSD`) operate 24/7 per global schedule.

### 4. Notification Toggles
- `"ENABLE_TRADE_TARGETS": false` (Trade Targets TP1/TP2/SL are hidden).
- `"ENABLE_TECHNICAL_METRICS": true` (RSI, 20 EMA Trend, Volume Ratio, ATR, and Consolidation Squeeze are included in Telegram alerts).

### 5. Automated Breakout Performance Auditor
- `alert_auditor.py`: Parses historical alert logs (`app.log*`) and queries Delta Exchange REST API to compute real win rate, Max Favorable Excursion (peak profit %), Max Adverse Excursion (drawdown), fakeout rate, and setup quality score correlation.
- **On-Demand Telegram Commands**:
  - `/audit` or `/audit 24h`: Audits last 24 hours.
  - `/audit 7d`: Audits last 7 days.
  - `/audit 30d`: Audits last 30 days.
  - `/audit all`: Audits all-time alert history.
  - `/check [symbol]`: Checks live High/Low boundaries on demand.
- **Automated Audit Scheduling**:
  - `"ENABLE_DAILY_AUDIT_REPORT": true` at `"DAILY_AUDIT_TIME_IST": "08:00"`.
  - `"ENABLE_WEEKLY_AUDIT_REPORT": true` on `"WEEKLY_AUDIT_DAY_IST": "SUN"` at `"WEEKLY_AUDIT_TIME_IST": "18:00"`.

### 6. Breakout Retest & Bounce Confirmation
- `RetestTracker` in `state_manager.py`: Monitors breakouts for price pullbacks to the broken boundary level within a configured tolerance (`"RETEST_TOLERANCE_PCT": 0.40`). When price bounces away in the breakout direction, a high-confidence Retest Confirmed notification is dispatched.
- `"ENABLE_RETEST_ALERTS": true`
- `"ALERT_ON_RETEST_ONLY": false` (Set to `true` to only receive retest alerts and skip initial wicks).

### 7. Live Trade Lifecycle Tracking (TP1 / TP2 / SL Hits)
- `LifecycleTracker` in `state_manager.py`: Tracks all active trade setups in real-time.
- Automatically sends Telegram follow-up notifications when Target 1 (TP1) is reached, Target 2 (TP2) is reached, or when the setup invalidates at Stop-Loss (SL).
- `"ENABLE_LIFECYCLE_TRACKING": true`
- `"TARGET_1_RR": 1.5`, `"TARGET_2_RR": 3.0`

### 8. Live Open Interest (OI) & Funding Rate Confluence
- Directly queries Delta Exchange Ticker API (`/v2/tickers/{symbol}`) to calculate 6-hour OI USD changes and funding rate.
- Categorizes sentiment: Institutional Inflow (OI surge > 2%), Short Squeeze Trap (OI drop < -2%), or Steady.
- Included seamlessly in Telegram Technical Metrics.
- `"ENABLE_OI_METRICS": true`

### 9. Dark-Themed Candlestick Chart Snapshots
- `chart_generator.py`: Generates ultra-lightweight in-memory PNG candlestick charts (~35 KB) via Matplotlib highlighting the broken boundary, 20 EMA, and volume bars.
- Sent directly with Telegram alerts via `sendPhoto` (< 1% of Render monthly bandwidth).
- `"ENABLE_CHART_SNAPSHOTS": true`

---

## 📁 Key File Structure
- `main.py`: Main event loop, REST sync engine, persistent WebSocket listener, breakout evaluator, Telegram dispatcher, audit scheduler, and command listener.
- `alert_auditor.py`: Historical alert performance evaluator, Delta candle analyzer, and Telegram audit report generator.
- `config.py`: Configuration manager (`ConfigManager`), schema sanitizer, IST time range validators, chart URL builder, and timeframe math.
- `config.json`: Live system configuration file (hot-reloaded every 30s).
- `state_manager.py`: `BoundaryState`, `CooldownTracker` (with JSON file persistence), `HealthMonitor`, `MuteState`, and `ConfluenceTracker`.
- `ta_engine.py`: Technical indicators (RSI, 20 EMA, Volume Ratio, ATR, Consolidation Squeeze, Setup Quality Score). Note: HTML tags (`<` & `>`) in text outputs are escaped as `&lt;` and `&gt;` for Telegram compatibility.
- `install_service.bat`: Automated administrator setup script for NSSM service registration.
- `uninstall_service.bat`: Stops and uninstalls the Windows Service.
- `render.yaml`: 1-click Render blueprint specification for cloud 24/7 deployment.
- `Dockerfile`: Container build recipe for cloud deployment.

---

## 🛠️ Service Management Commands
To manage the service on Windows:
```cmd
:: Check Service Status
sc query DeltaCryptoAlerts

:: Restart Service (Run as Administrator)
net stop DeltaCryptoAlerts && net start DeltaCryptoAlerts

:: Check Application Logs
type C:\ProgramData\DeltaAlerts\app.log
```

---

## 🚀 Transferring to a New Laptop
When moving this project folder to a new laptop:
1. Copy or git clone the `Delta Exchange Alerts System` folder to the new laptop.
2. Install Python dependencies: `py -m pip install -r requirements.txt`.
3. Open PowerShell/CMD as **Administrator** and run `install_service.bat` to set up the background Windows Service on the new machine.
4. Open the project directory in Antigravity IDE. Antigravity will automatically read this `AGENTS.md` file and resume with complete context of all features, schedules, and code logic!
