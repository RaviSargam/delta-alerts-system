# Delta Exchange India Crypto Breakout Alert System & Windows Service

A production-grade, highly resilient asynchronous Python application designed to monitor cryptocurrency candlestick breakouts in real-time on Delta Exchange India (`api.india.delta.exchange`) and dispatch instant, rich notifications to Telegram, running as an unattended background Windows Service.

---

## Architecture Highlights

1. **High Scalability & Zero Hardcoding**:
   - Symbols and timeframes are managed strictly in `config.json`.
   - Supports scalable timeframes: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `6h`, `1d`.
   - Supports any valid Delta India trading symbol array (e.g., `["BTCINR", "ETHINR"]`).

2. **Dual Synchronized Data Pipeline**:
   - **REST Sync**: Historical OHLC boundaries are fetched on service startup and at every candle close completion via `GET /v2/history/candles`.
   - **WebSocket Stream**: Single persistent WebSocket connection (`wss://public-socket.india.delta.exchange`) listening to live ticker/trade updates for all symbols.

3. **Breakout Detection & Cooldown Lock**:
   - Triggers when `Live Traded Price > Previous Candle High` (Bullish) or `Live Traded Price < Previous Candle Low` (Bearish).
   - Cooldown locking per `(Symbol, Timeframe, Direction)` for `ALERT_COOLDOWN_MINUTES` to eliminate tick-by-tick message spam.

4. **Production Enhancements**:
   - **Config Hot-Reloading**: Automatically reloads `config.json` every 30 seconds without restarting the Windows Service.
   - **Periodic Telegram Heartbeat**: Periodic status updates reporting uptime, WS state, message counts, and alert counts.
   - **WS Keep-Alive Ping/Pong**: Prevents socket drops during network sleep or idle states.
   - **REST Rate Limiter**: Async Semaphore concurrency control for API boundary syncing.
   - **Rotating Logger**: Configured to `C:\ProgramData\DeltaAlerts\app.log` (10MB max, 5 rotation backups).

---

## File Structure

- `config.json`: Production configuration settings.
- `config.py`: Timeframe duration math, next candle close calculation, config loader, and chart link generator.
- `state_manager.py`: Thread-safe boundary dictionary, cooldown locks, and system health counters.
- `alert_auditor.py`: Historical alert performance evaluator, Delta candle analyzer, and Telegram audit report generator.
- `main.py`: Core asynchronous engine (REST fetcher, WS stream, breakout evaluator, Telegram poster, task schedulers).
- `service_wrapper.py`: PyWin32 Windows Service integration wrapper.
- `install_service.bat`: Automated administrator script to register and start the Windows Service.
- `uninstall_service.bat`: Helper script to stop and remove the Windows Service.
- `requirements.txt`: Python package dependencies (`aiohttp`, `websockets`, `pywin32`).

---

## Configuration Setup

Edit `config.json` before starting the service:

```json
{
  "SYMBOLS": [
    "BTCINR",
    "ETHINR"
  ],
  "TIMEFRAMES": [
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "4h",
    "6h",
    "1d"
  ],
  "ALERT_COOLDOWN_MINUTES": 5,
  "TELEGRAM_BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN_HERE",
  "TELEGRAM_CHAT_ID": "YOUR_TELEGRAM_CHAT_ID_HERE",
  "DELTA_REST_URL": "https://api.india.delta.exchange",
  "DELTA_WS_URL": "wss://public-socket.india.delta.exchange",
  "LOG_FILE_PATH": "C:\\ProgramData\\DeltaAlerts\\app.log",
  "HEARTBEAT_INTERVAL_HOURS": 12,
  "REST_CONCURRENCY_LIMIT": 5,
  "CONFIG_CHECK_INTERVAL_SECONDS": 30
}
```

---

## Installation & Deployment as a Windows Service

1. Open **Command Prompt** or **PowerShell** as **Administrator**.
2. Navigate to project directory:
   ```cmd
   cd /d "c:\Delta Exchange Alerts System"
   ```
3. Run `install_service.bat`:
   ```cmd
   install_service.bat
   ```
4. The script will automatically install dependencies, create `C:\ProgramData\DeltaAlerts`, register `DeltaCryptoAlerts` as an Automatic Windows Service, and start execution.

---

## Manual / Interactive Testing

To test execution interactively in your terminal before running as a service:
```cmd
python main.py
```

To stop or uninstall the Windows Service:
```cmd
uninstall_service.bat
```
