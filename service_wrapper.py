"""
Windows Service Wrapper using pywin32.
Allows the Delta Exchange Breakout System to run continuous, unattended
background execution in Windows Service Control Manager.
"""

import sys
import os
import asyncio
import logging

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    PYWIN32_AVAILABLE = True
except ImportError:
    PYWIN32_AVAILABLE = False


if PYWIN32_AVAILABLE:
    class DeltaCryptoAlertsService(win32serviceutil.ServiceFramework):
        _svc_name_ = "DeltaCryptoAlerts"
        _svc_display_name_ = "Delta Exchange Crypto Breakout Alerts System"
        _svc_description_ = (
            "Continuous background monitoring of cryptocurrency candlestick breakouts "
            "on Delta Exchange India with instant Telegram alerts."
        )

        def __init__(self, args):
            super().__init__(args)
            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
            self.stop_requested = False

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.hWaitStop)
            self.stop_requested = True

        def SvcDoRun(self):
            app_dir = os.path.dirname(os.path.abspath(__file__))
            os.chdir(app_dir)
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            self.main()

        def main(self):
            app_dir = os.path.dirname(os.path.abspath(__file__))
            os.chdir(app_dir)
            import main as app_main
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def run_until_stop():
                main_task = loop.create_task(app_main.async_main())
                while not self.stop_requested:
                    # Check stop signal every 1 second
                    rc = win32event.WaitForSingleObject(self.hWaitStop, 1000)
                    if rc == win32event.WAIT_OBJECT_0:
                        break
                main_task.cancel()
                try:
                    await main_task
                except asyncio.CancelledError:
                    pass

            try:
                loop.run_until_complete(run_until_stop())
            finally:
                loop.close()


if __name__ == "__main__":
    if PYWIN32_AVAILABLE:
        win32serviceutil.HandleCommandLine(DeltaCryptoAlertsService)
    else:
        print("pywin32 is not installed. Run 'pip install pywin32' to enable Windows Service support.")
