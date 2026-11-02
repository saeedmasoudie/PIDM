from __future__ import annotations

import platform
import subprocess

from PySide6.QtCore import QCoreApplication, QObject, QTimer, Signal, Slot


class SystemActions(QObject):
    actionStarted = Signal(str)
    errorOccurred = Signal(str)

    @Slot(str, str)
    def execute(self, action: str, queue_name: str) -> None:
        if action == "none":
            return
        if action == "exit":
            self.actionStarted.emit(
                f"Queue '{queue_name}' finished. PIDM will close in 10 seconds."
            )
            QTimer.singleShot(10_000, QCoreApplication.quit)
            return
        if action != "shutdown":
            return
        try:
            system = platform.system()
            if system == "Windows":
                command = [
                    "shutdown", "/s", "/t", "60", "/c",
                    f"PIDM queue '{queue_name}' completed.",
                ]
            elif system == "Darwin":
                command = [
                    "osascript", "-e",
                    'tell application "System Events" to shut down',
                ]
            else:
                command = ["shutdown", "-h", "+1"]
            subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.actionStarted.emit(
                f"Queue '{queue_name}' finished. System shutdown was requested."
            )
        except OSError as exc:
            self.errorOccurred.emit(f"Could not request system shutdown: {exc}")
