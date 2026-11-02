from __future__ import annotations

import os
import platform
import plistlib
import shlex
import sys
from pathlib import Path

from PySide6.QtCore import QObject, Property, Signal


class AutostartController(QObject):
    changed = Signal()
    errorOccurred = Signal(str)

    def __init__(self, main_script: Path, parent=None):
        super().__init__(parent)
        self._main_script = main_script.resolve()

    def _arguments(self) -> list[str]:
        if getattr(sys, "frozen", False):
            return [str(Path(sys.executable).resolve())]
        executable = Path(sys.executable).resolve()
        if platform.system() == "Windows" and executable.name.lower() == "python.exe":
            pythonw = executable.with_name("pythonw.exe")
            if pythonw.is_file():
                executable = pythonw
        return [str(executable), str(self._main_script)]

    def _linux_path(self) -> Path:
        config_home = Path(
            os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")
        )
        return config_home / "autostart" / "pidm.desktop"

    @staticmethod
    def _mac_path() -> Path:
        return Path.home() / "Library" / "LaunchAgents" / "com.pidm.downloadmanager.plist"

    @Property(bool, constant=True)
    def supported(self) -> bool:
        return platform.system() in {"Windows", "Darwin", "Linux"}

    @Property(bool, notify=changed)
    def enabled(self) -> bool:
        system = platform.system()
        try:
            if system == "Windows":
                import winreg

                with winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion\Run",
                ) as key:
                    winreg.QueryValueEx(key, "PIDM")
                return True
            if system == "Darwin":
                return self._mac_path().is_file()
            if system == "Linux":
                return self._linux_path().is_file()
        except (FileNotFoundError, OSError):
            pass
        return False

    @enabled.setter
    def enabled(self, value: bool) -> None:
        try:
            self._set_enabled(bool(value))
        except OSError as exc:
            self.errorOccurred.emit(f"Could not update startup settings: {exc}")
        self.changed.emit()

    def _set_enabled(self, enabled: bool) -> None:
        system = platform.system()
        if system == "Windows":
            import winreg

            with winreg.CreateKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Run",
            ) as key:
                if enabled:
                    winreg.SetValueEx(
                        key,
                        "PIDM",
                        0,
                        winreg.REG_SZ,
                        subprocess_command(self._arguments()),
                    )
                else:
                    try:
                        winreg.DeleteValue(key, "PIDM")
                    except FileNotFoundError:
                        pass
            return
        if system == "Darwin":
            path = self._mac_path()
            if enabled:
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = {
                    "Label": "com.pidm.downloadmanager",
                    "ProgramArguments": self._arguments(),
                    "RunAtLoad": True,
                }
                with path.open("wb") as target:
                    plistlib.dump(payload, target)
            else:
                path.unlink(missing_ok=True)
            return
        if system == "Linux":
            path = self._linux_path()
            if enabled:
                path.parent.mkdir(parents=True, exist_ok=True)
                command = " ".join(desktop_quote(arg) for arg in self._arguments())
                path.write_text(
                    "[Desktop Entry]\n"
                    "Type=Application\n"
                    "Name=PIDM\n"
                    f"Exec={command}\n"
                    "Terminal=false\n"
                    "X-GNOME-Autostart-enabled=true\n",
                    encoding="utf-8",
                )
            else:
                path.unlink(missing_ok=True)


def subprocess_command(arguments: list[str]) -> str:
    if platform.system() == "Windows":
        import subprocess

        return subprocess.list2cmdline(arguments)
    return shlex.join(arguments)


def desktop_quote(argument: str) -> str:
    escaped = argument.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'
