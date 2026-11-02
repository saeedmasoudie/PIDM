from __future__ import annotations

from urllib.parse import urlparse

from PySide6.QtCore import QObject, Signal, Slot
from PySide6.QtGui import QGuiApplication

from controllers.download_settings import DownloadSettings


class ClipboardMonitor(QObject):
    urlDetected = Signal(str)

    def __init__(self, settings: DownloadSettings, parent=None):
        super().__init__(parent)
        self._settings = settings
        self._last_url = ""
        clipboard = QGuiApplication.clipboard()
        clipboard.dataChanged.connect(self._check_clipboard)

    def _check_clipboard(self) -> None:
        if not self._settings.clipboardMonitoring:
            return
        text = QGuiApplication.clipboard().text().strip()
        parsed = urlparse(text)
        if (
            text != self._last_url
            and (
                (parsed.scheme in {"http", "https"} and bool(parsed.netloc))
                or (
                    parsed.scheme == "magnet"
                    and "urn:btih:" in text.lower()
                )
            )
            and "\n" not in text
        ):
            self._last_url = text
            self.urlDetected.emit(text)

    @Slot(result=str)
    def text(self) -> str:
        return QGuiApplication.clipboard().text().strip()
