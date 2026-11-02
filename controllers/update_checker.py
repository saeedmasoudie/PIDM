from __future__ import annotations

import os
import re
import threading
import time

import httpx
from PySide6.QtCore import QObject, Property, QSettings, QTimer, QUrl, Signal, Slot
from PySide6.QtGui import QDesktopServices

from core.build_info import APP_VERSION, GITHUB_REPOSITORY


class UpdateChecker(QObject):
    updateAvailable = Signal(str, str, str)
    checkFailed = Signal(str)
    _checkCompleted = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._settings = QSettings()
        repository = (
            os.environ.get("PIDM_GITHUB_REPOSITORY", "").strip()
            or GITHUB_REPOSITORY
        )
        self._repository = (
            repository.removeprefix("https://github.com/")
            .removeprefix("http://github.com/")
            .strip("/")
        )
        self._checking = False
        self._checkCompleted.connect(self._finish_check)

    currentVersion = Property(str, lambda self: APP_VERSION, constant=True)
    configured = Property(bool, lambda self: bool(self._repository), constant=True)

    def startAutomaticCheck(self) -> None:
        QTimer.singleShot(5000, lambda: self.checkForUpdates(False))

    @Slot(bool)
    def checkForUpdates(self, force: bool = False) -> None:
        if self._checking or not self._repository:
            return
        last_check = float(self._settings.value("updates/lastCheck", 0) or 0)
        if not force and time.time() - last_check < 24 * 60 * 60:
            return
        self._checking = True
        threading.Thread(
            target=self._check_worker,
            name="pidm-update-checker",
            daemon=True,
        ).start()

    def _check_worker(self) -> None:
        successful = False
        try:
            response = httpx.get(
                f"https://api.github.com/repos/{self._repository}/releases/latest",
                headers={
                    "Accept": "application/vnd.github+json",
                    "User-Agent": f"PIDM/{APP_VERSION}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                timeout=10,
                follow_redirects=True,
            )
            if response.status_code == 404:
                successful = True
                return
            response.raise_for_status()
            release = response.json()
            successful = True
            latest = str(release.get("tag_name") or "").lstrip("v")
            if latest and self._version_key(latest) > self._version_key(APP_VERSION):
                self.updateAvailable.emit(
                    latest,
                    str(release.get("html_url") or ""),
                    str(release.get("body") or "")[:1200],
                )
        except Exception as exc:
            self.checkFailed.emit(str(exc))
        finally:
            self._checkCompleted.emit(successful)

    @Slot(bool)
    def _finish_check(self, successful: bool) -> None:
        self._checking = False
        if successful:
            self._settings.setValue("updates/lastCheck", time.time())

    @staticmethod
    def _version_key(value: str) -> tuple:
        core, separator, prerelease = value.lstrip("v").partition("-")
        numbers = [int(part) for part in re.findall(r"\d+", core)[:4]]
        numbers.extend([0] * (4 - len(numbers)))
        return (*numbers, 1 if not separator else 0, prerelease)

    @Slot(str)
    def openRelease(self, url: str) -> None:
        parsed = QUrl(url)
        if (
            parsed.scheme() == "https"
            and parsed.host().lower() == "github.com"
        ):
            QDesktopServices.openUrl(parsed)
