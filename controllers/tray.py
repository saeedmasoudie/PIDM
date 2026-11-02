from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, Signal, Slot
from PySide6.QtGui import QAction, QColor, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QMenu, QSystemTrayIcon


def create_app_icon(logo_path: Path | None = None) -> QIcon:
    if logo_path and logo_path.is_file():
        icon = QIcon(str(logo_path))
        if not icon.isNull():
            return icon
    pixmap = QPixmap(64, 64)
    pixmap.fill(QColor("#7357FF"))
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing)
    painter.setPen(QColor("white"))
    font = painter.font()
    font.setBold(True)
    font.setPixelSize(27)
    painter.setFont(font)
    painter.drawText(pixmap.rect(), 0x84, "P")
    painter.end()
    return QIcon(pixmap)


class TrayController(QObject):
    showRequested = Signal()
    addDownloadRequested = Signal()
    downloadDetailsRequested = Signal(int)
    quitRequested = Signal()

    def __init__(self, app: QApplication, downloads, settings, icon, parent=None):
        super().__init__(parent)
        self._app = app
        self._quitting = False
        self._tray = QSystemTrayIcon(icon, self)
        self._tray.setToolTip("PIDM — Download manager")

        self._menu = QMenu()
        self._menu.setSeparatorsCollapsible(False)
        self._menu.setStyleSheet(
            """
            QMenu {
                background-color: #111722;
                color: #EDF0F7;
                border: 1px solid #30394A;
                border-radius: 8px;
                padding: 4px;
                font-family: "Segoe UI";
                font-size: 9pt;
            }
            QMenu::item {
                min-width: 170px;
                padding: 6px 24px 6px 25px;
                margin: 1px 0;
                border-radius: 5px;
            }
            QMenu::item:selected {
                background-color: #7357FF;
                color: white;
            }
            QMenu::item:disabled {
                color: #7F899B;
                background: transparent;
            }
            QMenu::separator {
                height: 1px;
                background: #30394A;
                margin: 4px 6px;
            }
            """
        )

        show_action = QAction("Open PIDM", self._menu)
        show_action.triggered.connect(self.showRequested)
        self._menu.addAction(show_action)
        self._menu.setDefaultAction(show_action)

        add_action = QAction("Add download…", self._menu)
        add_action.triggered.connect(self.addDownloadRequested)
        self._menu.addAction(add_action)
        self._menu.addSeparator()

        self._active_downloads_menu = self._menu.addMenu("Active download details")

        self._transfer_action = QAction("Pause all", self._menu)
        self._transfer_action.triggered.connect(self._toggle_transfers)
        self._menu.addAction(self._transfer_action)
        self._menu.addSeparator()

        quit_action = QAction("Exit PIDM", self._menu)
        quit_action.triggered.connect(self.requestQuit)
        self._menu.addAction(quit_action)

        self._downloads = downloads
        self._menu.aboutToShow.connect(self._refresh_menu)
        downloads.statsChanged.connect(self._refresh_menu)
        self._tray.setContextMenu(self._menu)
        self._tray.activated.connect(self._activated)
        downloads.downloadCompleted.connect(
            lambda name: self._notify(
                "Download complete", f"{name} is ready to open.", settings
            )
        )
        downloads.downloadFailed.connect(
            lambda name: self._notify(
                "Download needs attention", f"{name} could not be downloaded.", settings
            )
        )
        if QSystemTrayIcon.isSystemTrayAvailable():
            self._refresh_menu()
            self._tray.show()

    @property
    def available(self) -> bool:
        return QSystemTrayIcon.isSystemTrayAvailable()

    @Slot(result=bool)
    def isAvailable(self) -> bool:
        return self.available

    @Slot()
    def requestQuit(self) -> None:
        self.quitRequested.emit()

    @Slot()
    def confirmQuit(self) -> None:
        self._quitting = True
        self._tray.hide()
        self._app.quit()

    @Slot(result=bool)
    def shouldCloseToTray(self) -> bool:
        return self.available and not self._quitting

    def _activated(self, reason: QSystemTrayIcon.ActivationReason) -> None:
        if reason in (
            QSystemTrayIcon.ActivationReason.Trigger,
            QSystemTrayIcon.ActivationReason.DoubleClick,
        ):
            self.showRequested.emit()

    def _notify(self, title: str, message: str, settings) -> None:
        if self.available and settings.completionNotifications:
            self._tray.showMessage(
                title,
                message,
                QSystemTrayIcon.MessageIcon.Information,
                5000,
            )

    def _refresh_menu(self) -> None:
        active = self._downloads.activeCount
        pending = self._downloads.problemCount
        total = self._downloads.totalCount
        active_downloads = self._downloads.active_downloads_for_tray()
        self._active_downloads_menu.clear()
        self._active_downloads_menu.setEnabled(bool(active_downloads))
        for download_id, name, progress in active_downloads:
            display_name = name if len(name) <= 42 else name[:39] + "…"
            action = self._active_downloads_menu.addAction(
                f"{display_name}  ·  {progress}%"
            )
            action.triggered.connect(
                lambda checked=False, item_id=download_id:
                    self.downloadDetailsRequested.emit(item_id)
            )
        if active:
            self._transfer_action.setText("Pause all")
            self._transfer_action.setEnabled(True)
            status = f"{active} active  ·  {self._downloads.totalSpeed}"
        elif pending:
            self._transfer_action.setText("Resume pending")
            self._transfer_action.setEnabled(True)
            status = f"{pending} waiting for attention  ·  {total} total"
        else:
            self._transfer_action.setText("Pause all")
            self._transfer_action.setEnabled(False)
            status = f"All caught up  ·  {total} downloads"
        self._tray.setToolTip(f"PIDM — {status}")

    def _toggle_transfers(self) -> None:
        if self._downloads.activeCount > 0:
            self._downloads.pauseAll()
        else:
            self._downloads.resumeAll()
