from __future__ import annotations

import sys
import platform
from pathlib import Path

from platformdirs import user_data_path
from PySide6.QtCore import QUrl
from PySide6.QtGui import QFont, QFontDatabase
from PySide6.QtQml import QQmlApplicationEngine, qmlRegisterSingletonInstance
from PySide6.QtQuickControls2 import QQuickStyle
from PySide6.QtWidgets import QApplication

from controllers.i18n import I18n
from controllers.autostart import AutostartController
from controllers.browser_bridge import BrowserBridge
from controllers.clipboard_monitor import ClipboardMonitor
from controllers.protocol_capabilities import ProtocolCapabilities
from controllers.download_settings import DownloadSettings
from controllers.sound import SoundController
from controllers.system_actions import SystemActions
from controllers.theme import ThemeController
from controllers.tray import TrayController, create_app_icon
from controllers.single_instance import SingleInstance
from controllers.update_checker import UpdateChecker
from core.database import DownloadDatabase
from models.download_model import DownloadModel
from models.queue_model import QueueModel


def main() -> int:
    QQuickStyle.setStyle("Basic")
    app = QApplication(sys.argv)
    app.setApplicationName("PIDM")
    app.setOrganizationName("PIDM")
    default_font_family = app.font().family()
    if platform.system() == "Windows":
        try:
            import ctypes

            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                "PIDM.DownloadManager"
            )
        except (AttributeError, OSError):
            pass

    single_instance = SingleInstance("PIDM.DownloadManager")
    if not single_instance.claim():
        return 0

    root = Path(__file__).parent
    logo_path = root / "assets" / "logo.png"
    app_icon = create_app_icon(logo_path)
    app.setWindowIcon(app_icon)
    i18n = I18n()
    font_path = root / "assets" / "fonts" / "Vazirmatn[wght].ttf"
    bundled_persian_fonts = ()
    if font_path.is_file():
        font_id = QFontDatabase.addApplicationFont(str(font_path))
        if font_id >= 0:
            bundled_persian_fonts = tuple(
                QFontDatabase.applicationFontFamilies(font_id)
            )
    preferred_fonts = ("Segoe UI Variable", "Segoe UI", "Inter", "Noto Sans")
    persian_fonts = bundled_persian_fonts + (
        "Vazirmatn",
        "Noto Sans Arabic",
        "Tahoma",
        "Segoe UI",
    )
    available_fonts = set(QFontDatabase.families())
    persian_font_family = next(
        (name for name in persian_fonts if name in available_fonts),
        default_font_family,
    )
    preferred_font_family = next(
        (name for name in preferred_fonts if name in available_fonts),
        default_font_family,
    )
    i18n.set_font_families(preferred_font_family, persian_font_family)

    def apply_language_font() -> None:
        app.setFont(QFont(i18n.current_font_family(), 10))

    apply_language_font()
    i18n.languageChanged.connect(apply_language_font)

    qmlRegisterSingletonInstance(I18n, "PIDM", 1, 0, "I18n", i18n)
    engine = QQmlApplicationEngine()
    context = engine.rootContext()
    database = DownloadDatabase(Path(user_data_path("PIDM", "PIDM")) / "downloads-v2.db")
    download_settings = DownloadSettings()
    downloads_model = DownloadModel(database, download_settings)
    sound_controller = SoundController(download_settings, root / "assets")
    downloads_model.downloadCompleted.connect(
        lambda _name: sound_controller.playCompletion()
    )
    downloads_model.downloadFailed.connect(
        lambda _name: sound_controller.playError()
    )
    tray = TrayController(app, downloads_model, download_settings, app_icon)
    single_instance.activationRequested.connect(tray.showRequested.emit)
    protocol_capabilities = ProtocolCapabilities(download_settings)
    browser_bridge = BrowserBridge(protocol_capabilities)
    browser_bridge.refreshLinkReceived.connect(downloads_model.refreshDownloadUrl)
    browser_bridge.singleUrlReceived.connect(
        lambda _url: tray.showRequested.emit()
    )
    browser_bridge.batchReceived.connect(
        lambda _items, _title: tray.showRequested.emit()
    )
    clipboard_monitor = ClipboardMonitor(download_settings)
    app.setQuitOnLastWindowClosed(not tray.available)
    queues_model = QueueModel(database, downloads_model)
    system_actions = SystemActions()
    autostart = AutostartController(root / "main.py")
    update_checker = UpdateChecker()
    update_checker.startAutomaticCheck()
    queues_model.postActionRequested.connect(system_actions.execute)
    theme = ThemeController()
    context.setContextProperty("downloadsModel", downloads_model)
    context.setContextProperty("downloadSettings", download_settings)
    context.setContextProperty("soundController", sound_controller)
    context.setContextProperty("queuesModel", queues_model)
    context.setContextProperty("systemActions", system_actions)
    context.setContextProperty("autostartController", autostart)
    context.setContextProperty("updateChecker", update_checker)
    context.setContextProperty("browserBridge", browser_bridge)
    context.setContextProperty("theme", theme)
    context.setContextProperty("trayController", tray)
    context.setContextProperty("clipboardMonitor", clipboard_monitor)
    context.setContextProperty("protocolCapabilities", protocol_capabilities)
    context.setContextProperty(
        "appLogoUrl",
        QUrl.fromLocalFile(str(logo_path)).toString()
        if logo_path.is_file()
        else "",
    )
    app.aboutToQuit.connect(downloads_model.shutdown)

    qml_file = root / "qml" / "Main.qml"
    engine.load(QUrl.fromLocalFile(str(qml_file)))
    if not engine.rootObjects():
        return 1
    exit_code = app.exec()
    del engine
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
