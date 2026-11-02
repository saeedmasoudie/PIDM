from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, QUrl, Slot
from PySide6.QtMultimedia import QSoundEffect

from controllers.download_settings import DownloadSettings


class SoundController(QObject):
    def __init__(
        self,
        settings: DownloadSettings,
        assets_root: Path,
        parent: QObject | None = None,
    ):
        super().__init__(parent)
        self._settings = settings
        self._completion_default = assets_root / "sounds" / "completion.wav"
        self._error_default = assets_root / "sounds" / "error.wav"
        self._completion_effect = QSoundEffect(self)
        self._completion_effect.setVolume(0.65)
        self._error_effect = QSoundEffect(self)
        self._error_effect.setVolume(0.7)

    @staticmethod
    def _local_url(value: str, fallback: Path) -> QUrl:
        if value:
            url = QUrl(value)
            if url.isLocalFile() and Path(url.toLocalFile()).is_file():
                return url
            path = Path(value).expanduser()
            if path.is_file():
                return QUrl.fromLocalFile(str(path.resolve()))
        return QUrl.fromLocalFile(str(fallback.resolve()))

    @staticmethod
    def _play(effect: QSoundEffect, source: QUrl) -> None:
        if effect.source() != source:
            effect.setSource(source)
        effect.play()

    @Slot()
    def playCompletion(self) -> None:
        if self._settings.completionSoundEnabled:
            self.previewCompletion()

    @Slot()
    def playError(self) -> None:
        if self._settings.errorSoundEnabled:
            self.previewError()

    @Slot()
    def previewCompletion(self) -> None:
        self._play(
            self._completion_effect,
            self._local_url(
                self._settings.completionSound,
                self._completion_default,
            ),
        )

    @Slot()
    def previewError(self) -> None:
        self._play(
            self._error_effect,
            self._local_url(self._settings.errorSound, self._error_default),
        )
