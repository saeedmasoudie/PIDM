from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtCore import QObject, Property, QSettings, Signal, Slot


class I18n(QObject):
    languageChanged = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._settings = QSettings()
        self._catalogs = {}
        translations = Path(__file__).parents[1] / "translations"
        for path in translations.glob("*.json"):
            self._catalogs[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        saved_language = self._settings.value("appearance/language", "en")
        self._language = saved_language if saved_language in self._catalogs else "en"
        self._default_font_family = ""
        self._persian_font_family = "Vazirmatn"

    @Property(str, notify=languageChanged)
    def language(self):
        return self._language

    @language.setter
    def language(self, value):
        if value in self._catalogs and value != self._language:
            self._language = value
            self._settings.setValue("appearance/language", value)
            self.languageChanged.emit()

    @Property(bool, notify=languageChanged)
    def rtl(self):
        return self._language == "fa"

    @Property(str, notify=languageChanged)
    def fontFamily(self):
        return self.current_font_family()

    def current_font_family(self) -> str:
        if self._language == "fa":
            return self._persian_font_family
        return self._default_font_family

    def set_font_families(self, default_family: str, persian_family: str) -> None:
        self._default_font_family = default_family
        self._persian_font_family = persian_family or "Vazirmatn"

    @Slot(str, result=str)
    def t(self, key):
        return self._catalogs.get(self._language, {}).get(
            key, self._catalogs.get("en", {}).get(key, key)
        )
