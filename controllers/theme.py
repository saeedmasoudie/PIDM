from PySide6.QtCore import QObject, Property, QSettings, Signal, Slot


PALETTES = {
    "midnight": {
        "background": "#0A0D14", "sidebar": "#0D111A", "surface": "#111722",
        "surfaceRaised": "#171E2B", "surfaceHover": "#1B2433", "text": "#F3F6FC",
        "muted": "#8B96AA", "subtle": "#596579", "accent": "#7357FF",
        "accentHover": "#846CFF", "accentSoft": "#251F4D", "line": "#222C3B",
        "success": "#38C990", "warning": "#F1B74A", "danger": "#F06D78",
    },
    "light": {
        "background": "#F3F5F9", "sidebar": "#FFFFFF", "surface": "#FFFFFF",
        "surfaceRaised": "#F8F9FC", "surfaceHover": "#EEF1F6", "text": "#182033",
        "muted": "#667085", "subtle": "#98A2B3", "accent": "#654CE6",
        "accentHover": "#563CCF", "accentSoft": "#ECE8FF", "line": "#E2E7EF",
        "success": "#169B69", "warning": "#C98512", "danger": "#D84D5A",
    },
    "ocean": {
        "background": "#061318", "sidebar": "#071A21", "surface": "#0B2028",
        "surfaceRaised": "#102B35", "surfaceHover": "#153641", "text": "#EDF9FC",
        "muted": "#83A7B0", "subtle": "#4E737D", "accent": "#12A8C2",
        "accentHover": "#28BCD3", "accentSoft": "#0B3B46", "line": "#193C46",
        "success": "#44CE9B", "warning": "#EAAF49", "danger": "#EC727A",
    },
}


class ThemeController(QObject):
    changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._settings = QSettings()
        saved_name = self._settings.value("appearance/theme", "midnight")
        self._name = saved_name if saved_name in PALETTES else "midnight"

    name = Property(str, lambda self: self._name, notify=changed)
    background = Property(str, lambda self: PALETTES[self._name]["background"], notify=changed)
    sidebar = Property(str, lambda self: PALETTES[self._name]["sidebar"], notify=changed)
    surface = Property(str, lambda self: PALETTES[self._name]["surface"], notify=changed)
    surfaceRaised = Property(str, lambda self: PALETTES[self._name]["surfaceRaised"], notify=changed)
    surfaceHover = Property(str, lambda self: PALETTES[self._name]["surfaceHover"], notify=changed)
    text = Property(str, lambda self: PALETTES[self._name]["text"], notify=changed)
    muted = Property(str, lambda self: PALETTES[self._name]["muted"], notify=changed)
    subtle = Property(str, lambda self: PALETTES[self._name]["subtle"], notify=changed)
    accent = Property(str, lambda self: PALETTES[self._name]["accent"], notify=changed)
    accentHover = Property(str, lambda self: PALETTES[self._name]["accentHover"], notify=changed)
    accentSoft = Property(str, lambda self: PALETTES[self._name]["accentSoft"], notify=changed)
    line = Property(str, lambda self: PALETTES[self._name]["line"], notify=changed)
    success = Property(str, lambda self: PALETTES[self._name]["success"], notify=changed)
    warning = Property(str, lambda self: PALETTES[self._name]["warning"], notify=changed)
    danger = Property(str, lambda self: PALETTES[self._name]["danger"], notify=changed)

    @Slot(str)
    def select(self, name):
        if name in PALETTES and name != self._name:
            self._name = name
            self._settings.setValue("appearance/theme", name)
            self.changed.emit()
