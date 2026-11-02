from __future__ import annotations

import json
import ipaddress
from urllib.parse import urlparse

from PySide6.QtCore import QObject, Property, QSettings, Signal, Slot
from PySide6.QtNetwork import QAbstractSocket, QNetworkInterface


class DownloadSettings(QObject):
    changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._settings = QSettings()
        self._max_connections = self._read_int("downloads/maxConnections", 4, 1, 8)
        self._speed_limit_kbps = self._read_int("downloads/speedLimitKbps", 0, 0, 10_000_000)
        self._max_retries = self._read_int("downloads/maxRetries", 3, 0, 10)
        self._retry_delay = self._read_int("downloads/retryDelay", 3, 1, 300)
        self._clipboard_monitoring = self._read_bool(
            "desktop/clipboardMonitoring", False
        )
        self._completion_notifications = self._read_bool(
            "desktop/completionNotifications", True
        )
        self._completion_sound_enabled = self._read_bool(
            "desktop/completionSoundEnabled", True
        )
        self._error_sound_enabled = self._read_bool(
            "desktop/errorSoundEnabled", True
        )
        self._completion_sound = str(
            self._settings.value("desktop/completionSound", "") or ""
        )
        self._error_sound = str(
            self._settings.value("desktop/errorSound", "") or ""
        )
        self._speed_unit = str(
            self._settings.value("downloads/speedUnit", "auto") or "auto"
        )
        if self._speed_unit not in {"auto", "KB/s", "MB/s", "Mbps"}:
            self._speed_unit = "auto"
        self._multi_interface_enabled = self._read_bool(
            "network/multiInterfaceEnabled", False
        )
        try:
            selected_interfaces = json.loads(
                self._settings.value("network/multiInterfaceAddresses", "[]")
            )
        except (json.JSONDecodeError, TypeError):
            selected_interfaces = []
        self._selected_interface_addresses = [
            str(address) for address in selected_interfaces
            if isinstance(address, str)
        ]
        self._available_interfaces: list[dict[str, str]] = []
        self.refreshInterfaces()
        self._proxy_url = str(self._settings.value("network/proxyUrl", "") or "")
        self._user_agent = str(
            self._settings.value(
                "network/userAgent",
                "PIDM/2.0 (+desktop download manager)",
            )
            or ""
        )
        self._interface_address = str(
            self._settings.value("network/interfaceAddress", "") or ""
        )
        cookie_browser = str(
            self._settings.value("media/cookieBrowser", "") or ""
        ).lower()
        self._media_cookie_browser = (
            cookie_browser
            if cookie_browser in {"edge", "chrome", "firefox"}
            else ""
        )
        try:
            self._site_limits = json.loads(
                self._settings.value("downloads/siteLimits", "{}")
            )
        except (json.JSONDecodeError, TypeError):
            self._site_limits = {}

    def _read_int(self, key: str, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(self._settings.value(key, default))
        except (TypeError, ValueError):
            value = default
        return max(minimum, min(maximum, value))

    def _read_bool(self, key: str, default: bool) -> bool:
        value = self._settings.value(key, default)
        if isinstance(value, str):
            return value.lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _store(self, key: str, value: object) -> None:
        self._settings.setValue(key, value)
        self.changed.emit()

    @Property(int, notify=changed)
    def maxConnections(self):
        return self._max_connections

    @maxConnections.setter
    def maxConnections(self, value):
        self._max_connections = max(1, min(8, int(value)))
        self._store("downloads/maxConnections", self._max_connections)

    @Property(int, notify=changed)
    def speedLimitKbps(self):
        return self._speed_limit_kbps

    @speedLimitKbps.setter
    def speedLimitKbps(self, value):
        self._speed_limit_kbps = max(0, int(value))
        self._store("downloads/speedLimitKbps", self._speed_limit_kbps)

    @Property(int, notify=changed)
    def maxRetries(self):
        return self._max_retries

    @maxRetries.setter
    def maxRetries(self, value):
        self._max_retries = max(0, min(10, int(value)))
        self._store("downloads/maxRetries", self._max_retries)

    @Property(int, notify=changed)
    def retryDelay(self):
        return self._retry_delay

    @retryDelay.setter
    def retryDelay(self, value):
        self._retry_delay = max(1, min(300, int(value)))
        self._store("downloads/retryDelay", self._retry_delay)

    @Property(bool, notify=changed)
    def clipboardMonitoring(self):
        return self._clipboard_monitoring

    @clipboardMonitoring.setter
    def clipboardMonitoring(self, value):
        self._clipboard_monitoring = bool(value)
        self._store("desktop/clipboardMonitoring", self._clipboard_monitoring)

    @Property(bool, notify=changed)
    def completionNotifications(self):
        return self._completion_notifications

    @completionNotifications.setter
    def completionNotifications(self, value):
        self._completion_notifications = bool(value)
        self._store(
            "desktop/completionNotifications", self._completion_notifications
        )

    @Property(bool, notify=changed)
    def completionSoundEnabled(self):
        return self._completion_sound_enabled

    @completionSoundEnabled.setter
    def completionSoundEnabled(self, value):
        self._completion_sound_enabled = bool(value)
        self._store(
            "desktop/completionSoundEnabled", self._completion_sound_enabled
        )

    @Property(bool, notify=changed)
    def errorSoundEnabled(self):
        return self._error_sound_enabled

    @errorSoundEnabled.setter
    def errorSoundEnabled(self, value):
        self._error_sound_enabled = bool(value)
        self._store("desktop/errorSoundEnabled", self._error_sound_enabled)

    @Property(str, notify=changed)
    def completionSound(self):
        return self._completion_sound

    @completionSound.setter
    def completionSound(self, value):
        self._completion_sound = str(value).strip()
        self._store("desktop/completionSound", self._completion_sound)

    @Property(str, notify=changed)
    def errorSound(self):
        return self._error_sound

    @errorSound.setter
    def errorSound(self, value):
        self._error_sound = str(value).strip()
        self._store("desktop/errorSound", self._error_sound)

    @Property(str, notify=changed)
    def speedUnit(self):
        return self._speed_unit

    @speedUnit.setter
    def speedUnit(self, value):
        unit = str(value)
        self._speed_unit = unit if unit in {"auto", "KB/s", "MB/s", "Mbps"} else "auto"
        self._store("downloads/speedUnit", self._speed_unit)

    @Property(bool, notify=changed)
    def multiInterfaceEnabled(self):
        return self._multi_interface_enabled

    @multiInterfaceEnabled.setter
    def multiInterfaceEnabled(self, value):
        self._multi_interface_enabled = bool(value)
        self._store(
            "network/multiInterfaceEnabled", self._multi_interface_enabled
        )

    @Property("QVariantList", notify=changed)
    def availableInterfaces(self):
        selected = set(self._selected_interface_addresses)
        return [
            {**interface, "selected": interface["address"] in selected}
            for interface in self._available_interfaces
        ]

    @Property("QVariantList", notify=changed)
    def selectedInterfaceAddresses(self):
        return list(self._selected_interface_addresses)

    @Slot()
    def refreshInterfaces(self) -> None:
        interfaces: list[dict[str, str]] = []
        seen: set[str] = set()
        required_flags = (
            QNetworkInterface.InterfaceFlag.IsUp
            | QNetworkInterface.InterfaceFlag.IsRunning
        )
        for interface in QNetworkInterface.allInterfaces():
            if interface.flags() & required_flags != required_flags:
                continue
            for entry in interface.addressEntries():
                address = entry.ip()
                if address.protocol() != QAbstractSocket.NetworkLayerProtocol.IPv4Protocol:
                    continue
                value = address.toString()
                try:
                    parsed = ipaddress.ip_address(value)
                except ValueError:
                    continue
                if parsed.is_loopback or parsed.is_link_local or value in seen:
                    continue
                seen.add(value)
                interfaces.append(
                    {
                        "name": interface.humanReadableName() or interface.name(),
                        "address": value,
                    }
                )
        self._available_interfaces = interfaces
        self.changed.emit()

    @Slot(str)
    def toggleInterface(self, address: str) -> None:
        address = str(address)
        available = {item["address"] for item in self._available_interfaces}
        if address not in available:
            return
        if address in self._selected_interface_addresses:
            self._selected_interface_addresses.remove(address)
        else:
            self._selected_interface_addresses.append(address)
        self._settings.setValue(
            "network/multiInterfaceAddresses",
            json.dumps(self._selected_interface_addresses),
        )
        self.changed.emit()

    def interface_addresses(self) -> list[str]:
        if not self._multi_interface_enabled:
            return []
        available = {item["address"] for item in self._available_interfaces}
        return [
            address for address in self._selected_interface_addresses
            if address in available
        ]

    @Property(str, notify=changed)
    def proxyUrl(self):
        return self._proxy_url

    @proxyUrl.setter
    def proxyUrl(self, value):
        self._proxy_url = str(value).strip()
        self._settings.setValue("network/proxyUrl", self._proxy_url)
        self.changed.emit()

    @Property(str, notify=changed)
    def userAgent(self):
        return self._user_agent

    @userAgent.setter
    def userAgent(self, value):
        self._user_agent = str(value).strip()
        self._settings.setValue("network/userAgent", self._user_agent)
        self.changed.emit()

    @Property(str, notify=changed)
    def interfaceAddress(self):
        return self._interface_address

    @interfaceAddress.setter
    def interfaceAddress(self, value):
        self._interface_address = str(value).strip()
        self._settings.setValue("network/interfaceAddress", self._interface_address)
        self.changed.emit()

    @Property(str, notify=changed)
    def mediaCookieBrowser(self):
        return self._media_cookie_browser

    @mediaCookieBrowser.setter
    def mediaCookieBrowser(self, value):
        browser = str(value).strip().lower()
        self._media_cookie_browser = (
            browser if browser in {"edge", "chrome", "firefox"} else ""
        )
        self._settings.setValue(
            "media/cookieBrowser", self._media_cookie_browser
        )
        self.changed.emit()

    @Property("QVariantList", notify=changed)
    def siteLimits(self):
        return [
            {"host": host, "connections": connections}
            for host, connections in sorted(self._site_limits.items())
        ]

    @Slot(str, int, result=bool)
    def setSiteLimit(self, host: str, connections: int) -> bool:
        host = host.strip().lower()
        if "://" in host:
            host = (urlparse(host).hostname or "").lower()
        host = host.strip(".")
        if not host or " " in host:
            return False
        self._site_limits[host] = max(1, min(8, int(connections)))
        self._settings.setValue(
            "downloads/siteLimits",
            json.dumps(self._site_limits, sort_keys=True),
        )
        self.changed.emit()
        return True

    @Slot(str)
    def removeSiteLimit(self, host: str) -> None:
        self._site_limits.pop(host, None)
        self._settings.setValue(
            "downloads/siteLimits",
            json.dumps(self._site_limits, sort_keys=True),
        )
        self.changed.emit()

    def connections_for_url(self, url: str) -> int:
        host = (urlparse(url).hostname or "").lower()
        if host in self._site_limits:
            return int(self._site_limits[host])
        for configured, value in self._site_limits.items():
            if host.endswith("." + configured):
                return int(value)
        return self._max_connections
