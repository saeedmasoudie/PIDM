from __future__ import annotations

import base64
import binascii
import re
import shutil
import threading
import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from platformdirs import user_data_path
from PySide6.QtCore import (
    QAbstractListModel,
    QModelIndex,
    QObject,
    Property,
    QStandardPaths,
    QTimer,
    Qt,
    QUrl,
    Signal,
    Slot,
)
from PySide6.QtGui import QDesktopServices, QGuiApplication

from core.database import DownloadDatabase
from core.http_downloader import download_http
from core.media_downloader import download_media
from core.torrent_downloader import download_torrent
from controllers.download_settings import DownloadSettings

MEDIA_SOURCE_HEADER = "X-PIDM-Media-Source"
MEDIA_FORMAT_HEADER = "X-PIDM-Media-Format"
MEDIA_TITLE_HEADER = "X-PIDM-Media-Title"
MEDIA_EXTENSION_HEADER = "X-PIDM-Media-Extension"
MEDIA_COOKIE_BROWSER_HEADER = "X-PIDM-Media-Cookie-Browser"
MEDIA_METADATA_HEADERS = {
    MEDIA_SOURCE_HEADER,
    MEDIA_FORMAT_HEADER,
    MEDIA_TITLE_HEADER,
    MEDIA_EXTENSION_HEADER,
    MEDIA_COOKIE_BROWSER_HEADER,
}


def format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def format_speed(value: float, unit: str) -> str:
    if value <= 0:
        return "—"
    if unit == "KB/s":
        return f"{value / 1024:.1f} KB/s"
    if unit == "MB/s":
        return f"{value / (1024 * 1024):.2f} MB/s"
    if unit == "Mbps":
        return f"{value * 8 / 1_000_000:.2f} Mbps"
    return f"{format_bytes(int(value))}/s"


def format_timestamp(value: str) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d  %H:%M")
    except ValueError:
        return value


def format_duration(seconds: float) -> str:
    if seconds <= 0 or seconds == float("inf"):
        return "—"
    minutes, second = divmod(int(seconds), 60)
    hours, minute = divmod(minutes, 60)
    return f"{hours:02d}:{minute:02d}:{second:02d}" if hours else f"{minute:02d}:{second:02d}"


def safe_filename(url: str) -> str:
    if url.lower().startswith("magnet:"):
        query = parse_qs(urlparse(url).query)
        display_name = unquote((query.get("dn") or [""])[0])
        name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", display_name).strip(" .")
        return (name[:180] if name else "magnet-download") + ".torrent"
    path_name = unquote(Path(urlparse(url).path).name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", path_name).strip(" .")
    return name[:180] or "download.bin"


def _decode_media_metadata(value: str) -> str:
    if not value:
        return ""
    try:
        return base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return ""


def _safe_media_filename(title: str, extension: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", title).strip(" .")
    suffix = re.sub(r"[^a-zA-Z0-9]", "", extension).lower() or "bin"
    return f"{name[:170] or 'media'}.{suffix}"


def is_torrent_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme.lower() == "magnet" or parsed.path.lower().endswith(".torrent")


def unique_destination(directory: Path, filename: str, reserved: set[str] | None = None) -> Path:
    reserved = reserved or set()
    candidate = directory / filename
    if (
        str(candidate) not in reserved
        and not candidate.exists()
        and not candidate.with_name(candidate.name + ".part").exists()
    ):
        return candidate
    for number in range(1, 10_000):
        alternate = directory / f"{candidate.stem} ({number}){candidate.suffix}"
        if (
            str(alternate) not in reserved
            and not alternate.exists()
            and not alternate.with_name(alternate.name + ".part").exists()
        ):
            return alternate
    raise RuntimeError("Could not create a unique destination filename")


@dataclass(slots=True)
class Download:
    id: int
    url: str
    destination: str
    name: str
    source: str
    status: str = "queued"
    downloaded: int = 0
    total: int = 0
    speed_value: float = 0.0
    smoothed_speed: float = 0.0
    error: str = ""
    queue_id: int | None = None
    queue_position: int = 0
    created_at: str = ""
    finished_at: str = ""
    connections: int = 1
    retry_attempt: int = 0
    retry_message: str = ""
    speed_limit: int = 0
    connection_limit: int = 0
    checksum_expected: str = ""
    checksum_actual: str = ""
    request_headers: dict[str, str] = field(default_factory=dict)
    proxy_url: str = ""
    interface_address: str = ""
    torrent_files: list[int] = field(default_factory=list)
    last_metrics_emit: float = 0.0
    stop_event: threading.Event = field(default_factory=threading.Event, repr=False)


class WorkerBridge(QObject):
    progress = Signal(int, "qlonglong", "qlonglong", float)
    finished = Signal(int, str, str, str)
    modeChanged = Signal(int, int)
    retryChanged = Signal(int, int, str)


class DownloadModel(QAbstractListModel):
    IdRole = Qt.UserRole + 1
    NameRole = Qt.UserRole + 2
    SourceRole = Qt.UserRole + 3
    SizeRole = Qt.UserRole + 4
    ProgressRole = Qt.UserRole + 5
    SpeedRole = Qt.UserRole + 6
    EtaRole = Qt.UserRole + 7
    StatusRole = Qt.UserRole + 8
    ProtocolRole = Qt.UserRole + 9
    ErrorRole = Qt.UserRole + 10
    PathRole = Qt.UserRole + 11
    QueueNameRole = Qt.UserRole + 12
    CreatedRole = Qt.UserRole + 13

    errorOccurred = Signal(str)
    statsChanged = Signal()
    queueActivityChanged = Signal(int, bool)
    queueCompleted = Signal(int)
    downloadCompleted = Signal(str)
    downloadFailed = Signal(str)

    def __init__(
        self,
        database: DownloadDatabase | None = None,
        settings: DownloadSettings | None = None,
        parent=None,
    ):
        super().__init__(parent)
        if database is None:
            data_dir = Path(user_data_path("PIDM", "PIDM"))
            database = DownloadDatabase(data_dir / "downloads-v2.db")
        self._database = database
        self._settings = settings or DownloadSettings()
        self._items = [self._from_record(record) for record in self._database.all()]
        self._threads: dict[int, threading.Thread] = {}
        self._discard_paths: dict[int, Path] = {}
        self._queue_limits: dict[int, int] = {}
        self._queue_delays: dict[int, int] = {}
        self._sort_key = "date"
        self._sort_descending = True
        self._bridge = WorkerBridge(self)
        self._bridge.progress.connect(self._on_progress)
        self._bridge.finished.connect(self._on_finished)
        self._bridge.modeChanged.connect(self._on_mode_changed)
        self._bridge.retryChanged.connect(self._on_retry_changed)
        self._settings.changed.connect(self._refresh_display_settings)

        for item in self._items:
            if item.status in {"downloading", "queued", "pausing"}:
                item.status = "paused"
                item.speed_value = 0
                item.smoothed_speed = 0
                self._database.update(item.id, status="paused", speed=0)

    @staticmethod
    def _from_record(record: dict) -> Download:
        return Download(
            id=record["id"],
            url=record["url"],
            destination=record["destination"],
            name=record["name"],
            source=record["source"],
            status=record["status"],
            downloaded=record["downloaded"],
            total=record["total"],
            speed_value=record["speed"],
            error=record["error"],
            queue_id=record.get("queue_id"),
            queue_position=record.get("queue_position", 0),
            created_at=record.get("created_at", ""),
            finished_at=record.get("finished_at", ""),
            speed_limit=record.get("speed_limit", 0),
            connection_limit=record.get("connection_limit", 0),
            checksum_expected=record.get("checksum_expected", ""),
            checksum_actual=record.get("checksum_actual", ""),
            request_headers=DownloadModel._decode_headers(
                record.get("request_headers", "{}")
            ),
            proxy_url=record.get("proxy_url", ""),
            interface_address=record.get("interface_address", ""),
            torrent_files=DownloadModel._decode_int_list(
                record.get("torrent_files", "[]")
            ),
        )

    def roleNames(self):
        return {
            self.IdRole: b"downloadId",
            self.NameRole: b"name",
            self.SourceRole: b"source",
            self.SizeRole: b"size",
            self.ProgressRole: b"progress",
            self.SpeedRole: b"speed",
            self.EtaRole: b"eta",
            self.StatusRole: b"status",
            self.ProtocolRole: b"protocol",
            self.ErrorRole: b"errorMessage",
            self.PathRole: b"filePath",
            self.QueueNameRole: b"queueName",
            self.CreatedRole: b"createdAt",
        }

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._items)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not 0 <= index.row() < len(self._items):
            return None
        item = self._items[index.row()]
        progress = (
            100
            if item.status == "complete"
            else min(100, max(0, int(item.downloaded * 100 / item.total)))
            if item.total
            else 0
        )
        remaining = max(item.total - item.downloaded, 0)
        eta = remaining / item.speed_value if item.speed_value > 0 else 0
        fields = {
            self.IdRole: item.id,
            self.NameRole: item.name,
            self.SourceRole: item.source,
            self.SizeRole: format_bytes(item.total) if item.total else "Unknown size",
            self.ProgressRole: progress,
            self.SpeedRole: format_speed(
                item.speed_value, self._settings.speedUnit
            ),
            self.EtaRole: "Done" if item.status == "complete" else format_duration(eta),
            self.StatusRole: item.status,
            self.ProtocolRole: (
                "TORRENT"
                if is_torrent_url(item.url)
                else "HTTPS"
                if item.url.startswith("https:")
                else "HTTP"
            ),
            self.ErrorRole: item.error,
            self.PathRole: item.destination,
            self.QueueNameRole: self._database.queue_name(item.queue_id),
            self.CreatedRole: format_timestamp(item.created_at),
        }
        return fields.get(role)

    @Property(str, constant=True)
    def defaultDownloadPath(self):
        locations = QStandardPaths.standardLocations(QStandardPaths.DownloadLocation)
        return locations[0] if locations else str(Path.home() / "Downloads")

    @Property(int, notify=statsChanged)
    def activeCount(self):
        return sum(item.status == "downloading" for item in self._items)

    @Property(int, notify=statsChanged)
    def completedCount(self):
        return sum(item.status == "complete" for item in self._items)

    @Property(int, notify=statsChanged)
    def totalCount(self):
        return len(self._items)

    @Property(int, notify=statsChanged)
    def problemCount(self):
        return sum(item.status in {"error", "paused"} for item in self._items)

    @Property(str, notify=statsChanged)
    def totalSpeed(self):
        return format_speed(
            sum(item.speed_value for item in self._items),
            self._settings.speedUnit,
        )

    @Slot()
    def _refresh_display_settings(self) -> None:
        if self._items:
            top = self.index(0)
            bottom = self.index(len(self._items) - 1)
            self.dataChanged.emit(top, bottom, [self.SpeedRole])
        self.statsChanged.emit()

    @Slot(str, bool)
    def sortDownloads(self, key: str, descending: bool) -> None:
        if key not in {"date", "name", "size", "status"}:
            key = "date"
        self._sort_key = key
        self._sort_descending = bool(descending)
        status_order = {
            "downloading": 0,
            "pausing": 1,
            "queued": 2,
            "paused": 3,
            "error": 4,
            "complete": 5,
        }

        def sort_value(item: Download):
            if key == "name":
                return (item.name.casefold(), item.id)
            if key == "size":
                return (item.total, item.id)
            if key == "status":
                return (status_order.get(item.status, 99), item.id)
            return (item.created_at, item.id)

        self.beginResetModel()
        self._items.sort(key=sort_value, reverse=self._sort_descending)
        self.endResetModel()

    def _row_for_id(self, download_id: int) -> int:
        return next((row for row, item in enumerate(self._items) if item.id == download_id), -1)

    @staticmethod
    def _decode_headers(value: str | dict) -> dict[str, str]:
        if isinstance(value, dict):
            return {str(key): str(item) for key, item in value.items()}
        try:
            parsed = json.loads(value or "{}")
        except (json.JSONDecodeError, TypeError):
            return {}
        return (
            {str(key): str(item) for key, item in parsed.items()}
            if isinstance(parsed, dict)
            else {}
        )

    @staticmethod
    def _decode_int_list(value: str | list) -> list[int]:
        try:
            parsed = value if isinstance(value, list) else json.loads(value or "[]")
            return [int(item) for item in parsed] if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError, ValueError):
            return []

    @staticmethod
    def _parse_headers(value: str) -> dict[str, str]:
        headers: dict[str, str] = {}
        forbidden = {"host", "content-length", "range"}
        for raw in re.split(
            r"\n+|;\s*(?=[A-Za-z0-9!#$%&'*+.^_`|~-]+\s*:)",
            value,
        ):
            raw = raw.strip()
            if not raw:
                continue
            if ":" not in raw:
                raise ValueError(f"Invalid request header: {raw}")
            name, header_value = (part.strip() for part in raw.split(":", 1))
            if not name or not header_value:
                raise ValueError(f"Invalid request header: {raw}")
            if name.lower() in forbidden:
                raise ValueError(f"PIDM manages the {name} header automatically.")
            headers[name] = header_value
        return headers

    def _emit_row(
        self, row: int, roles: list[int], notify_stats: bool = True
    ) -> None:
        model_index = self.index(row)
        self.dataChanged.emit(model_index, model_index, roles)
        if notify_stats:
            self.statsChanged.emit()

    @Slot(str, str, result=bool)
    def addDownload(self, url: str, directory: str) -> bool:
        return self._add_download(
            url, directory, self._database.main_queue_id(), start_now=True
        )

    @Slot(
        str, str, int, bool, str, str, str, str, "QVariantList", "qlonglong",
        result=bool,
    )
    def addDownloadToQueue(
        self,
        url: str,
        directory: str,
        queue_id: int,
        start_now: bool,
        checksum_expected: str,
        headers_text: str,
        proxy_url: str,
        interface_address: str,
        torrent_files: list,
        expected_size: int,
    ) -> bool:
        return self._add_download(
            url,
            directory,
            queue_id,
            start_now,
            checksum_expected,
            headers_text,
            proxy_url,
            interface_address,
            torrent_files,
            expected_size,
        )

    def _add_download(
        self,
        url: str,
        directory: str,
        queue_id: int,
        start_now: bool,
        checksum_expected: str = "",
        headers_text: str = "",
        proxy_url: str = "",
        interface_address: str = "",
        torrent_files: list[int] | None = None,
        expected_size: int = 0,
    ) -> bool:
        url = url.strip()
        parsed = urlparse(url)
        is_magnet = parsed.scheme == "magnet" and "urn:btih:" in url.lower()
        is_web = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        if not is_web and not is_magnet:
            self.errorOccurred.emit(
                "Enter a complete HTTP, HTTPS, magnet, or .torrent URL."
            )
            return False
        duplicate = next(
            (
                item for item in self._items
                if item.url == url and item.status != "complete"
            ),
            None,
        )
        if duplicate:
            self.errorOccurred.emit(
                f"This URL is already in PIDM as '{duplicate.name}'."
            )
            return False
        checksum_expected = checksum_expected.strip().lower()
        if checksum_expected and is_torrent_url(url):
            self.errorOccurred.emit(
                "Whole-transfer SHA-256 verification is not available for torrents."
            )
            return False
        if checksum_expected and not re.fullmatch(r"[0-9a-f]{64}", checksum_expected):
            self.errorOccurred.emit("SHA-256 must contain exactly 64 hexadecimal characters.")
            return False
        try:
            request_headers = self._parse_headers(headers_text)
        except ValueError as exc:
            self.errorOccurred.emit(str(exc))
            return False
        if self._settings.userAgent and not any(
            name.lower() == "user-agent" for name in request_headers
        ):
            request_headers["User-Agent"] = self._settings.userAgent

        directory_path = (
            Path(QUrl(directory).toLocalFile())
            if directory.startswith("file:")
            else Path(directory or self.defaultDownloadPath)
        )
        try:
            directory_path.mkdir(parents=True, exist_ok=True)
            reserved = {item.destination for item in self._items}
            media_source = _decode_media_metadata(
                request_headers.get(MEDIA_SOURCE_HEADER, "")
            )
            if media_source:
                filename = _safe_media_filename(
                    _decode_media_metadata(
                        request_headers.get(MEDIA_TITLE_HEADER, "")
                    ),
                    _decode_media_metadata(
                        request_headers.get(MEDIA_EXTENSION_HEADER, "")
                    ),
                )
            else:
                filename = safe_filename(url)
            destination = unique_destination(directory_path, filename, reserved)
            download_id = self._database.add(
                url=url,
                destination=str(destination),
                name=destination.name,
                source=parsed.netloc or "BitTorrent",
                status="queued",
                queue_id=queue_id,
                checksum_expected=checksum_expected,
                request_headers=request_headers,
                proxy_url=proxy_url.strip(),
                interface_address=interface_address.strip(),
                torrent_files=torrent_files if is_torrent_url(url) else [],
            )
            expected_size = max(0, int(expected_size))
            if expected_size:
                self._database.update(download_id, total=expected_size)
        except (OSError, RuntimeError, ValueError) as exc:
            self.errorOccurred.emit(f"Could not create the download: {exc}")
            return False

        item = Download(
            download_id,
            url,
            str(destination),
            destination.name,
            parsed.netloc or "BitTorrent",
            queue_id=queue_id,
            created_at=datetime.now().isoformat(timespec="seconds"),
            checksum_expected=checksum_expected,
            request_headers=request_headers,
            proxy_url=proxy_url.strip(),
            interface_address=interface_address.strip(),
            total=expected_size,
            torrent_files=[
                int(index) for index in (torrent_files or [])
            ] if is_torrent_url(url) else [],
        )
        self.beginInsertRows(QModelIndex(), 0, 0)
        self._items.insert(0, item)
        self.endInsertRows()
        if self._sort_key != "date" or not self._sort_descending:
            self.sortDownloads(self._sort_key, self._sort_descending)
        self.statsChanged.emit()
        if start_now:
            self._start(item)
        return True

    def _start(self, item: Download) -> None:
        current = self._threads.get(item.id)
        if current and current.is_alive():
            return
        item.stop_event = threading.Event()
        item.status = "downloading"
        item.error = ""
        item.speed_value = 0
        item.smoothed_speed = 0
        item.last_metrics_emit = 0
        self._database.update(item.id, status="downloading", error="", speed=0)
        row = self._row_for_id(item.id)
        self._emit_row(row, [self.StatusRole, self.ErrorRole, self.SpeedRole])

        interface_address = (
            item.interface_address or self._settings.interfaceAddress
        )
        thread = threading.Thread(
            target=self._run_download,
            args=(
                item.id,
                item.url,
                Path(item.destination),
                item.stop_event,
                item.connection_limit or self._settings.connections_for_url(item.url),
                item.speed_limit or self._settings.speedLimitKbps,
                item.checksum_expected,
                item.request_headers,
                item.proxy_url or self._settings.proxyUrl,
                interface_address,
                []
                if interface_address
                else self._settings.interface_addresses(),
                item.torrent_files,
                item.total,
            ),
            name=f"pidm-download-{item.id}",
            daemon=True,
        )
        self._threads[item.id] = thread
        thread.start()

    def _run_download(
        self,
        download_id: int,
        url: str,
        destination: Path,
        stop_event: threading.Event,
        connections: int,
        speed_limit_kbps: int,
        checksum_expected: str,
        request_headers: dict[str, str],
        proxy_url: str,
        interface_address: str,
        interface_addresses: list[str],
        torrent_files: list[int],
        expected_size: int,
    ) -> None:
        try:
            progress = lambda received, total, speed: self._bridge.progress.emit(
                download_id, received, total, speed
            )
            worker_headers = dict(request_headers)
            media_source = _decode_media_metadata(
                worker_headers.pop(MEDIA_SOURCE_HEADER, "")
            )
            media_format = _decode_media_metadata(
                worker_headers.pop(MEDIA_FORMAT_HEADER, "")
            )
            worker_headers.pop(MEDIA_TITLE_HEADER, None)
            worker_headers.pop(MEDIA_EXTENSION_HEADER, None)
            media_cookie_browser = _decode_media_metadata(
                worker_headers.pop(MEDIA_COOKIE_BROWSER_HEADER, "")
            )
            if is_torrent_url(url):
                self._bridge.modeChanged.emit(download_id, 1)
                result = download_torrent(
                    url,
                    destination.parent,
                    stop_event,
                    progress,
                    speed_limit=speed_limit_kbps * 1024,
                    request_headers=request_headers,
                    proxy_url=proxy_url,
                    selected_files=torrent_files,
                    expected_size=expected_size,
                )
            elif media_source:
                self._bridge.modeChanged.emit(download_id, 1)
                result = download_media(
                    media_source,
                    destination,
                    media_format,
                    stop_event,
                    progress,
                    request_headers=worker_headers,
                    proxy_url=proxy_url,
                    interface_address=interface_address,
                    cookie_browser=media_cookie_browser,
                    speed_limit=speed_limit_kbps * 1024,
                    concurrent_fragments=connections,
                )
            else:
                result = download_http(
                    url,
                    destination,
                    stop_event,
                    progress,
                    on_mode=lambda count: self._bridge.modeChanged.emit(
                        download_id, count
                    ),
                    on_retry=lambda attempt, message: self._bridge.retryChanged.emit(
                        download_id, attempt, message
                    ),
                    connections=connections,
                    speed_limit=speed_limit_kbps * 1024,
                    max_retries=self._settings.maxRetries,
                    retry_delay=self._settings.retryDelay,
                    request_headers=worker_headers,
                    proxy_url=proxy_url,
                    interface_address=interface_address,
                    interface_addresses=interface_addresses,
                )
            checksum_actual = ""
            if result.status == "complete" and checksum_expected:
                digest = hashlib.sha256()
                with destination.open("rb") as source:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                checksum_actual = digest.hexdigest()
                if checksum_actual != checksum_expected:
                    self._bridge.finished.emit(
                        download_id,
                        "error",
                        "SHA-256 verification failed. The downloaded file was kept for inspection.",
                        checksum_actual,
                    )
                    return
            self._bridge.finished.emit(download_id, result.status, "", checksum_actual)
        except Exception as exc:
            self._bridge.finished.emit(download_id, "error", str(exc), "")

    @Slot(int, int)
    def _on_mode_changed(self, download_id: int, connections: int) -> None:
        item = self._item_by_id(download_id)
        if item:
            item.connections = connections

    @Slot(int, int, str)
    def _on_retry_changed(
        self, download_id: int, attempt: int, message: str
    ) -> None:
        item = self._item_by_id(download_id)
        if item:
            item.retry_attempt = attempt
            item.retry_message = message

    @Slot(int, "qlonglong", "qlonglong", float)
    def _on_progress(self, download_id: int, downloaded: int, total: int, speed: float) -> None:
        row = self._row_for_id(download_id)
        if row < 0:
            return
        item = self._items[row]
        item.downloaded = max(0, downloaded)
        item.total = max(0, total) or item.total
        sample_speed = max(0.0, speed)
        if item.smoothed_speed <= 0:
            item.smoothed_speed = sample_speed
        else:
            smoothing = 0.08 if sample_speed == 0 else 0.18
            item.smoothed_speed += smoothing * (
                sample_speed - item.smoothed_speed
            )
        now = time.monotonic()
        metrics_due = (
            item.last_metrics_emit == 0
            or now - item.last_metrics_emit >= 1.0
        )
        if metrics_due:
            item.last_metrics_emit = now
            item.speed_value = item.smoothed_speed
        self._database.update(
            download_id,
            downloaded=item.downloaded,
            total=item.total,
            speed=item.speed_value,
        )
        roles = [self.ProgressRole, self.SizeRole]
        if metrics_due:
            roles.extend([self.SpeedRole, self.EtaRole])
        self._emit_row(
            row,
            roles,
            notify_stats=metrics_due,
        )

    @Slot(int, str, str, str)
    def _on_finished(
        self, download_id: int, status: str, error: str, checksum_actual: str
    ) -> None:
        self._threads.pop(download_id, None)
        row = self._row_for_id(download_id)
        if row < 0:
            discarded_path = self._discard_paths.pop(download_id, None)
            if discarded_path:
                self._delete_partial_files(discarded_path)
            return
        item = self._items[row]
        item.status = status
        item.speed_value = 0
        item.smoothed_speed = 0
        item.error = error
        if checksum_actual:
            item.checksum_actual = checksum_actual
        if status == "complete":
            item.finished_at = datetime.now().isoformat(timespec="seconds")
        self._database.update(
            download_id,
            status=status,
            speed=0,
            error=error,
            checksum_actual=item.checksum_actual,
        )
        if status == "complete":
            self._database.update(download_id, finished_at=item.finished_at)
            self.downloadCompleted.emit(item.name)
        elif status == "error":
            self.downloadFailed.emit(item.name)
        self._emit_row(
            row, [self.StatusRole, self.SpeedRole, self.EtaRole, self.ErrorRole]
        )
        if error:
            self.errorOccurred.emit(f"{item.name}: {error}")
        if item.queue_id in self._queue_limits:
            delay = self._queue_delays.get(item.queue_id, 0)
            if delay:
                QTimer.singleShot(
                    delay * 1000,
                    lambda queue_id=item.queue_id: self._pump_queue(queue_id),
                )
            else:
                self._pump_queue(item.queue_id)

    @Slot(int)
    def togglePaused(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        item = self._items[row]
        if item.status == "downloading":
            item.stop_event.set()
            item.status = "pausing"
            self._emit_row(row, [self.StatusRole])
        elif item.status in {"paused", "error", "queued"}:
            self._start(item)

    @Slot(int)
    def openDownload(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        path = Path(self._items[row].destination)
        target = path if path.exists() else path.parent
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))

    def _item_by_id(self, download_id: int) -> Download | None:
        return next((item for item in self._items if item.id == download_id), None)

    def active_downloads_for_tray(self) -> list[tuple[int, str, int]]:
        return [
            (
                item.id,
                item.name,
                min(100, max(0, int(item.downloaded * 100 / item.total)))
                if item.total
                else 0,
            )
            for item in self._items
            if item.status in {"downloading", "pausing"}
        ]

    @staticmethod
    def _partial_paths(destination: Path) -> list[Path]:
        paths = [destination.with_name(destination.name + ".part")]
        paths.extend(
            sorted(destination.parent.glob(destination.name + ".part.*"))
        )
        return paths

    def _delete_partial_files(self, destination: Path) -> None:
        failures: list[str] = []
        for path in self._partial_paths(destination):
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                failures.append(f"{path.name}: {exc}")
        if failures:
            self.errorOccurred.emit(
                "Could not remove all partial download files: "
                + "; ".join(failures)
            )

    @Slot(int, result="QVariantMap")
    def detailsById(self, download_id: int) -> dict:
        item = self._item_by_id(download_id)
        if not item:
            return {}
        progress = (
            100
            if item.status == "complete"
            else min(100, max(0, int(item.downloaded * 100 / item.total)))
            if item.total
            else 0
        )
        remaining = max(item.total - item.downloaded, 0)
        eta = remaining / item.speed_value if item.speed_value > 0 else 0
        return {
            "id": item.id,
            "name": item.name,
            "url": item.url,
            "source": item.source,
            "destination": item.destination,
            "directory": str(Path(item.destination).parent),
            "status": item.status,
            "protocol": (
                "TORRENT"
                if is_torrent_url(item.url)
                else "HTTPS"
                if item.url.startswith("https:")
                else "HTTP"
            ),
            "progress": progress,
            "downloaded": format_bytes(item.downloaded),
            "total": format_bytes(item.total) if item.total else "Unknown",
            "speed": format_speed(
                item.speed_value, self._settings.speedUnit
            ),
            "eta": "Done" if item.status == "complete" else format_duration(eta),
            "queue": self._database.queue_name(item.queue_id),
            "createdAt": item.created_at,
            "finishedAt": item.finished_at,
            "error": item.error,
            "connections": item.connections,
            "connectionMode": (
                f"{item.connections} parallel connections"
                if item.connections > 1
                else "Single connection"
            ),
            "remaining": format_bytes(max(item.total - item.downloaded, 0))
            if item.total
            else "Unknown",
            "fileType": Path(item.name).suffix.lstrip(".").upper() or "FILE",
            "retryAttempt": item.retry_attempt,
            "retryMessage": item.retry_message,
            "speedLimit": (
                f"{item.speed_limit or self._settings.speedLimitKbps} KB/s"
                if item.speed_limit or self._settings.speedLimitKbps
                else "Unlimited"
            ),
            "retryState": (
                f"Retry {item.retry_attempt}: {item.retry_message}"
                if item.retry_attempt
                else "No retries"
            ),
            "speedLimitValue": item.speed_limit,
            "connectionLimitValue": item.connection_limit,
            "effectiveConnections": (
                item.connection_limit or self._settings.connections_for_url(item.url)
            ),
            "canEditOptions": (
                item.status not in {"downloading", "pausing"}
                and not is_torrent_url(item.url)
            ),
            "checksumExpected": item.checksum_expected,
            "checksumActual": item.checksum_actual,
            "checksumStatus": (
                "Verified"
                if item.checksum_expected
                and item.checksum_actual == item.checksum_expected
                else "Failed"
                if item.checksum_actual
                else "Pending"
                if item.checksum_expected
                else "Not requested"
            ),
            "requestHeaders": "; ".join(
                f"{name}: {value}"
                for name, value in item.request_headers.items()
                if name not in MEDIA_METADATA_HEADERS
            ),
            "proxyUrl": item.proxy_url or self._settings.proxyUrl or "Direct",
            "interfaceAddress": (
                item.interface_address
                or self._settings.interfaceAddress
                or (
                    ", ".join(self._settings.interface_addresses())
                    if self._settings.interface_addresses()
                    else "Automatic"
                )
            ),
        }

    @Slot(int, str, int, int, result=bool)
    def updateDownloadOptions(
        self,
        download_id: int,
        directory: str,
        speed_limit_kbps: int,
        connection_limit: int,
    ) -> bool:
        item = self._item_by_id(download_id)
        if not item:
            return False
        if item.status in {"downloading", "pausing"}:
            self.errorOccurred.emit("Pause the download before changing its options.")
            return False
        target_directory = (
            Path(QUrl(directory).toLocalFile())
            if directory.startswith("file:")
            else Path(directory)
        )
        try:
            target_directory.mkdir(parents=True, exist_ok=True)
            old_destination = Path(item.destination)
            new_destination = target_directory / old_destination.name
            if new_destination != old_destination:
                old_files = [old_destination, *self._partial_paths(old_destination)]
                existing_sources = [path for path in old_files if path.exists()]
                targets = [
                    new_destination
                    if path == old_destination
                    else new_destination.with_name(
                        new_destination.name + path.name[len(old_destination.name):]
                    )
                    for path in existing_sources
                ]
                if any(path.exists() for path in targets):
                    self.errorOccurred.emit(
                        "A file with the same name already exists in that folder."
                    )
                    return False
                moved: list[tuple[Path, Path]] = []
                try:
                    for source, target in zip(existing_sources, targets):
                        shutil.move(str(source), str(target))
                        moved.append((source, target))
                except OSError:
                    for source, target in reversed(moved):
                        if target.exists() and not source.exists():
                            try:
                                shutil.move(str(target), str(source))
                            except OSError:
                                pass
                    raise
                item.destination = str(new_destination)
            item.speed_limit = max(0, int(speed_limit_kbps))
            item.connection_limit = max(0, min(8, int(connection_limit)))
            self._database.update(
                item.id,
                destination=item.destination,
                speed_limit=item.speed_limit,
                connection_limit=item.connection_limit,
            )
        except (OSError, ValueError) as exc:
            self.errorOccurred.emit(f"Could not update download options: {exc}")
            return False
        row = self._row_for_id(download_id)
        self._emit_row(row, [self.PathRole])
        return True

    @Slot(int)
    def togglePausedById(self, download_id: int) -> None:
        row = self._row_for_id(download_id)
        if row >= 0:
            self.togglePaused(row)

    @Slot(int)
    def openDownloadById(self, download_id: int) -> None:
        row = self._row_for_id(download_id)
        if row >= 0:
            self.openDownload(row)

    @Slot(int)
    def openFolderById(self, download_id: int) -> None:
        item = self._item_by_id(download_id)
        if item:
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(Path(item.destination).parent)))

    @Slot(int)
    def copyUrlById(self, download_id: int) -> None:
        item = self._item_by_id(download_id)
        if item:
            QGuiApplication.clipboard().setText(item.url)

    @Slot(int, str, result=bool)
    def refreshDownloadUrl(self, download_id: int, url: str) -> bool:
        item = self._item_by_id(download_id)
        if not item or item.status in {"downloading", "pausing", "complete"}:
            return False
        url = url.strip()
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            self.errorOccurred.emit(
                "The refreshed download link must be a complete HTTP or HTTPS URL."
            )
            return False
        duplicate = next(
            (
                existing
                for existing in self._items
                if existing.id != item.id
                and existing.url == url
                and existing.status != "complete"
            ),
            None,
        )
        if duplicate:
            self.errorOccurred.emit(
                f"This URL is already in PIDM as '{duplicate.name}'."
            )
            return False
        item.url = url
        item.source = parsed.netloc
        item.error = ""
        item.retry_attempt = 0
        item.retry_message = ""
        if item.status == "error":
            item.status = "paused"
        self._database.update(
            item.id,
            url=item.url,
            source=item.source,
            status=item.status,
            error="",
        )
        row = self._row_for_id(download_id)
        self._emit_row(
            row,
            [
                self.SourceRole,
                self.StatusRole,
                self.ErrorRole,
            ],
        )
        return True

    @Slot(str, result=bool)
    def exportDownloadList(self, path: str) -> bool:
        target = Path(QUrl(path).toLocalFile() if path.startswith("file:") else path)
        if not target.suffix:
            target = target.with_suffix(".json")
        payload = {
            "format": "pidm-download-list",
            "version": 1,
            "downloads": [
                {
                    "url": item.url,
                    "directory": str(Path(item.destination).parent),
                    "checksumSha256": item.checksum_expected,
                }
                for item in self._items
            ],
        }
        try:
            target.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as exc:
            self.errorOccurred.emit(f"Could not export the download list: {exc}")
            return False
        return True

    @Slot(str, result=int)
    def importDownloadList(self, path: str) -> int:
        source = Path(QUrl(path).toLocalFile() if path.startswith("file:") else path)
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
            records = payload.get("downloads", []) if isinstance(payload, dict) else payload
            if not isinstance(records, list):
                raise ValueError("The file does not contain a download list.")
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            self.errorOccurred.emit(f"Could not import the download list: {exc}")
            return 0
        imported = 0
        for record in records:
            if not isinstance(record, dict):
                continue
            if self._add_download(
                str(record.get("url", "")),
                str(record.get("directory", self.defaultDownloadPath)),
                self._database.main_queue_id(),
                False,
                str(record.get("checksumSha256", "")),
            ):
                imported += 1
        return imported

    @Slot(int)
    def removeDownload(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        item = self._items[row]
        if item.status in {"downloading", "pausing"}:
            item.stop_event.set()
            self._discard_paths[item.id] = Path(item.destination)
        else:
            self._delete_partial_files(Path(item.destination))
        self.beginRemoveRows(QModelIndex(), row, row)
        self._items.pop(row)
        self.endRemoveRows()
        self._database.remove(item.id)
        self.statsChanged.emit()

    @Slot()
    def pauseAll(self) -> None:
        for row, item in enumerate(self._items):
            if item.status == "downloading":
                item.stop_event.set()
                item.status = "pausing"
                self._emit_row(row, [self.StatusRole])

    @Slot()
    def resumeAll(self) -> None:
        for item in self._items:
            if item.status in {"paused", "error", "queued"}:
                self._start(item)

    @Slot(int)
    def retryQueueErrors(self, queue_id: int) -> None:
        for item in self._items:
            if item.queue_id == queue_id and item.status == "error":
                item.status = "queued"
                item.error = ""
                self._database.update(item.id, status="queued", error="")
                row = self._row_for_id(item.id)
                self._emit_row(row, [self.StatusRole, self.ErrorRole])

    @Slot()
    def clearCompleted(self) -> None:
        for row in range(len(self._items) - 1, -1, -1):
            if self._items[row].status == "complete":
                self.removeDownload(row)

    @Slot(int, int)
    def assignQueue(self, download_id: int, queue_id: int) -> None:
        item = self._item_by_id(download_id)
        if not item or item.status == "downloading":
            return
        item.queue_id = queue_id
        item.queue_position = sum(
            existing.queue_id == queue_id for existing in self._items
        )
        self._database.update(
            item.id, queue_id=queue_id, queue_position=item.queue_position
        )
        row = self._row_for_id(download_id)
        self._emit_row(row, [self.QueueNameRole])

    @Slot(int, int, int)
    def startQueue(
        self, queue_id: int, max_concurrent: int, pause_between: int
    ) -> None:
        self._queue_limits[queue_id] = max(1, max_concurrent)
        self._queue_delays[queue_id] = max(0, pause_between)
        self._pump_queue(queue_id)

    def _pump_queue(self, queue_id: int) -> None:
        limit = self._queue_limits.get(queue_id)
        if not limit:
            return
        active = [
            item for item in self._items
            if item.queue_id == queue_id and item.status in {"downloading", "pausing"}
        ]
        waiting = sorted(
            (
                item for item in self._items
                if item.queue_id == queue_id
                and item.status in {"queued", "paused"}
            ),
            key=lambda item: (item.queue_position, item.id),
        )
        for item in waiting[: max(0, limit - len(active))]:
            self._start(item)
        if not active and not waiting:
            self._queue_limits.pop(queue_id, None)
            self._queue_delays.pop(queue_id, None)
            has_errors = any(
                item.queue_id == queue_id and item.status == "error"
                for item in self._items
            )
            if not has_errors:
                self.queueCompleted.emit(queue_id)
            self.queueActivityChanged.emit(queue_id, False)
        else:
            self.queueActivityChanged.emit(queue_id, True)

    @Slot(int)
    def stopQueue(self, queue_id: int) -> None:
        self._queue_limits.pop(queue_id, None)
        self._queue_delays.pop(queue_id, None)
        for row, item in enumerate(self._items):
            if item.queue_id == queue_id and item.status == "downloading":
                item.stop_event.set()
                item.status = "pausing"
                self._emit_row(row, [self.StatusRole])
        self.queueActivityChanged.emit(queue_id, False)

    @Slot()
    def shutdown(self) -> None:
        for item in self._items:
            if item.status in {"downloading", "pausing"}:
                item.stop_event.set()
        deadline = time.monotonic() + 5.0
        for thread in list(self._threads.values()):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)
