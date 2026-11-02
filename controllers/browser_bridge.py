from __future__ import annotations

import json
import secrets
import time
from urllib.parse import urlparse

from PySide6.QtCore import QObject, Property, Signal, Slot
from PySide6.QtNetwork import QHostAddress, QTcpServer


class BrowserBridge(QObject):
    batchReceived = Signal("QVariantList", str)
    singleUrlReceived = Signal(str)
    refreshLinkReceived = Signal(int, str)
    refreshWaitingChanged = Signal()

    PORT = 17654
    MAX_REQUEST_SIZE = 1024 * 1024
    MAX_BATCH_SIZE = 250

    def __init__(self, protocol_capabilities, parent=None):
        super().__init__(parent)
        self._protocol_capabilities = protocol_capabilities
        self._server = QTcpServer(self)
        self._buffers: dict[object, bytearray] = {}
        self._pending_media: dict[str, tuple[object, str, str]] = {}
        self._media_choices: dict[str, tuple[float, str, str, dict]] = {}
        self._refresh_download_id = -1
        protocol_capabilities.browserMediaFormatsResolved.connect(
            self._media_formats_ready
        )
        protocol_capabilities.browserMediaError.connect(self._media_error)
        self._server.newConnection.connect(self._accept_connections)
        self._server.listen(
            QHostAddress(QHostAddress.SpecialAddress.LocalHost),
            self.PORT,
        )

    def _accept_connections(self) -> None:
        while self._server.hasPendingConnections():
            socket = self._server.nextPendingConnection()
            self._buffers[socket] = bytearray()
            socket.readyRead.connect(
                lambda current=socket: self._read_request(current)
            )
            socket.disconnected.connect(
                lambda current=socket: self._socket_disconnected(current)
            )

    def _socket_disconnected(self, socket) -> None:
        self._buffers.pop(socket, None)
        abandoned = [
            request_id
            for request_id, pending in self._pending_media.items()
            if pending[0] is socket
        ]
        for request_id in abandoned:
            self._pending_media.pop(request_id, None)
        socket.deleteLater()

    def _read_request(self, socket) -> None:
        buffer = self._buffers.get(socket)
        if buffer is None:
            return
        buffer.extend(bytes(socket.readAll()))
        if len(buffer) > self.MAX_REQUEST_SIZE:
            self._respond(socket, 413, {"error": "Request is too large."})
            return
        header_end = buffer.find(b"\r\n\r\n")
        if header_end < 0:
            return
        try:
            header_text = bytes(buffer[:header_end]).decode("iso-8859-1")
            lines = header_text.split("\r\n")
            method, path, _version = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                name, separator, value = line.partition(":")
                if separator:
                    headers[name.strip().lower()] = value.strip()
            content_length = int(headers.get("content-length", "0") or 0)
        except (UnicodeDecodeError, ValueError):
            self._respond(socket, 400, {"error": "Malformed HTTP request."})
            return
        if content_length < 0 or content_length > self.MAX_REQUEST_SIZE:
            self._respond(socket, 413, {"error": "Request is too large."})
            return
        body_start = header_end + 4
        if len(buffer) < body_start + content_length:
            return
        origin = headers.get("origin", "")
        if not self._origin_allowed(origin):
            self._respond(socket, 403, {"error": "Origin is not allowed."}, origin)
            return
        body = bytes(buffer[body_start:body_start + content_length])
        self._dispatch(socket, method.upper(), path, body, origin)

    @staticmethod
    def _origin_allowed(origin: str) -> bool:
        return (
            not origin
            or origin == "null"
            or origin.startswith(
                (
                    "chrome-extension://",
                    "moz-extension://",
                    "safari-web-extension://",
                )
            )
        )

    def _dispatch(
        self,
        socket,
        method: str,
        path: str,
        body: bytes,
        origin: str,
    ) -> None:
        if method == "OPTIONS":
            self._respond(socket, 204, None, origin)
            return
        if method == "GET" and path == "/health":
            self._respond(
                socket,
                200,
                {
                    "application": "PIDM",
                    "ready": True,
                    "version": 2,
                    "refreshPending": self.refreshWaiting,
                },
                origin,
            )
            return
        if method != "POST" or path not in {
            "/batch",
            "/single",
            "/media/formats",
            "/media/download",
        }:
            self._respond(socket, 404, {"error": "Unknown endpoint."}, origin)
            return
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._respond(socket, 400, {"error": "Invalid JSON body."}, origin)
            return
        if path in {"/single", "/media/formats"}:
            url = self._valid_url(str(payload.get("url", "")))
            if not url:
                self._respond(socket, 422, {"error": "Invalid download URL."}, origin)
                return
            if path == "/media/formats":
                request_id = secrets.token_urlsafe(18)
                self._buffers.pop(socket, None)
                self._pending_media[request_id] = (socket, origin, url)
                self._protocol_capabilities.resolveMediaForBrowser(
                    request_id,
                    url,
                )
                return
            if self._refresh_download_id >= 0:
                download_id = self._refresh_download_id
                self._refresh_download_id = -1
                self.refreshWaitingChanged.emit()
                self.refreshLinkReceived.emit(download_id, url)
                self._respond(
                    socket,
                    202,
                    {"accepted": 1, "refreshedDownloadId": download_id},
                    origin,
                )
            else:
                self.singleUrlReceived.emit(url)
                self._respond(socket, 202, {"accepted": 1}, origin)
            return
        if path == "/media/download":
            self._expire_media_choices()
            token = str(payload.get("token", ""))
            choice = self._media_choices.pop(token, None)
            if not choice:
                self._respond(
                    socket,
                    422,
                    {"error": "This quality selection expired. Resolve the page again."},
                    origin,
                )
                return
            _expires, page_url, title, media_format = choice
            headers = "; ".join(
                part
                for part in (
                    str(media_format.get("headers", "")),
                    str(media_format.get("internalHeaders", "")),
                )
                if part
            )
            item = {
                "url": media_format["url"],
                "title": f"{title} — {media_format['quality']}",
                "kind": (
                    "audio"
                    if media_format["kind"] == "audio"
                    else "video"
                ),
                "pageUrl": page_url,
                "headers": headers,
                "expectedSize": int(media_format.get("sizeBytes", 0) or 0),
                "selected": True,
            }
            self.batchReceived.emit([item], title)
            self._respond(socket, 202, {"accepted": 1}, origin)
            return
        links = self._normalize_links(payload.get("links", []))
        if not links:
            self._respond(socket, 422, {"error": "No valid links were supplied."}, origin)
            return
        source_title = str(payload.get("sourceTitle", "")).strip()[:200]
        self.batchReceived.emit(links, source_title)
        self._respond(socket, 202, {"accepted": len(links)}, origin)

    @Property(bool, notify=refreshWaitingChanged)
    def refreshWaiting(self) -> bool:
        return self._refresh_download_id >= 0

    @Property(int, notify=refreshWaitingChanged)
    def refreshDownloadId(self) -> int:
        return self._refresh_download_id

    @Slot(int)
    def waitForRefresh(self, download_id: int) -> None:
        if download_id < 0:
            return
        if self._refresh_download_id != download_id:
            self._refresh_download_id = download_id
            self.refreshWaitingChanged.emit()

    @Slot(int)
    def cancelRefresh(self, download_id: int) -> None:
        if self._refresh_download_id == download_id:
            self._refresh_download_id = -1
            self.refreshWaitingChanged.emit()

    @Slot(str, str, "QVariantList")
    def _media_formats_ready(
        self,
        request_id: str,
        title: str,
        formats: list,
    ) -> None:
        pending = self._pending_media.pop(request_id, None)
        if not pending:
            return
        socket, origin, page_url = pending
        self._expire_media_choices()
        grouped: dict[str, list[dict]] = {
            "combined": [],
            "video": [],
            "audio": [],
        }
        seen = set()
        for media_format in formats:
            kind = str(media_format.get("kind", "video"))
            if kind not in grouped:
                kind = "video"
            key = (kind, str(media_format.get("quality", "Media")))
            if key in seen:
                continue
            seen.add(key)
            grouped[kind].append(media_format)

        selected_groups = {
            "combined": grouped["combined"][:15],
            "video": grouped["video"][:12],
            "audio": grouped["audio"][:8],
        }

        def make_public(media_format: dict) -> dict:
            token = secrets.token_urlsafe(24)
            self._media_choices[token] = (
                time.monotonic() + 15 * 60,
                page_url,
                title,
                dict(media_format),
            )
            return {
                "token": token,
                "quality": str(media_format.get("quality", "Media")),
                "details": str(media_format.get("details", "")),
                "kind": str(media_format.get("kind", "video")),
                "size": str(media_format.get("size", "Unknown size")),
            }

        combined = [
            make_public(media_format)
            for media_format in selected_groups["combined"]
        ]
        video_only = [
            make_public(media_format)
            for media_format in selected_groups["video"]
        ]
        audio_only = [
            make_public(media_format)
            for media_format in selected_groups["audio"]
        ]
        if combined:
            recommended = combined
            recommended_label = "Video with audio"
        elif video_only:
            recommended = video_only
            video_only = []
            recommended_label = "Video formats"
        else:
            recommended = audio_only
            audio_only = []
            recommended_label = "Audio formats"

        public_formats = [*recommended, *video_only, *audio_only]
        if not public_formats:
            self._respond(
                socket,
                422,
                {"error": "No downloadable qualities were found."},
                origin,
            )
            return
        self._respond(
            socket,
            200,
            {
                "title": title,
                "formats": recommended,
                "recommendedLabel": recommended_label,
                "videoOnlyFormats": video_only,
                "audioFormats": audio_only,
                "totalFormats": len(public_formats),
            },
            origin,
        )

    @Slot(str, str)
    def _media_error(self, request_id: str, message: str) -> None:
        pending = self._pending_media.pop(request_id, None)
        if not pending:
            return
        socket, origin, _page_url = pending
        self._respond(socket, 422, {"error": message}, origin)

    def _expire_media_choices(self) -> None:
        now = time.monotonic()
        expired = [
            token
            for token, choice in self._media_choices.items()
            if choice[0] <= now
        ]
        for token in expired:
            self._media_choices.pop(token, None)

    @classmethod
    def _normalize_links(cls, raw_links) -> list[dict]:
        if not isinstance(raw_links, list):
            return []
        normalized = []
        seen = set()
        for raw in raw_links[: cls.MAX_BATCH_SIZE]:
            if not isinstance(raw, dict):
                continue
            url = cls._valid_url(str(raw.get("url", "")))
            if not url or url in seen:
                continue
            seen.add(url)
            page_url = cls._valid_url(str(raw.get("pageUrl", "")))
            title = str(raw.get("title", "")).strip()[:300]
            kind = str(raw.get("kind", "link")).strip().lower()
            if kind not in {"link", "file", "video", "audio", "stream"}:
                kind = "link"
            headers = ""
            if page_url and page_url != url:
                headers = f"Referer: {page_url}"
            normalized.append(
                {
                    "url": url,
                    "title": title or url.rsplit("/", 1)[-1] or url,
                    "kind": kind,
                    "pageUrl": page_url,
                    "headers": headers,
                    "expectedSize": 0,
                    "selected": True,
                }
            )
        return normalized

    @staticmethod
    def _valid_url(value: str) -> str:
        value = value.strip()
        parsed = urlparse(value)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return value
        if parsed.scheme == "magnet" and value.lower().startswith("magnet:?xt=urn:btih:"):
            return value
        return ""

    @staticmethod
    def _respond(socket, status: int, payload, origin: str = "") -> None:
        reasons = {
            200: "OK",
            202: "Accepted",
            204: "No Content",
            400: "Bad Request",
            403: "Forbidden",
            404: "Not Found",
            413: "Payload Too Large",
            422: "Unprocessable Content",
        }
        body = (
            b""
            if payload is None
            else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        allowed_origin = (
            origin
            if origin.startswith(
                (
                    "chrome-extension://",
                    "moz-extension://",
                    "safari-web-extension://",
                )
            )
            else "*"
        )
        response = (
            f"HTTP/1.1 {status} {reasons.get(status, '')}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Content-Type: application/json; charset=utf-8\r\n"
            f"Access-Control-Allow-Origin: {allowed_origin}\r\n"
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type\r\n"
            "Access-Control-Allow-Private-Network: true\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii") + body
        socket.write(response)
        socket.disconnectFromHost()
