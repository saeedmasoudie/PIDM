from __future__ import annotations

import base64
import importlib.util
import re
import threading

from PySide6.QtCore import QObject, Property, Signal, Slot


class ProtocolCapabilities(QObject):
    mediaResolved = Signal(str, str, str)
    mediaFormatsResolved = Signal(str, "QVariantList")
    browserMediaFormatsResolved = Signal(str, str, "QVariantList")
    browserMediaError = Signal(str, str)
    torrentFilesResolved = Signal(str, "QVariantList")
    errorOccurred = Signal(str)

    def __init__(self, settings=None, parent=None):
        super().__init__(parent)
        self._settings = settings

    @Property(bool, constant=True)
    def mediaExtractionAvailable(self):
        return importlib.util.find_spec("yt_dlp") is not None

    @Property("QVariantList", constant=True)
    def statuses(self):
        return [
            {
                "nameKey": "networkRouting",
                "available": True,
                "detailKey": "networkRoutingDetail",
            },
            {
                "nameKey": "torrentDownloads",
                "available": importlib.util.find_spec("libtorrent") is not None,
                "detailKey": "torrentDownloadsDetail",
            },
            {
                "nameKey": "http3",
                "available": importlib.util.find_spec("aioquic") is not None,
                "detailKey": "http3Detail",
            },
            {
                "nameKey": "mediaExtraction",
                "available": (
                    self.mediaExtractionAvailable
                ),
                "detailKey": "mediaExtractionDetail",
            },
        ]

    @Slot(str, result=bool)
    def acceptsUrl(self, url: str) -> bool:
        return url.strip().lower().startswith(("http://", "https://", "magnet:"))

    @Slot(str, result=bool)
    def shouldResolveMedia(self, url: str) -> bool:
        url = url.strip()
        if not url.lower().startswith(("http://", "https://")):
            return False
        if importlib.util.find_spec("yt_dlp") is None:
            return False
        try:
            from yt_dlp.extractor import gen_extractor_classes

            return any(
                extractor.IE_NAME != "generic" and extractor.suitable(url)
                for extractor in gen_extractor_classes()
            )
        except Exception:
            return False

    @Slot(str, result=bool)
    def shouldResolveTorrent(self, url: str) -> bool:
        parsed = url.strip().lower()
        return parsed.startswith("magnet:") or parsed.split("?", 1)[0].endswith(
            ".torrent"
        )

    @Slot(str)
    def resolveTorrent(self, url: str) -> None:
        url = url.strip()
        settings_snapshot = {}
        if self._settings is not None:
            settings_snapshot = {
                "proxy": self._settings.proxyUrl,
                "user_agent": self._settings.userAgent,
            }
        threading.Thread(
            target=self._resolve_torrent_worker,
            args=(url, settings_snapshot),
            name="pidm-torrent-metadata",
            daemon=True,
        ).start()

    def _resolve_torrent_worker(
        self, url: str, settings: dict[str, str]
    ) -> None:
        try:
            from core.torrent_downloader import inspect_torrent

            headers = {}
            if settings.get("user_agent"):
                headers["User-Agent"] = settings["user_agent"]
            title, files = inspect_torrent(
                url,
                request_headers=headers,
                proxy_url=settings.get("proxy", ""),
            )
            self.torrentFilesResolved.emit(title, files)
        except Exception as exc:
            self.errorOccurred.emit(
                f"Could not read torrent metadata: {exc}"
            )

    @Slot(str)
    def resolveMedia(self, url: str) -> None:
        url = url.strip()
        if not self.acceptsUrl(url):
            self.errorOccurred.emit("Enter a complete HTTP or HTTPS media page URL.")
            return
        if importlib.util.find_spec("yt_dlp") is None:
            self.errorOccurred.emit(
                "Media extraction requires the optional yt-dlp Python package."
            )
            return
        settings_snapshot = {}
        if self._settings is not None:
            settings_snapshot = {
                "proxy": self._settings.proxyUrl,
                "source_address": self._settings.interfaceAddress,
                "user_agent": self._settings.userAgent,
                "cookie_browser": self._settings.mediaCookieBrowser,
            }
        threading.Thread(
            target=self._resolve_media_worker,
            args=(url, settings_snapshot),
            name="pidm-media-resolver",
            daemon=True,
        ).start()

    @Slot(str, str)
    def resolveMediaForBrowser(self, request_id: str, url: str) -> None:
        url = url.strip()
        if not self.acceptsUrl(url):
            self.browserMediaError.emit(
                request_id,
                "Enter a complete HTTP or HTTPS media page URL.",
            )
            return
        if importlib.util.find_spec("yt_dlp") is None:
            self.browserMediaError.emit(
                request_id,
                "Media extraction requires the optional yt-dlp Python package.",
            )
            return
        settings_snapshot = {}
        if self._settings is not None:
            settings_snapshot = {
                "proxy": self._settings.proxyUrl,
                "source_address": self._settings.interfaceAddress,
                "user_agent": self._settings.userAgent,
                "cookie_browser": self._settings.mediaCookieBrowser,
            }
        threading.Thread(
            target=self._resolve_media_worker,
            args=(url, settings_snapshot, request_id),
            name="pidm-browser-media-resolver",
            daemon=True,
        ).start()

    def _resolve_media_worker(
        self,
        url: str,
        settings: dict[str, str],
        browser_request_id: str = "",
    ) -> None:
        try:
            import yt_dlp
            from yt_dlp.networking.impersonate import ImpersonateTarget

            messages: list[str] = []

            class Logger:
                def debug(self, message):
                    pass

                def info(self, message):
                    pass

                def warning(self, message):
                    messages.append(str(message))

                def error(self, message):
                    messages.append(str(message))

            options = {
                "quiet": True,
                "no_warnings": False,
                "noplaylist": True,
                "logger": Logger(),
                "socket_timeout": 20,
                "retries": 2,
                "extractor_retries": 2,
                "impersonate": ImpersonateTarget(client="chrome"),
                "js_runtimes": {"deno": {}},
                "format": (
                    "best[protocol=https][vcodec!=none][acodec!=none]"
                    "/best[protocol=http][vcodec!=none][acodec!=none]"
                    "/best[protocol=https]/best[protocol=http]"
                ),
            }
            if settings:
                if settings["proxy"]:
                    options["proxy"] = settings["proxy"]
                if settings["source_address"]:
                    options["source_address"] = settings["source_address"]
                if settings["user_agent"]:
                    options["http_headers"] = {
                        "User-Agent": settings["user_agent"]
                    }
                if settings["cookie_browser"]:
                    options["cookiesfrombrowser"] = (
                        settings["cookie_browser"],
                    )
            with yt_dlp.YoutubeDL(options) as downloader:
                info = downloader.extract_info(url, download=False)
            if "entries" in info:
                info = next((entry for entry in info["entries"] if entry), None)
            if not info or not info.get("url"):
                if not info or not info.get("formats"):
                    raise RuntimeError("No compatible direct HTTP media stream was found.")
            formats = self._build_format_options(info, downloader)
            if formats:
                title = str(info.get("title") or "Media")
                self._attach_download_metadata(
                    formats,
                    url,
                    title,
                    settings,
                )
                if browser_request_id:
                    self.browserMediaFormatsResolved.emit(
                        browser_request_id,
                        title,
                        formats,
                    )
                else:
                    self.mediaFormatsResolved.emit(title, formats)
                return
            headers = dict(info.get("http_headers") or {})
            cookie_header = downloader.cookiejar.get_cookie_header(str(info["url"]))
            if cookie_header:
                headers["Cookie"] = cookie_header
            header_text = "; ".join(
                f"{name}: {value}"
                for name, value in headers.items()
                if name.lower() not in {"host", "content-length", "range"}
            )
            title = str(info.get("title") or "Media")
            if browser_request_id:
                size_bytes = int(
                    info.get("filesize")
                    or info.get("filesize_approx")
                    or 0
                )
                self.browserMediaFormatsResolved.emit(
                    browser_request_id,
                    title,
                    [
                        {
                            "id": str(info.get("format_id") or "direct"),
                            "quality": str(
                                info.get("format_note")
                                or info.get("resolution")
                                or "Best available"
                            ),
                            "details": str(info.get("ext") or "Media").upper(),
                            "kind": "combined",
                            "size": self._format_size(size_bytes),
                            "sizeBytes": size_bytes,
                            "url": str(info["url"]),
                            "headers": header_text,
                            "extension": str(info.get("ext") or "bin").lower(),
                            "height": int(info.get("height") or 0),
                            "rate": float(info.get("abr") or 0),
                        }
                    ],
                )
            else:
                self.mediaResolved.emit(title, str(info["url"]), header_text)
        except Exception as exc:
            raw_error = str(exc).strip() or type(exc).__name__
            if "messages" in locals() and messages:
                raw_error = next(
                    (message for message in reversed(messages) if message.strip()),
                    raw_error,
                )
            raw_error = re.sub(r"\x1b\[[0-9;]*m", "", raw_error)
            raw_error = re.sub(r"^(?:ERROR:\s*)+", "", raw_error).strip()
            if "429" in raw_error or "Too Many Requests" in raw_error:
                guidance = (
                    "YouTube temporarily blocked the request. Wait a few minutes, "
                    "or select your signed-in browser under Settings → Request "
                    "controls → Media cookies."
                )
            elif "CERTIFICATE_VERIFY_FAILED" in raw_error:
                guidance = (
                    "The secure connection could not be verified. Check the PC "
                    "clock and certificate store, and disable HTTPS inspection "
                    "for PIDM in antivirus or proxy software."
                )
            elif any(
                phrase in raw_error.lower()
                for phrase in ("sign in", "cookies", "not a bot", "confirm your age")
            ):
                guidance = (
                    "YouTube requires a signed-in session. Select Edge, Chrome, "
                    "or Firefox under Settings → Request controls → Media cookies."
                )
            else:
                guidance = "yt-dlp could not resolve this page."
            message = f"{guidance}\n\nDetails: {raw_error[:500]}"
            if browser_request_id:
                self.browserMediaError.emit(browser_request_id, message)
            else:
                self.errorOccurred.emit(message)

    @staticmethod
    def _build_format_options(info: dict, downloader) -> list[dict]:
        options = []
        seen = set()
        base_headers = dict(info.get("http_headers") or {})
        for media_format in info.get("formats") or []:
            url = str(media_format.get("url") or "")
            protocol = str(media_format.get("protocol") or "").lower()
            format_id = str(media_format.get("format_id") or "")
            if (
                not url
                or not format_id
                or format_id in seen
                or protocol not in {
                    "http",
                    "https",
                    "m3u8",
                    "m3u8_native",
                    "http_dash_segments",
                }
                or media_format.get("has_drm")
            ):
                continue
            video_codec = str(media_format.get("vcodec") or "none")
            audio_codec = str(media_format.get("acodec") or "none")
            has_video = video_codec != "none"
            has_audio = audio_codec != "none"
            if not has_video and not has_audio:
                continue
            kind = (
                "combined"
                if has_video and has_audio
                else "video"
                if has_video
                else "audio"
            )
            height = int(media_format.get("height") or 0)
            fps = float(media_format.get("fps") or 0)
            audio_rate = float(media_format.get("abr") or 0)
            if has_video:
                quality = f"{height}p" if height else (
                    str(media_format.get("resolution") or "Video")
                )
                if fps:
                    quality += f" · {fps:g} fps"
            else:
                quality = f"Audio · {audio_rate:g} kbps" if audio_rate else "Audio"
            extension = str(media_format.get("ext") or "bin").upper()
            codecs = []
            if has_video:
                codecs.append(video_codec.split(".")[0].upper())
            if has_audio:
                codecs.append(audio_codec.split(".")[0].upper())
            details = f"{extension} · {' + '.join(codecs)}"
            note = str(media_format.get("format_note") or "").strip()
            if note and note.lower() not in quality.lower():
                details += f" · {note}"
            size_bytes = int(
                media_format.get("filesize")
                or media_format.get("filesize_approx")
                or 0
            )
            headers = {
                **base_headers,
                **dict(media_format.get("http_headers") or {}),
            }
            cookie_header = downloader.cookiejar.get_cookie_header(url)
            if cookie_header:
                headers["Cookie"] = cookie_header
            header_text = "; ".join(
                f"{name}: {value}"
                for name, value in headers.items()
                if name.lower() not in {"host", "content-length", "range"}
            )
            options.append(
                {
                    "id": format_id,
                    "quality": quality,
                    "details": details,
                    "kind": kind,
                    "size": ProtocolCapabilities._format_size(size_bytes),
                    "sizeBytes": size_bytes,
                    "url": url,
                    "headers": header_text,
                    "extension": str(media_format.get("ext") or "bin").lower(),
                    "height": height,
                    "rate": audio_rate,
                }
            )
            seen.add(format_id)
        native_combined = {
            item["quality"]
            for item in options
            if item["kind"] == "combined"
        }
        audio_options = sorted(
            (item for item in options if item["kind"] == "audio"),
            key=lambda item: -item["rate"],
        )
        video_options = sorted(
            (item for item in options if item["kind"] == "video"),
            key=lambda item: (
                item["extension"] != "mp4",
                -item["height"],
                item["id"],
            ),
        )
        merged_qualities = set(native_combined)
        merged_options = []
        for video in video_options:
            if video["quality"] in merged_qualities or not audio_options:
                continue
            preferred_extension = (
                "m4a" if video["extension"] == "mp4" else "webm"
            )
            audio = next(
                (
                    item
                    for item in audio_options
                    if item["extension"] == preferred_extension
                ),
                audio_options[0],
            )
            size_bytes = int(video["sizeBytes"]) + int(audio["sizeBytes"])
            merged_options.append(
                {
                    **video,
                    "id": f"{video['id']}+{audio['id']}",
                    "kind": "combined",
                    "details": f"{video['details']} + audio · merged",
                    "size": ProtocolCapabilities._format_size(size_bytes),
                    "sizeBytes": size_bytes,
                    "rate": audio["rate"],
                }
            )
            merged_qualities.add(video["quality"])
        options.extend(merged_options)
        kind_order = {"combined": 0, "video": 1, "audio": 2}
        options.sort(
            key=lambda item: (
                kind_order[item["kind"]],
                -item["height"],
                -item["rate"],
                item["id"],
            )
        )
        return options

    @staticmethod
    def _attach_download_metadata(
        formats: list[dict],
        source_url: str,
        title: str,
        settings: dict[str, str],
    ) -> None:
        def encode(value: str) -> str:
            return base64.urlsafe_b64encode(
                value.encode("utf-8")
            ).decode("ascii")

        for media_format in formats:
            metadata = [
                f"X-PIDM-Media-Source: {encode(source_url)}",
                f"X-PIDM-Media-Format: {encode(str(media_format['id']))}",
                f"X-PIDM-Media-Title: {encode(title)}",
                "X-PIDM-Media-Extension: "
                f"{encode(str(media_format.get('extension') or 'bin'))}",
            ]
            cookie_browser = settings.get("cookie_browser", "")
            if cookie_browser:
                metadata.append(
                    f"X-PIDM-Media-Cookie-Browser: {encode(cookie_browser)}"
                )
            media_format["internalHeaders"] = "; ".join(metadata)

    @staticmethod
    def _format_size(value: int) -> str:
        if value <= 0:
            return "Unknown size"
        size = float(value)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024 or unit == "TB":
                return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
