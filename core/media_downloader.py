from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Callable


ProgressCallback = Callable[[int, int, float], None]


@dataclass(frozen=True, slots=True)
class MediaDownloadResult:
    status: str
    downloaded: int
    total: int
    connections: int = 1


def download_media(
    source_url: str,
    destination: Path,
    format_id: str,
    stop_event: Event,
    on_progress: ProgressCallback,
    *,
    request_headers: dict[str, str] | None = None,
    proxy_url: str = "",
    interface_address: str = "",
    cookie_browser: str = "",
    speed_limit: int = 0,
    concurrent_fragments: int = 1,
) -> MediaDownloadResult:
    import yt_dlp

    downloaded = destination.stat().st_size if destination.exists() else 0
    total = 0
    speed = 0.0
    started_at = time.monotonic()

    def progress_hook(status: dict) -> None:
        nonlocal downloaded, total, speed
        if stop_event.is_set():
            raise yt_dlp.utils.DownloadCancelled("Download paused by user.")
        if status.get("status") not in {"downloading", "finished"}:
            return
        downloaded = int(status.get("downloaded_bytes") or downloaded or 0)
        total = int(
            status.get("total_bytes")
            or status.get("total_bytes_estimate")
            or total
            or 0
        )
        speed = float(status.get("speed") or 0)
        if not speed and downloaded:
            speed = downloaded / max(time.monotonic() - started_at, 0.001)
        on_progress(downloaded, total, speed)

    options = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "continuedl": True,
        "overwrites": False,
        "retries": 3,
        "fragment_retries": 3,
        "concurrent_fragment_downloads": max(1, int(concurrent_fragments)),
        "outtmpl": {"default": str(destination)},
        "format": format_id or "best",
        "progress_hooks": [progress_hook],
    }
    try:
        import imageio_ffmpeg

        options["ffmpeg_location"] = imageio_ffmpeg.get_ffmpeg_exe()
    except (ImportError, RuntimeError):
        pass
    if request_headers:
        options["http_headers"] = dict(request_headers)
    if proxy_url:
        options["proxy"] = proxy_url
    if interface_address:
        options["source_address"] = interface_address
    if cookie_browser:
        options["cookiesfrombrowser"] = (cookie_browser,)
    if speed_limit:
        options["ratelimit"] = max(1, int(speed_limit))

    try:
        with yt_dlp.YoutubeDL(options) as downloader:
            return_code = downloader.download([source_url])
    except yt_dlp.utils.DownloadCancelled:
        return MediaDownloadResult("paused", downloaded, total or downloaded)

    if stop_event.is_set():
        return MediaDownloadResult("paused", downloaded, total or downloaded)
    if return_code:
        raise RuntimeError(f"yt-dlp exited with status {return_code}.")
    if destination.exists():
        downloaded = destination.stat().st_size
    total = total or downloaded
    on_progress(downloaded, total, 0.0)
    return MediaDownloadResult("complete", downloaded, total)
