from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event

import httpx


@dataclass(frozen=True, slots=True)
class TorrentResult:
    status: str
    downloaded: int
    total: int
    connections: int = 1


def inspect_torrent(
    source: str,
    *,
    request_headers: dict[str, str] | None = None,
    proxy_url: str = "",
    timeout: float = 45.0,
) -> tuple[str, list[dict]]:
    try:
        import libtorrent as lt
    except ImportError as exc:
        raise RuntimeError(
            "Torrent downloads require libtorrent and Python 3.11–3.13."
        ) from exc

    session = None
    temporary_path = None
    try:
        if source.lower().startswith("magnet:"):
            session = lt.session()
            session.apply_settings(
                {"enable_dht": True, "enable_lsd": True}
            )
            params = lt.parse_magnet_uri(source)
            with tempfile.TemporaryDirectory() as metadata_directory:
                params.save_path = metadata_directory
                handle = session.add_torrent(params)
                deadline = time.monotonic() + timeout
                while not handle.status().has_metadata:
                    status = handle.status()
                    if status.errc.value() != 0:
                        raise RuntimeError(status.errc.message())
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            "Timed out while waiting for magnet metadata."
                        )
                    time.sleep(0.1)
                torrent_info = handle.torrent_file()
        else:
            transport = httpx.HTTPTransport(proxy=proxy_url or None)
            with httpx.Client(
                transport=transport,
                headers=request_headers or {},
                follow_redirects=True,
                timeout=timeout,
            ) as client:
                response = client.get(source)
                response.raise_for_status()
            temporary = tempfile.NamedTemporaryFile(
                suffix=".torrent", delete=False
            )
            temporary.write(response.content)
            temporary.close()
            temporary_path = Path(temporary.name)
            torrent_info = lt.torrent_info(str(temporary_path))

        storage = torrent_info.files()
        files = []
        for index in range(storage.num_files()):
            if hasattr(storage, "pad_file_at") and storage.pad_file_at(index):
                continue
            size = int(storage.file_size(index))
            files.append(
                {
                    "index": index,
                    "path": str(storage.file_path(index)),
                    "name": str(storage.file_name(index)),
                    "sizeBytes": size,
                    "size": _format_size(size),
                    "selected": True,
                }
            )
        if not files:
            raise RuntimeError("The torrent metadata does not contain any files.")
        return str(torrent_info.name() or "Torrent"), files
    finally:
        if session is not None:
            session.pause()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _format_size(value: int) -> str:
    size = float(max(0, value))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def download_torrent(
    source: str,
    destination_directory: Path,
    stop_event: Event,
    on_progress,
    *,
    speed_limit: int = 0,
    request_headers: dict[str, str] | None = None,
    proxy_url: str = "",
    timeout: float = 30.0,
    selected_files: list[int] | None = None,
    expected_size: int = 0,
) -> TorrentResult:
    try:
        import libtorrent as lt
    except ImportError as exc:
        raise RuntimeError(
            "Torrent downloads require libtorrent and Python 3.11–3.13."
        ) from exc

    destination_directory.mkdir(parents=True, exist_ok=True)
    session = lt.session()
    settings = {
        "enable_dht": True,
        "enable_lsd": True,
        "enable_upnp": True,
        "enable_natpmp": True,
    }
    if speed_limit > 0:
        settings["download_rate_limit"] = speed_limit
    session.apply_settings(settings)

    temporary_path = None
    try:
        if source.lower().startswith("magnet:"):
            params = lt.parse_magnet_uri(source)
        else:
            transport = httpx.HTTPTransport(proxy=proxy_url or None)
            with httpx.Client(
                transport=transport,
                headers=request_headers or {},
                follow_redirects=True,
                timeout=timeout,
            ) as client:
                response = client.get(source)
                response.raise_for_status()
            temporary = tempfile.NamedTemporaryFile(suffix=".torrent", delete=False)
            temporary.write(response.content)
            temporary.close()
            temporary_path = Path(temporary.name)
            params = lt.add_torrent_params()
            params.ti = lt.torrent_info(str(temporary_path))

        params.save_path = str(destination_directory)
        handle = session.add_torrent(params)
        last_report = time.monotonic()
        priorities_applied = False
        while True:
            status = handle.status()
            if status.errc.value() != 0:
                raise RuntimeError(status.errc.message())
            if status.has_metadata and not priorities_applied:
                storage = handle.torrent_file().files()
                file_count = storage.num_files()
                if selected_files:
                    selected = {
                        int(index)
                        for index in selected_files
                        if 0 <= int(index) < file_count
                    }
                    if not selected:
                        raise RuntimeError("No valid torrent files were selected.")
                    handle.prioritize_files(
                        [4 if index in selected else 0 for index in range(file_count)]
                    )
                priorities_applied = True
            total = int(status.total_wanted)
            downloaded = int(status.total_wanted_done)
            display_total = (
                max(0, expected_size)
                if selected_files and expected_size > 0
                else total
            )
            display_downloaded = (
                min(downloaded, display_total)
                if display_total > 0
                else downloaded
            )
            now = time.monotonic()
            if now - last_report >= 0.25:
                on_progress(
                    display_downloaded,
                    display_total,
                    float(status.download_rate),
                )
                last_report = now
            if stop_event.is_set():
                handle.pause()
                on_progress(display_downloaded, display_total, 0.0)
                return TorrentResult("paused", display_downloaded, display_total)
            if status.is_seeding or (total > 0 and downloaded >= total):
                completed_total = display_total or total
                on_progress(completed_total, completed_total, 0.0)
                return TorrentResult("complete", completed_total, completed_total)
            time.sleep(0.1)
    finally:
        session.pause()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
