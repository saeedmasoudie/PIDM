from __future__ import annotations

import os
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Callable

import httpx

ProgressCallback = Callable[[int, int, float], None]
ModeCallback = Callable[[int], None]
RetryCallback = Callable[[int, str], None]

DEFAULT_CONNECTIONS = 4
SEGMENT_THRESHOLD = 8 * 1024 * 1024
CHUNK_SIZE = 256 * 1024
DISK_RESERVE = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class DownloadResult:
    status: str
    downloaded: int
    total: int
    connections: int = 1


class RateLimiter:
    def __init__(self, bytes_per_second: int):
        self._rate = max(0, bytes_per_second)
        self._lock = threading.Lock()
        self._next_slot = time.monotonic()

    def wait(self, byte_count: int, stop_event: Event) -> bool:
        if not self._rate:
            return not stop_event.is_set()
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_slot)
            self._next_slot = start + byte_count / self._rate
            delay = max(0.0, start - now)
        return not stop_event.wait(delay)


def _ensure_disk_space(destination: Path, remaining: int) -> None:
    if remaining <= 0:
        return
    free = shutil.disk_usage(destination.parent).free
    if free < remaining + DISK_RESERVE:
        raise RuntimeError(
            "Not enough free disk space for this download "
            f"({remaining + DISK_RESERVE:,} bytes required)."
        )


def _total_size(response: httpx.Response, offset: int) -> int:
    content_range = response.headers.get("content-range", "")
    match = re.search(r"/(\d+)$", content_range)
    if match:
        return int(match.group(1))
    length = int(response.headers.get("content-length", "0") or 0)
    return offset + length if response.status_code == 206 else length


def _make_client(
    timeout: float,
    headers: dict[str, str],
    proxy_url: str,
    interface_address: str,
) -> httpx.Client:
    transport = httpx.HTTPTransport(
        proxy=proxy_url or None,
        local_address=interface_address or "0.0.0.0",
        retries=0,
    )
    return httpx.Client(
        transport=transport,
        headers=headers,
        follow_redirects=True,
        timeout=timeout,
    )


def _probe_range_support(
    url: str,
    timeout: float,
    request_headers: dict[str, str],
    proxy_url: str,
    interface_address: str,
) -> tuple[bool, int, bool]:
    try:
        with _make_client(
            timeout, request_headers, proxy_url, interface_address
        ) as client:
            with client.stream(
                "GET", url, headers={**request_headers, "Range": "bytes=0-0"}
            ) as response:
                if response.status_code != 206:
                    return (
                        False,
                        0,
                        "h3" in response.headers.get("alt-svc", "").lower(),
                    )
                total = _total_size(response, 0)
                return (
                    total > 0,
                    total,
                    "h3" in response.headers.get("alt-svc", "").lower(),
                )
    except (httpx.HTTPError, ValueError):
        return False, 0, False


def _download_single(
    url: str,
    destination: Path,
    stop_event: Event,
    on_progress: ProgressCallback,
    *,
    chunk_size: int,
    timeout: float,
    limiter: RateLimiter,
    request_headers: dict[str, str],
    proxy_url: str,
    interface_address: str,
) -> DownloadResult:
    part_path = destination.with_name(destination.name + ".part")
    offset = part_path.stat().st_size if part_path.exists() else 0
    headers = dict(request_headers)
    if offset:
        headers["Range"] = f"bytes={offset}-"
    started_at = time.monotonic()
    last_report_at = started_at
    last_report_bytes = offset

    with _make_client(
        timeout, request_headers, proxy_url, interface_address
    ) as client:
        with client.stream("GET", url, headers=headers) as response:
            if response.status_code == 416 and offset:
                content_range = response.headers.get("content-range", "")
                match = re.search(r"\*/(\d+)$", content_range)
                if match and int(match.group(1)) == offset:
                    os.replace(part_path, destination)
                    on_progress(offset, offset, 0.0)
                    return DownloadResult("complete", offset, offset)

            response.raise_for_status()
            resumed = offset > 0 and response.status_code == 206
            if offset and not resumed:
                offset = 0
            total = _total_size(response, offset)
            _ensure_disk_space(destination, max(0, total - offset))
            downloaded = offset
            mode = "ab" if resumed else "wb"

            with part_path.open(mode) as output:
                for chunk in response.iter_bytes(chunk_size):
                    if stop_event.is_set():
                        output.flush()
                        os.fsync(output.fileno())
                        on_progress(downloaded, total, 0.0)
                        return DownloadResult("paused", downloaded, total)
                    if not limiter.wait(len(chunk), stop_event):
                        output.flush()
                        os.fsync(output.fileno())
                        on_progress(downloaded, total, 0.0)
                        return DownloadResult("paused", downloaded, total)
                    output.write(chunk)
                    downloaded += len(chunk)
                    now = time.monotonic()
                    if now - last_report_at >= 0.25:
                        speed = (downloaded - last_report_bytes) / (now - last_report_at)
                        on_progress(downloaded, total, speed)
                        last_report_at = now
                        last_report_bytes = downloaded

    os.replace(part_path, destination)
    elapsed = max(time.monotonic() - started_at, 0.001)
    on_progress(downloaded, total or downloaded, max(0.0, (downloaded - offset) / elapsed))
    return DownloadResult("complete", downloaded, total or downloaded)


def _download_segmented(
    url: str,
    destination: Path,
    total: int,
    connection_count: int,
    stop_event: Event,
    on_progress: ProgressCallback,
    *,
    chunk_size: int,
    timeout: float,
    limiter: RateLimiter,
    request_headers: dict[str, str],
    proxy_url: str,
    interface_address: str,
    interface_addresses: list[str],
) -> DownloadResult:
    existing_bytes = sum(
        path.stat().st_size
        for path in destination.parent.glob(destination.name + ".part.*")
    )
    _ensure_disk_space(destination, max(0, total - existing_bytes))
    segment_size = (total + connection_count - 1) // connection_count
    ranges = [
        (index, index * segment_size, min(total - 1, (index + 1) * segment_size - 1))
        for index in range(connection_count)
        if index * segment_size < total
    ]
    part_paths = [
        destination.with_name(destination.name + f".part.{index}")
        for index, _, _ in ranges
    ]
    initial_sizes = [
        min(path.stat().st_size, end - start + 1) if path.exists() else 0
        for path, (_, start, end) in zip(part_paths, ranges)
    ]
    progress_by_segment = initial_sizes[:]
    lock = threading.Lock()
    abort_event = threading.Event()
    started_at = time.monotonic()
    report = {
        "time": started_at,
        "bytes": sum(initial_sizes),
    }

    def worker(index: int, start: int, end: int, path: Path) -> None:
        existing = min(path.stat().st_size, end - start + 1) if path.exists() else 0
        if existing >= end - start + 1:
            return
        request_start = start + existing
        headers = {
            **request_headers,
            "Range": f"bytes={request_start}-{end}",
        }
        segment_interface = (
            interface_addresses[index % len(interface_addresses)]
            if interface_addresses
            else interface_address
        )
        with _make_client(
            timeout, request_headers, proxy_url, segment_interface
        ) as client:
            with client.stream("GET", url, headers=headers) as response:
                if response.status_code != 206:
                    raise RuntimeError("Server stopped accepting parallel range requests.")
                response.raise_for_status()
                with path.open("ab" if existing else "wb") as output:
                    for chunk in response.iter_bytes(chunk_size):
                        if stop_event.is_set() or abort_event.is_set():
                            output.flush()
                            os.fsync(output.fileno())
                            return
                        if not limiter.wait(len(chunk), stop_event):
                            output.flush()
                            os.fsync(output.fileno())
                            return
                        output.write(chunk)
                        with lock:
                            progress_by_segment[index] += len(chunk)
                            downloaded = sum(progress_by_segment)
                            now = time.monotonic()
                            if now - report["time"] >= 0.25:
                                speed = (downloaded - report["bytes"]) / (now - report["time"])
                                on_progress(downloaded, total, speed)
                                report["time"] = now
                                report["bytes"] = downloaded

    try:
        with ThreadPoolExecutor(
            max_workers=len(ranges), thread_name_prefix="pidm-segment"
        ) as executor:
            futures = [
                executor.submit(worker, index, start, end, part_paths[index])
                for index, start, end in ranges
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    abort_event.set()
                    for pending in futures:
                        pending.cancel()
                    raise
    except Exception:
        abort_event.set()
        raise

    downloaded = sum(
        min(path.stat().st_size, end - start + 1) if path.exists() else 0
        for path, (_, start, end) in zip(part_paths, ranges)
    )
    if stop_event.is_set():
        on_progress(downloaded, total, 0.0)
        return DownloadResult("paused", downloaded, total, len(ranges))

    if downloaded != total:
        raise RuntimeError(
            f"Parallel download ended early ({downloaded} of {total} bytes)."
        )

    merge_path = destination.with_name(destination.name + ".part")
    with merge_path.open("wb") as merged:
        for path in part_paths:
            with path.open("rb") as segment:
                while chunk := segment.read(chunk_size):
                    merged.write(chunk)
    os.replace(merge_path, destination)
    for path in part_paths:
        path.unlink(missing_ok=True)
    elapsed = max(time.monotonic() - started_at, 0.001)
    on_progress(total, total, max(0.0, (total - sum(initial_sizes)) / elapsed))
    return DownloadResult("complete", total, total, len(ranges))


def download_http(
    url: str,
    destination: Path,
    stop_event: Event,
    on_progress: ProgressCallback,
    *,
    on_mode: ModeCallback | None = None,
    on_retry: RetryCallback | None = None,
    connections: int = DEFAULT_CONNECTIONS,
    speed_limit: int = 0,
    max_retries: int = 3,
    retry_delay: int = 3,
    chunk_size: int = CHUNK_SIZE,
    timeout: float = 30.0,
    request_headers: dict[str, str] | None = None,
    proxy_url: str = "",
    interface_address: str = "",
    interface_addresses: list[str] | None = None,
    prefer_http3: bool = True,
) -> DownloadResult:
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    legacy_part = destination.with_name(destination.name + ".part")
    has_segments = any(
        destination.parent.glob(destination.name + ".part.*")
    )
    limiter = RateLimiter(speed_limit)
    request_headers = dict(request_headers or {})
    interface_addresses = [
        str(address).strip()
        for address in (interface_addresses or [])
        if str(address).strip()
    ]
    if proxy_url:
        interface_addresses = []
    last_error: Exception | None = None
    http3_attempted = False
    for attempt in range(max(0, max_retries) + 1):
        try:
            supports_ranges, total, advertises_http3 = _probe_range_support(
                url,
                timeout,
                request_headers,
                proxy_url,
                interface_address,
            )
            if (
                prefer_http3
                and advertises_http3
                and url.lower().startswith("https://")
                and not http3_attempted
                and not proxy_url
                and not interface_address
                and not interface_addresses
            ):
                http3_attempted = True
                try:
                    from core.http3_downloader import download_http3

                    if on_mode:
                        on_mode(1)
                    return download_http3(
                        url,
                        destination,
                        stop_event,
                        on_progress,
                        request_headers=request_headers,
                        timeout=min(timeout, 15.0),
                    )
                except Exception:
                    pass
            use_segments = (
                connections > 1
                and supports_ranges
                and total >= SEGMENT_THRESHOLD
                and (has_segments or not legacy_part.exists())
            )
            if use_segments:
                actual_connections = min(max(2, connections), 8)
                if on_mode:
                    on_mode(actual_connections)
                return _download_segmented(
                    url,
                    destination,
                    total,
                    actual_connections,
                    stop_event,
                    on_progress,
                    chunk_size=chunk_size,
                    timeout=timeout,
                    limiter=limiter,
                    request_headers=request_headers,
                    proxy_url=proxy_url,
                    interface_address=interface_address,
                    interface_addresses=interface_addresses,
                )

            if on_mode:
                on_mode(1)
            return _download_single(
                url,
                destination,
                stop_event,
                on_progress,
                chunk_size=chunk_size,
                timeout=timeout,
                limiter=limiter,
                request_headers=request_headers,
                proxy_url=proxy_url,
                interface_address=interface_address,
            )
        except (httpx.HTTPError, OSError, RuntimeError) as exc:
            last_error = exc
            if stop_event.is_set() or attempt >= max_retries:
                raise
            retry_number = attempt + 1
            if on_retry:
                on_retry(retry_number, str(exc))
            backoff = min(60, max(1, retry_delay) * (2 ** attempt))
            if stop_event.wait(backoff):
                break
    if last_error:
        raise last_error
    raise RuntimeError("Download stopped before completion.")
