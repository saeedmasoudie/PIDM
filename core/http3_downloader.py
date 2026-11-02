from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class Http3Result:
    status: str
    downloaded: int
    total: int
    connections: int = 1


def download_http3(
    url: str,
    destination: Path,
    stop_event: Event,
    on_progress,
    *,
    request_headers: dict[str, str],
    timeout: float,
) -> Http3Result:
    return asyncio.run(
        _download_http3(
            url,
            destination,
            stop_event,
            on_progress,
            request_headers,
            timeout,
        )
    )


async def _download_http3(
    url: str,
    destination: Path,
    stop_event: Event,
    on_progress,
    request_headers: dict[str, str],
    timeout: float,
) -> Http3Result:
    from aioquic.asyncio import QuicConnectionProtocol, connect
    from aioquic.h3.connection import H3_ALPN, H3Connection
    from aioquic.h3.events import DataReceived, HeadersReceived
    from aioquic.quic.configuration import QuicConfiguration

    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RuntimeError("HTTP/3 requires an HTTPS URL.")
    part_path = destination.with_name(destination.name + ".part")
    offset = part_path.stat().st_size if part_path.exists() else 0
    loop = asyncio.get_running_loop()

    class Protocol(QuicConnectionProtocol):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.http = H3Connection(self._quic)
            self.future = loop.create_future()
            self.output = None
            self.downloaded = offset
            self.total = 0
            self.started_at = time.monotonic()
            self.last_report_at = self.started_at
            self.last_report_bytes = offset
            self.stream_id = -1

        async def get(self):
            self.stream_id = self._quic.get_next_available_stream_id()
            authority = parsed.hostname
            if parsed.port and parsed.port != 443:
                authority += f":{parsed.port}"
            path = parsed.path or "/"
            if parsed.query:
                path += f"?{parsed.query}"
            headers = [
                (b":method", b"GET"),
                (b":scheme", b"https"),
                (b":authority", authority.encode()),
                (b":path", path.encode()),
            ]
            for name, value in request_headers.items():
                if name.lower() not in {
                    "host", "connection", "content-length", "range"
                }:
                    headers.append((name.lower().encode(), value.encode()))
            if offset:
                headers.append((b"range", f"bytes={offset}-".encode()))
            self.http.send_headers(self.stream_id, headers, end_stream=True)
            self.transmit()
            return await self.future

        def quic_event_received(self, event):
            for http_event in self.http.handle_event(event):
                if getattr(http_event, "stream_id", None) != self.stream_id:
                    continue
                if isinstance(http_event, HeadersReceived):
                    headers = {
                        key.decode().lower(): value.decode()
                        for key, value in http_event.headers
                    }
                    status = int(headers.get(":status", "0"))
                    if status not in {200, 206}:
                        if not self.future.done():
                            self.future.set_exception(
                                RuntimeError(f"HTTP/3 server returned status {status}.")
                            )
                        return
                    resumed = offset > 0 and status == 206
                    self.downloaded = offset if resumed else 0
                    length = int(headers.get("content-length", "0") or 0)
                    self.total = self.downloaded + length
                    self.output = part_path.open("ab" if resumed else "wb")
                    if http_event.stream_ended and not self.future.done():
                        self.future.set_result("complete")
                elif isinstance(http_event, DataReceived):
                    if stop_event.is_set():
                        if not self.future.done():
                            self.future.set_result("paused")
                        self.close()
                        return
                    if self.output is None:
                        if not self.future.done():
                            self.future.set_exception(
                                RuntimeError("HTTP/3 data arrived before response headers.")
                            )
                        return
                    self.output.write(http_event.data)
                    self.downloaded += len(http_event.data)
                    now = time.monotonic()
                    if now - self.last_report_at >= 0.25:
                        speed = (
                            self.downloaded - self.last_report_bytes
                        ) / (now - self.last_report_at)
                        on_progress(self.downloaded, self.total, speed)
                        self.last_report_at = now
                        self.last_report_bytes = self.downloaded
                    if http_event.stream_ended and not self.future.done():
                        self.future.set_result("complete")

        def finish_output(self):
            if self.output is not None:
                self.output.flush()
                os.fsync(self.output.fileno())
                self.output.close()

    configuration = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    configuration.idle_timeout = timeout
    port = parsed.port or 443
    protocol = None
    try:
        async with connect(
            parsed.hostname,
            port,
            configuration=configuration,
            create_protocol=Protocol,
            wait_connected=True,
        ) as protocol:
            status = await asyncio.wait_for(protocol.get(), timeout=timeout)
    finally:
        if protocol is not None:
            protocol.finish_output()

    if status == "paused":
        on_progress(protocol.downloaded, protocol.total, 0.0)
        return Http3Result("paused", protocol.downloaded, protocol.total)
    os.replace(part_path, destination)
    elapsed = max(time.monotonic() - protocol.started_at, 0.001)
    on_progress(
        protocol.downloaded,
        protocol.total or protocol.downloaded,
        max(0.0, (protocol.downloaded - offset) / elapsed),
    )
    return Http3Result(
        "complete",
        protocol.downloaded,
        protocol.total or protocol.downloaded,
    )
