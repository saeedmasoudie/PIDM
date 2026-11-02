from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class DownloadDatabase:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._initialize()

    def _initialize(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                start_time TEXT NOT NULL DEFAULT '',
                stop_time TEXT NOT NULL DEFAULT '',
                days TEXT NOT NULL DEFAULT '[]',
                enabled INTEGER NOT NULL DEFAULT 0,
                max_concurrent INTEGER NOT NULL DEFAULT 2,
                pause_between INTEGER NOT NULL DEFAULT 0,
                running INTEGER NOT NULL DEFAULT 0,
                post_action TEXT NOT NULL DEFAULT 'none'
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                destination TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                downloaded INTEGER NOT NULL DEFAULT 0,
                total INTEGER NOT NULL DEFAULT 0,
                speed REAL NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                queue_id INTEGER DEFAULT NULL,
                queue_position INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT NOT NULL DEFAULT '',
                speed_limit INTEGER NOT NULL DEFAULT 0,
                connection_limit INTEGER NOT NULL DEFAULT 0,
                checksum_expected TEXT NOT NULL DEFAULT '',
                checksum_actual TEXT NOT NULL DEFAULT '',
                request_headers TEXT NOT NULL DEFAULT '{}',
                proxy_url TEXT NOT NULL DEFAULT '',
                interface_address TEXT NOT NULL DEFAULT '',
                torrent_files TEXT NOT NULL DEFAULT '[]',
                FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE SET NULL
            )
            """
        )
        self._migrate_downloads()
        self._migrate_queues()
        self._connection.execute(
            """
            INSERT OR IGNORE INTO queues
                (name, enabled, max_concurrent)
            VALUES ('Main Queue', 0, 3)
            """
        )
        self._connection.commit()

    def _migrate_downloads(self) -> None:
        columns = {
            row["name"]
            for row in self._connection.execute("PRAGMA table_info(downloads)").fetchall()
        }
        additions = {
            "queue_id": "INTEGER DEFAULT NULL REFERENCES queues(id) ON DELETE SET NULL",
            "queue_position": "INTEGER NOT NULL DEFAULT 0",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "finished_at": "TEXT NOT NULL DEFAULT ''",
            "speed_limit": "INTEGER NOT NULL DEFAULT 0",
            "connection_limit": "INTEGER NOT NULL DEFAULT 0",
            "checksum_expected": "TEXT NOT NULL DEFAULT ''",
            "checksum_actual": "TEXT NOT NULL DEFAULT ''",
            "request_headers": "TEXT NOT NULL DEFAULT '{}'",
            "proxy_url": "TEXT NOT NULL DEFAULT ''",
            "interface_address": "TEXT NOT NULL DEFAULT ''",
            "torrent_files": "TEXT NOT NULL DEFAULT '[]'",
        }
        for name, definition in additions.items():
            if name not in columns:
                self._connection.execute(
                    f"ALTER TABLE downloads ADD COLUMN {name} {definition}"
                )

    def _migrate_queues(self) -> None:
        columns = {
            row["name"]
            for row in self._connection.execute("PRAGMA table_info(queues)").fetchall()
        }
        if "post_action" not in columns:
            self._connection.execute(
                "ALTER TABLE queues ADD COLUMN post_action TEXT NOT NULL DEFAULT 'none'"
            )

    def all(self) -> list[dict]:
        rows = self._connection.execute(
            "SELECT * FROM downloads ORDER BY id DESC"
        ).fetchall()
        return [dict(row) for row in rows]

    def add(
        self,
        *,
        url: str,
        destination: str,
        name: str,
        source: str,
        status: str = "queued",
        queue_id: int | None = None,
        checksum_expected: str = "",
        request_headers: dict[str, str] | None = None,
        proxy_url: str = "",
        interface_address: str = "",
        torrent_files: list[int] | None = None,
    ) -> int:
        if queue_id is None:
            queue_id = self.main_queue_id()
        next_position = self._connection.execute(
            "SELECT COALESCE(MAX(queue_position), -1) + 1 FROM downloads WHERE queue_id = ?",
            (queue_id,),
        ).fetchone()[0]
        cursor = self._connection.execute(
            """
            INSERT INTO downloads
                (url, destination, name, source, status, queue_id, queue_position,
                 checksum_expected, request_headers, proxy_url, interface_address,
                 torrent_files)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                url, destination, name, source, status, queue_id, next_position,
                checksum_expected, json.dumps(request_headers or {}),
                proxy_url, interface_address, json.dumps(torrent_files or []),
            ),
        )
        self._connection.commit()
        return int(cursor.lastrowid)

    def update(self, download_id: int, **fields) -> None:
        allowed = {
            "url", "source", "status", "downloaded", "total", "speed", "error", "queue_id",
            "queue_position", "finished_at",
            "destination", "speed_limit", "connection_limit",
            "checksum_expected", "checksum_actual", "request_headers",
            "proxy_url", "interface_address",
            "torrent_files",
        }
        values = {key: value for key, value in fields.items() if key in allowed}
        if not values:
            return
        clause = ", ".join(f"{key} = ?" for key in values)
        self._connection.execute(
            f"UPDATE downloads SET {clause} WHERE id = ?",
            (*values.values(), download_id),
        )
        self._connection.commit()

    def remove(self, download_id: int) -> None:
        self._connection.execute("DELETE FROM downloads WHERE id = ?", (download_id,))
        self._connection.commit()

    def main_queue_id(self) -> int:
        row = self._connection.execute(
            "SELECT id FROM queues WHERE name = 'Main Queue'"
        ).fetchone()
        return int(row["id"])

    def queues(self) -> list[dict]:
        rows = self._connection.execute(
            """
            SELECT q.*,
                   COUNT(d.id) AS download_count,
                   SUM(CASE WHEN d.status = 'downloading' THEN 1 ELSE 0 END) AS active_count,
                   SUM(CASE WHEN d.status IN ('queued','paused','error') THEN 1 ELSE 0 END) AS waiting_count
                   , SUM(CASE WHEN d.status = 'error' THEN 1 ELSE 0 END) AS error_count
            FROM queues q
            LEFT JOIN downloads d ON d.queue_id = q.id
            GROUP BY q.id
            ORDER BY q.id
            """
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["days"] = json.loads(item.get("days") or "[]")
            except json.JSONDecodeError:
                item["days"] = []
            result.append(item)
        return result

    def queue(self, queue_id: int) -> dict | None:
        row = self._connection.execute(
            "SELECT * FROM queues WHERE id = ?", (queue_id,)
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        try:
            item["days"] = json.loads(item.get("days") or "[]")
        except json.JSONDecodeError:
            item["days"] = []
        return item

    def save_queue(
        self,
        queue_id: int,
        *,
        name: str,
        enabled: bool,
        start_time: str,
        stop_time: str,
        days: list[str],
        max_concurrent: int,
        pause_between: int,
        post_action: str,
    ) -> int:
        post_action = post_action if post_action in {"none", "shutdown", "exit"} else "none"
        if queue_id < 0:
            cursor = self._connection.execute(
                """
                INSERT INTO queues
                    (name, enabled, start_time, stop_time, days, max_concurrent,
                     pause_between, post_action)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    int(enabled),
                    start_time,
                    stop_time,
                    json.dumps(days),
                    max(1, max_concurrent),
                    max(0, pause_between),
                    post_action,
                ),
            )
            queue_id = int(cursor.lastrowid)
        else:
            self._connection.execute(
                """
                UPDATE queues
                SET name = ?, enabled = ?, start_time = ?, stop_time = ?,
                    days = ?, max_concurrent = ?, pause_between = ?
                    , post_action = ?
                WHERE id = ?
                """,
                (
                    name,
                    int(enabled),
                    start_time,
                    stop_time,
                    json.dumps(days),
                    max(1, max_concurrent),
                    max(0, pause_between),
                    post_action,
                    queue_id,
                ),
            )
        self._connection.commit()
        return queue_id

    def set_queue_running(self, queue_id: int, running: bool) -> None:
        self._connection.execute(
            "UPDATE queues SET running = ? WHERE id = ?",
            (int(running), queue_id),
        )
        self._connection.commit()

    def delete_queue(self, queue_id: int) -> None:
        if queue_id == self.main_queue_id():
            raise ValueError("The Main Queue cannot be deleted.")
        main_id = self.main_queue_id()
        self._connection.execute(
            "UPDATE downloads SET queue_id = ?, queue_position = 0 WHERE queue_id = ?",
            (main_id, queue_id),
        )
        self._connection.execute("DELETE FROM queues WHERE id = ?", (queue_id,))
        self._connection.commit()

    def queue_name(self, queue_id: int | None) -> str:
        if queue_id is None:
            return ""
        row = self._connection.execute(
            "SELECT name FROM queues WHERE id = ?", (queue_id,)
        ).fetchone()
        return str(row["name"]) if row else ""

    def downloads_for_queue(self, queue_id: int) -> list[dict]:
        rows = self._connection.execute(
            """
            SELECT id, name, status, downloaded, total, queue_position
            FROM downloads
            WHERE queue_id = ?
            ORDER BY queue_position, id
            """,
            (queue_id,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["progress"] = (
                min(100, max(0, int(item["downloaded"] * 100 / item["total"])))
                if item["total"]
                else 0
            )
            result.append(item)
        return result

    def move_queue_download(self, queue_id: int, download_id: int, direction: int) -> None:
        rows = self._connection.execute(
            """
            SELECT id FROM downloads
            WHERE queue_id = ?
            ORDER BY queue_position, id
            """,
            (queue_id,),
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        if download_id not in ids:
            return
        old_index = ids.index(download_id)
        new_index = max(0, min(len(ids) - 1, old_index + direction))
        if new_index == old_index:
            return
        ids[old_index], ids[new_index] = ids[new_index], ids[old_index]
        with self._connection:
            for position, item_id in enumerate(ids):
                self._connection.execute(
                    "UPDATE downloads SET queue_position = ? WHERE id = ?",
                    (position, item_id),
                )

    def remove_from_queue(self, download_id: int) -> None:
        main_id = self.main_queue_id()
        next_position = self._connection.execute(
            "SELECT COALESCE(MAX(queue_position), -1) + 1 FROM downloads WHERE queue_id = ?",
            (main_id,),
        ).fetchone()[0]
        self._connection.execute(
            "UPDATE downloads SET queue_id = ?, queue_position = ? WHERE id = ?",
            (main_id, next_position, download_id),
        )
        self._connection.commit()
