import logging
import mimetypes
import platform
import re
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse, unquote

import httpx
from PySide6.QtGui import QAction, QIcon, QActionGroup, QDesktopServices, QFontDatabase, QFont, QColor, QPixmap, \
    QPainter
from PySide6.QtNetwork import QLocalSocket, QLocalServer
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QToolBar, QTreeWidget, QTreeWidgetItem,
    QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget, QHBoxLayout,
    QHeaderView, QMenu, QPushButton, QLabel, QLineEdit, QDialog, QFileDialog, QCheckBox, QComboBox,
    QFormLayout, QDialogButtonBox, QMessageBox, QInputDialog, QProgressBar, QFileIconProvider, QTimeEdit, QGridLayout,
    QTabWidget, QRadioButton, QDateEdit, QAbstractItemView, QSpinBox, QSizePolicy, QSplitter, QTextEdit,
    QSystemTrayIcon
)
from PySide6.QtCore import Qt, QSize, QTranslator, QLocale, QLibraryInfo, QCoreApplication, QTimer, QDateTime, Slot, \
    QFileInfo, QUrl, QPoint, QTime, QDate, QProcess, QThread, Signal, QEventLoop, QObject
import sys
import os
import json
from pathlib import Path
import sqlite3


def get_user_data_dir() -> Path:
    if platform.system() == "Windows":
        return Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "PIDM"
    elif platform.system() == "Darwin":
        return Path.home() / "Library" / "Application Support" / "PIDM"
    else:
        return Path.home() / ".config" / "PIDM"

def setup_logger(log_file="pidm.log"):
    log_dir = get_user_data_dir() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    logger = logging.getLogger("PIDM")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

# --- STATUS CONSTANTS ---
STATUS_QUEUED = "queued"
STATUS_DOWNLOADING = "downloading"
STATUS_PAUSED = "paused"
STATUS_CANCELLED = "cancelled"
STATUS_COMPLETE = "complete"
STATUS_ERROR = "error"
STATUS_CONNECTING = "connecting"
STATUS_RESUMING = "resuming"
STATUS_INCOMPLETE = "incomplete"

APP_RESTART_CODE = 1000
logger = setup_logger()


class DatabaseManager:
    def __init__(self, db_path=None):
        if db_path is None:
            db_dir = get_user_data_dir()
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / "downloads.db"
        else:
            db_path = Path(db_path)

        self.db_path_str = str(db_path)
        self.conn = sqlite3.connect(self.db_path_str, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init_db()

    def init_db(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            file_name TEXT,
            save_path TEXT,
            description TEXT,
            status TEXT,
            progress INTEGER,
            speed TEXT,
            eta TEXT,
            queue_id INTEGER DEFAULT NULL,
            queue_position INTEGER DEFAULT 0,
            created_at TEXT,
            finished_at TEXT,
            bytes_downloaded INTEGER DEFAULT 0,
            total_size INTEGER DEFAULT 0,
            referrer TEXT DEFAULT NULL,
            custom_headers_json TEXT DEFAULT NULL,
            auth_json TEXT DEFAULT NULL,
            UNIQUE(url, save_path),
            FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE SET NULL
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS queues (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, start_time TEXT,
            stop_time TEXT, days TEXT DEFAULT '[]', enabled INTEGER DEFAULT 0,
            max_concurrent INTEGER DEFAULT 1, pause_between INTEGER DEFAULT 0
        )""")
        self.conn.commit()

        self._migrate_schema()

    def _migrate_schema(self):
        """Adds new columns to the downloads table if they don't exist."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("PRAGMA table_info(downloads)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'referrer' not in columns:
                logger.info("Adding 'referrer' column to 'downloads' table.")
                cursor.execute("ALTER TABLE downloads ADD COLUMN referrer TEXT DEFAULT NULL")
            if 'custom_headers_json' not in columns:
                logger.info("Adding 'custom_headers_json' column to 'downloads' table.")
                cursor.execute("ALTER TABLE downloads ADD COLUMN custom_headers_json TEXT DEFAULT NULL")
            if 'auth_json' not in columns:
                logger.info("Adding 'auth_json' column to 'downloads' table.")
                cursor.execute("ALTER TABLE downloads ADD COLUMN auth_json TEXT DEFAULT NULL")
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error during schema migration: {e}")

    def add_download(self, data: dict) -> int:
        logger.info(f"[DEBUG-REFERRER] 1. Received by add_download: '{data.get('referrer')}'")
        referrer_value = data.get("referrer")

        if referrer_value and len(referrer_value) < 10:
            logger.warning(f"Rejecting short/invalid referrer '{referrer_value}'. Storing as NULL instead.")
            referrer_value = None

        logger.info(f"[DEBUG-REFERRER] 2. Value being saved to DB: '{referrer_value}'")

        cur = self.conn.cursor()
        try:
            custom_headers_dict = data.get("custom_headers")
            auth_tuple = data.get("auth")

            custom_headers_json = json.dumps(custom_headers_dict) if custom_headers_dict else None
            auth_json = json.dumps(auth_tuple) if auth_tuple else None

            cur.execute("""
            INSERT INTO downloads (
                url, file_name, save_path, description, status,
                progress, speed, eta, queue_id, queue_position,
                created_at, bytes_downloaded, total_size, referrer,
                custom_headers_json, auth_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["url"], data["file_name"], data["save_path"],
                data.get("description", ""), data.get("status", "queued"),
                data.get("progress", 0), data.get("speed", ""), data.get("eta", ""),
                data.get("queue_id"), data.get("queue_position", 0),
                data.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                data.get("bytes_downloaded", 0), data.get("total_size", 0),
                referrer_value,
                custom_headers_json, auth_json
            ))
            self.conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            logger.warning(
                f"IntegrityError: Download with URL {data['url']} and save_path {data['save_path']} likely already exists.")
            cur.execute("SELECT id FROM downloads WHERE url = ? AND save_path = ?", (data["url"], data["save_path"]))
            row = cur.fetchone();
            return row["id"] if row else -1
        except sqlite3.Error as e:
            logger.error(f"SQLite error in add_download: {e}");
            return -1
        except json.JSONDecodeError as e:
            logger.error(f"JSON encoding error for custom_headers in add_download: {e}")
            return -1

    def update_download_progress_details(self, download_id, progress, bytes_downloaded):
        try:
            self.conn.execute("""
            UPDATE downloads
            SET progress = ?, bytes_downloaded = ?
            WHERE id = ?
            """, (progress, bytes_downloaded, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_download_progress_details for ID {download_id}: {e}")

    def reset_download_for_redownload(self, download_id: int):
        try:
            self.conn.execute("""
                UPDATE downloads
                SET bytes_downloaded = 0, progress = 0, status = ?, finished_at = NULL
                WHERE id = ?
            """, (STATUS_PAUSED, download_id))
            self.conn.commit()
            logger.info(f"Download {download_id} has been reset for redownload.")
        except sqlite3.Error as e:
            logger.error(f"SQLite error in reset_download_for_redownload for ID {download_id}: {e}")

    def update_total_size(self, download_id, total_size):
        try:
            self.conn.execute("UPDATE downloads SET total_size = ? WHERE id = ?", (total_size, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_total_size for ID {download_id}: {e}")

    def update_status(self, download_id, status):
        try:
            self.conn.execute("UPDATE downloads SET status = ? WHERE id = ?", (status, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_status for ID {download_id}: {e}")

    def update_status_and_progress_on_pause(self, download_id, status, progress, bytes_downloaded):
        try:
            self.conn.execute("""
            UPDATE downloads
            SET status = ?, progress = ?, bytes_downloaded = ?
            WHERE id = ?
            """, (status, progress, bytes_downloaded, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_status_and_progress_on_pause for ID {download_id}: {e}")

    def mark_complete(self, download_id, finished_at):
        try:
            self.conn.execute("""
            UPDATE downloads
            SET status = ?, progress = 100, finished_at = ?, queue_id = NULL, queue_position = 0
            WHERE id = ?
            """, (STATUS_COMPLETE, finished_at, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in mark_complete for ID {download_id}: {e}")

    def delete_download(self, download_id):
        try:
            self.conn.execute("DELETE FROM downloads WHERE id = ?", (download_id,))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in delete_download for ID {download_id}: {e}")

    def load_all(self) -> list:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT * FROM downloads ORDER BY created_at DESC, id DESC")
            return [dict(row) for row in cur.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"SQLite error in load_all: {e}")
            return []

    def get_download_by_id(self, download_id) -> dict | None:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT * FROM downloads WHERE id = ?", (download_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"SQLite error in get_download_by_id for ID {download_id}: {e}")
            return None

    def get_downloads_in_queue(self, queue_id: int) -> list:
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT * FROM downloads
                WHERE queue_id = ? 
                  AND status != ?  
                ORDER BY queue_position ASC, id ASC
            """, (queue_id, STATUS_COMPLETE))
            return [dict(row) for row in cur.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"SQLite error in get_downloads_in_queue for Queue ID {queue_id}: {e}")
            return []

    def update_download_queue_position(self, download_id: int, new_position: int):
        try:
            self.conn.execute("UPDATE downloads SET queue_position = ? WHERE id = ?", (new_position, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_download_queue_position for ID {download_id}: {e}")

    def update_download_queue_id(self, download_id: int, queue_id: int | None, position: int = 0):
        try:
            self.conn.execute("UPDATE downloads SET queue_id = ?, queue_position = ? WHERE id = ?",
                              (queue_id, position, download_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_download_queue_id for ID {download_id}: {e}")

    # -------------------- Queues --------------------
    def add_queue(self, name, start_time=None, stop_time=None, days=None,
                  enabled=0, max_concurrent=1, pause_between=0) -> int:
        if days is None: days = []
        cur = self.conn.cursor()
        try:
            cur.execute("""
            INSERT INTO queues (name, start_time, stop_time, days, enabled, max_concurrent, pause_between)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (name, start_time, stop_time, json.dumps(days), enabled, max_concurrent, pause_between))
            self.conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            cur.execute("SELECT id FROM queues WHERE name = ?", (name,))
            row = cur.fetchone()
            return row["id"] if row else -1
        except sqlite3.Error as e:
            logger.error(f"SQLite error in add_queue: {e}")
            return -1

    def get_all_queues(self) -> list:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT * FROM queues ORDER BY id")
            queues = []
            for row_data in cur.fetchall():
                q = dict(row_data)
                try:
                    days_raw = q.get("days", "[]")
                    if isinstance(days_raw, str):
                        q["days"] = json.loads(days_raw) if days_raw and days_raw.strip().startswith("[") else []
                    elif isinstance(days_raw, list): # Already a list, use as is
                        q["days"] = days_raw
                    else: # Unexpected type
                        q["days"] = []
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse 'days' for queue {q.get('name')}: {e}")
                    q["days"] = []
                queues.append(q)
            return queues
        except sqlite3.Error as e:
            logger.error(f"SQLite error in get_all_queues: {e}")
            return []

    def get_queue_by_id(self, queue_id: int):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT * FROM queues WHERE id = ?", (queue_id,))
            row = cur.fetchone()
            if row:
                q_data = dict(row)
                try:
                    days_raw = q_data.get("days", "[]")
                    if isinstance(days_raw, str):
                        q_data["days"] = json.loads(days_raw) if days_raw and days_raw.strip().startswith("[") else []
                    elif isinstance(days_raw, list):
                        q_data["days"] = days_raw
                    else:
                        q_data["days"] = []
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse 'days' for queue ID {queue_id}: {e}")
                    q_data["days"] = []
                return q_data
            return None
        except sqlite3.Error as e:
            logger.error(f"SQLite error in get_queue_by_id for Queue ID {queue_id}: {e}")
            return None

    def get_queue_by_name(self, name: str):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT * FROM queues WHERE name = ?", (name,))
            row = cur.fetchone()
            if row:
                q_data = dict(row)
                try:
                    days_raw = q_data.get("days", "[]")
                    if isinstance(days_raw, str):
                        q_data["days"] = json.loads(days_raw) if days_raw and days_raw.strip().startswith("[") else []
                    elif isinstance(days_raw, list):
                        q_data["days"] = days_raw
                    else:
                        q_data["days"] = []
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse 'days' for queue {name}: {e}")
                    q_data["days"] = []
                return q_data
            return None
        except sqlite3.Error as e:
            logger.error(f"SQLite error in get_queue_by_name for queue {name}: {e}")
            return None

    def update_queue(self, queue_id: int, name: str, start_time: str | None, stop_time: str | None, days: list,
                     enabled: int, max_concurrent: int = 1, pause_between: int = 0):
        try:
            self.conn.execute("""
                UPDATE queues
                SET name = ?, start_time = ?, stop_time = ?, days = ?, enabled = ?, max_concurrent = ?, pause_between = ?
                WHERE id = ?
            """, (name, start_time, stop_time, json.dumps(days), enabled, max_concurrent, pause_between, queue_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in update_queue for Queue ID {queue_id}: {e}")

    def delete_queue(self, queue_id):
        try:
            self.conn.execute("UPDATE downloads SET queue_id = NULL, queue_position = 0 WHERE queue_id = ?", (queue_id,))
            self.conn.execute("DELETE FROM queues WHERE id = ?", (queue_id,))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in delete_queue for Queue ID {queue_id}: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info(f"Database connection to {self.db_path_str} closed.")

    def __del__(self):
        self.close()


class SettingsManager:
    def __init__(self, filename=None):
        if filename is None:
            settings_dir = get_user_data_dir()
            settings_dir.mkdir(parents=True, exist_ok=True)
            filename = settings_dir / "settings.json"

        self.filename = str(filename)
        self.settings = {}
        self.load()

    def load(self):
        default_settings = {
            "default_download_directory": str(Path.home() / "Downloads"),
            "language": "en",  # Default language
            "theme": "dark",
            "global_speed_limit_kbps": 0,
            "default_max_concurrent": 3,
            "remembered_queue_id": None,
            "recent_paths": []
        }
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    loaded_settings = json.load(f)
                    # Merge with defaults to ensure all keys exist
                    self.settings = {**default_settings, **loaded_settings}
            except json.JSONDecodeError:
                logger.warning(f"Could not decode {self.filename}. Using default settings.")
                self.settings = default_settings
        else:
            self.settings = default_settings
        self.save()  # Save to ensure all default keys are written if it's a new file

    def save(self):
        try:
            with open(self.filename, "w", encoding="utf-8") as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            logger.error(f"Error on saving settings: {e}")

    def get(self, key, default=None):
        # Use default from init if key not present, rather than passed default
        return self.settings.get(key, default)

    def set(self, key, value):
        self.settings[key] = value
        self.save()

    def add_recent_path(self, path):
        paths = self.settings.get("recent_paths", [])
        if path in paths: paths.remove(path)
        paths.insert(0, path)
        self.settings["recent_paths"] = paths[:10]
        self.save()


class ResumeFileHandler:
    def __init__(self, file_path: str | Path):
        self.save_path = Path(file_path)
        self.resume_path = self.save_path.with_suffix(self.save_path.suffix + ".resume")

    def write(self, downloaded_bytes: int, total_size: int, url: str):
        resume_info = {
            "downloaded_bytes": downloaded_bytes,
            "total_size": total_size,
            "url": url,
            "save_path": str(self.save_path),
            "timestamp": time.time()
        }
        try:
            with open(self.resume_path, "w") as f:
                json.dump(resume_info, f)
            return True
        except Exception as e:
            logger.error(f"[ResumeFile] Failed to write resume file: {e}")
            return False

    def read(self) -> dict | None:
        if not self.resume_path.exists(): return None
        try:
            with open(self.resume_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[ResumeFile] Failed to read resume file: {e}")
            return None

    def delete(self):
        try:
            if self.resume_path.exists(): self.resume_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"[ResumeFile] Failed to delete resume file: {e}")


class DownloadWorker(QThread):
    progress_updated = Signal(int, str, str, int, int)
    status_changed = Signal(str)
    finished_successfully = Signal(str)
    total_size_known = Signal(int)
    global_speed_limit_bps = None

    def __init__(self, download_id: int, db_manager, url: str, save_path: str,
                 initial_downloaded_bytes: int = 0, total_file_size: int = 0,
                 auth=None, custom_headers: dict = None,
                 parent=None):
        super().__init__(parent)
        self.download_id = download_id
        self.db = db_manager
        self.url = url
        self.save_path = Path(save_path)
        self.auth = auth
        self.custom_headers = custom_headers if custom_headers is not None else {}
        self._last_db_write_time = time.time()
        self.resume_helper = ResumeFileHandler(self.save_path)

        self.httpx_response = None
        self.httpx_client = None
        self._total_size = total_file_size
        self._downloaded_bytes = initial_downloaded_bytes
        self._is_paused = False
        self._is_cancelled = False
        self._is_running = False
        self._db_write_interval = 1.0

        self._last_timestamp = time.time()
        self._last_downloaded_bytes_for_speed_calc = initial_downloaded_bytes
        self._speed_bps = 0
        self._eta_seconds = float('inf')
        self._last_timestamp_for_limit = time.time()

    @classmethod
    def set_global_speed_limit(cls, limit_kbps: int):
        if limit_kbps <= 0:
            cls.global_speed_limit_bps = None
        else:
            cls.global_speed_limit_bps = limit_kbps * 1024

    def pause(self):
        if self._is_running and not self._is_paused:
            self._is_paused = True
            self.status_changed.emit(STATUS_PAUSED)
            progress_percent = int((self._downloaded_bytes / self._total_size) * 100) if self._total_size > 0 else 0
            self.db.update_status_and_progress_on_pause(
                self.download_id, STATUS_PAUSED, progress_percent, self._downloaded_bytes
            )
            logger.info(f"Download {self.download_id} paused.")

    def resume(self):
        if self._is_running and self._is_paused:
            self._is_paused = False
            self._last_timestamp = time.time()
            self._last_downloaded_bytes_for_speed_calc = self._downloaded_bytes
            self._last_timestamp_for_limit = time.time()
            self.status_changed.emit(STATUS_RESUMING)
            self.db.update_status(self.download_id, STATUS_DOWNLOADING)
            logger.info(f"Download {self.download_id} resumed.")

    def cancel(self):
        self._is_cancelled = True
        logger.info(f"DownloadWorker {self.download_id} cancel called.")

        # Attempt to close network resources if they exist
        if self.httpx_response:
            try:
                self.httpx_response.close()
                logger.info(f"DownloadWorker {self.download_id} closed httpx response object during cancel.")
            except Exception as e:
                logger.warning(f"DownloadWorker {self.download_id} error closing httpx response during cancel: {e}")

        if self.httpx_client:
            try:
                self.httpx_client.close()
                logger.info(f"DownloadWorker {self.download_id} closed httpx client during cancel.")
            except Exception as e:
                logger.warning(f"DownloadWorker {self.download_id} error closing httpx client during cancel: {e}")

        if self.isRunning():
            self.quit()
            if not self.wait(3000):
                logger.warning(
                    f"DownloadWorker {self.download_id} did not terminate gracefully after quit(). Terminating.")
                self.terminate()
            else:
                logger.info(f"DownloadWorker {self.download_id} quit gracefully.")
        else:
            logger.info(
                f"DownloadWorker {self.download_id} was not running (or already finished) when cancel was called.")


        download_entry = self.db.get_download_by_id(self.download_id)
        if self.save_path.exists() and (not download_entry or download_entry.get('status') != STATUS_COMPLETE):
            try:
                self.save_path.unlink(missing_ok=True)
                logger.info(f"Partial file {self.save_path} for {self.download_id} deleted on cancel.")
            except OSError as e:
                logger.error(f"Error deleting partial file {self.save_path} for {self.download_id} on cancel: {e}")

        self.resume_helper.delete()

        self.db.update_status(self.download_id, STATUS_CANCELLED)
        self.status_changed.emit(STATUS_CANCELLED)

    def _calculate_speed_and_eta(self):
        current_time = time.time()
        time_delta = current_time - self._last_timestamp

        if time_delta > 0.5:
            bytes_delta = self._downloaded_bytes - self._last_downloaded_bytes_for_speed_calc
            self._speed_bps = bytes_delta / time_delta if time_delta > 0 else 0
            self._last_timestamp = current_time
            self._last_downloaded_bytes_for_speed_calc = self._downloaded_bytes

            if self._speed_bps > 0 and self._total_size > 0:
                remaining_bytes = self._total_size - self._downloaded_bytes
                if remaining_bytes > 0:
                    self._eta_seconds = remaining_bytes / self._speed_bps
                else:
                    self._eta_seconds = 0
            else:
                self._eta_seconds = float('inf')

        speed_bps_val = self._speed_bps
        if speed_bps_val >= 1024 ** 3:
            speed_str = f"{speed_bps_val / (1024 ** 3):.2f} GB/s"
        elif speed_bps_val >= 1024 ** 2:
            speed_str = f"{speed_bps_val / (1024 ** 2):.2f} MB/s"
        elif speed_bps_val >= 1024:
            speed_str = f"{speed_bps_val / 1024:.2f} KB/s"
        else:
            speed_str = f"{speed_bps_val:.0f} B/s"

        if self._eta_seconds == float('inf') or self._total_size == 0:
            eta_str = "--"
        elif self._eta_seconds == 0:
            eta_str = "00:00"
        else:
            eta_str = time.strftime("%H:%M:%S",
                                    time.gmtime(self._eta_seconds)) if self._eta_seconds >= 3600 else time.strftime(
                "%M:%S", time.gmtime(self._eta_seconds))
        return speed_str, eta_str

    def run(self):
        self._is_running = True

        if self._is_cancelled:
            logger.info(f"Download {self.download_id} run() called but already cancelled.")
            self.status_changed.emit(STATUS_CANCELLED)
            self._is_running = False
            return

        self.status_changed.emit(STATUS_CONNECTING)
        self.db.update_status(self.download_id, STATUS_CONNECTING)

        effective_headers = {
            "User-Agent": "PIDM/1.0 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        effective_headers.update(self.custom_headers)

        current_request_headers = effective_headers.copy()
        file_mode = "wb"

        if self._downloaded_bytes > 0:
            if not self.save_path.exists():
                logger.warning(f"[Resume] File {self.save_path} missing for {self.download_id}, restarting download.")
                self._downloaded_bytes = 0
                self.db.update_download_progress_details(self.download_id, 0, 0)
            else:
                current_file_size = self.save_path.stat().st_size
                resume_data_from_file = self.resume_helper.read()

                if resume_data_from_file and resume_data_from_file.get("url") == self.url:
                    self._downloaded_bytes = resume_data_from_file.get("downloaded_bytes", 0)
                    if self._total_size == 0 and resume_data_from_file.get("total_size", 0) > 0:
                        self._total_size = resume_data_from_file.get("total_size")
                        self.total_size_known.emit(self._total_size)
                        self.db.update_total_size(self.download_id, self._total_size)
                    logger.info(f"[Resume] Using .resume file: {self._downloaded_bytes} bytes for {self.download_id}.")
                    if abs(current_file_size - self._downloaded_bytes) > 1024 * 1024:
                        logger.warning(
                            f"[Resume] Large size mismatch (.resume: {self._downloaded_bytes}, file: {current_file_size}) for {self.download_id}. Trusting .resume.")
                elif abs(current_file_size - self._downloaded_bytes) > 1024 * 1024:
                    logger.warning(
                        f"[Resume] File size ({current_file_size}) vs DB ({self._downloaded_bytes}) mismatch for {self.download_id}. Restarting.")
                    self._downloaded_bytes = 0
                    self.db.update_download_progress_details(self.download_id, 0, 0)
                elif current_file_size < self._downloaded_bytes:
                    logger.warning(
                        f"[Resume] File size ({current_file_size}) < DB ({self._downloaded_bytes}) for {self.download_id}. Using file size.")
                    self._downloaded_bytes = current_file_size

                if self._downloaded_bytes > 0:
                    current_request_headers["Range"] = f"bytes={self._downloaded_bytes}-"
                    file_mode = "ab"
                    if not self._is_cancelled:
                        self.status_changed.emit(STATUS_RESUMING)
                        self.db.update_status(self.download_id, STATUS_RESUMING)
                else:
                    file_mode = "wb"

        if self._is_cancelled:
            logger.info(f"Download {self.download_id} cancelled before starting network stream.")
            self.status_changed.emit(STATUS_CANCELLED)
            self._is_running = False
            return

        try:
            self.save_path.parent.mkdir(parents=True, exist_ok=True)

            timeout_config = httpx.Timeout(30.0, connect=15.0)
            self.httpx_client = httpx.Client(auth=self.auth, timeout=timeout_config, follow_redirects=True)

            with self.httpx_client.stream("GET", self.url, headers=current_request_headers) as response:
                self.httpx_response = response

                if self._is_cancelled:
                    logger.info(f"Download {self.download_id} cancelled after stream opened, before reading.")
                    self.status_changed.emit(STATUS_CANCELLED)
                    return

                response.raise_for_status()

                new_total_size_from_server = 0
                content_range_header = response.headers.get("Content-Range")
                content_length_header = response.headers.get("Content-Length")

                if file_mode == "ab" and content_range_header and '/' in content_range_header:
                    try:
                        new_total_size_from_server = int(content_range_header.split('/')[-1])
                    except ValueError:
                        logger.warning(
                            f"Could not parse Content-Range total for {self.download_id}: {content_range_header}")
                elif content_length_header:
                    try:
                        server_reported_len = int(content_length_header)
                        new_total_size_from_server = (
                                    self._downloaded_bytes + server_reported_len) if file_mode == "ab" else server_reported_len
                    except ValueError:
                        logger.warning(
                            f"Could not parse Content-Length for {self.download_id}: {content_length_header}")

                if new_total_size_from_server > 0 and (
                        self._total_size == 0 or self._total_size != new_total_size_from_server):
                    self._total_size = new_total_size_from_server
                    self.total_size_known.emit(self._total_size)
                    self.db.update_total_size(self.download_id, self._total_size)
                elif self._total_size == 0:
                    if not self._is_cancelled: self.status_changed.emit(
                        self.tr("Warning: Total file size unknown. Progress may be inaccurate."))

                if self._is_cancelled:
                    logger.info(f"Download {self.download_id} cancelled after header processing.")
                    self.status_changed.emit(STATUS_CANCELLED)
                    return

                if not self._is_cancelled:
                    self.status_changed.emit(STATUS_DOWNLOADING)
                    self.db.update_status(self.download_id, STATUS_DOWNLOADING)

                chunk_size = 1024 * 64  # 64KB
                with open(self.save_path, file_mode) as file:
                    for chunk in response.iter_bytes(chunk_size=chunk_size):
                        if self._is_cancelled:
                            logger.info(f"Download {self.download_id} cancelled during chunk iteration.")
                            break

                        while self._is_paused:
                            if self._is_cancelled:
                                logger.info(f"Download {self.download_id} cancelled while paused.")
                                break
                            self.msleep(200)
                        if self._is_cancelled: break

                        file.write(chunk)
                        self._downloaded_bytes += len(chunk)

                        # Speed Limit Logic
                        if self.__class__.global_speed_limit_bps and self.__class__.global_speed_limit_bps > 0:
                            current_speed_calc_time = time.time()
                            time_for_chunk_at_limit = len(chunk) / self.__class__.global_speed_limit_bps
                            actual_time_for_chunk = current_speed_calc_time - self._last_timestamp_for_limit
                            if actual_time_for_chunk < time_for_chunk_at_limit:
                                sleep_duration_ms = int((time_for_chunk_at_limit - actual_time_for_chunk) * 1000)
                                if sleep_duration_ms > 0: self.msleep(sleep_duration_ms)
                            self._last_timestamp_for_limit = time.time()

                        progress_percent = int((self._downloaded_bytes / self._total_size) * 100) if self._total_size > 0 else -1
                        speed_str, eta_str = self._calculate_speed_and_eta()
                        if not self._is_cancelled:
                            self.progress_updated.emit(progress_percent, speed_str, eta_str, self._downloaded_bytes,
                                                       self._total_size)

                        now = time.time()
                        if now - self._last_db_write_time >= self._db_write_interval:
                            self.db.update_download_progress_details(self.download_id, max(0, progress_percent),
                                                                     self._downloaded_bytes)
                            self._last_db_write_time = now
                            if self._total_size > 0:
                                if not self._is_cancelled: self.resume_helper.write(self._downloaded_bytes,
                                                                                    self._total_size, self.url)

                if self._is_cancelled:
                    self.status_changed.emit(STATUS_CANCELLED)
                elif self._total_size > 0 and self._downloaded_bytes < self._total_size:
                    self.status_changed.emit(STATUS_INCOMPLETE)
                    self.db.update_status(self.download_id, STATUS_INCOMPLETE)
                elif self._total_size == 0 and self._downloaded_bytes > 0:
                    self.status_changed.emit(STATUS_COMPLETE)
                    self.db.mark_complete(self.download_id, QDateTime.currentDateTime().toString(Qt.ISODate))
                    self.finished_successfully.emit(str(self.save_path))
                elif self._downloaded_bytes >= self._total_size and self._total_size > 0:
                    self.status_changed.emit(STATUS_COMPLETE)
                    self.db.mark_complete(self.download_id, QDateTime.currentDateTime().toString(Qt.ISODate))
                    self.finished_successfully.emit(str(self.save_path))

        except httpx.HTTPStatusError as e:
            err_msg_for_ui = f"HTTP Error: {e.response.status_code}"
            response_text_snippet = "[Response body not read or unavailable]"
            try:
                e.response.read()
                response_text_snippet = e.response.text[:200]
            except httpx.ResponseNotRead:
                logger.warning(
                    f"Download {self.download_id} - Still ResponseNotRead after explicit read() for HTTPStatusError. Body might be empty or stream closed.")
            except httpx.StreamConsumed:
                logger.warning(f"Download {self.download_id} - StreamConsumed when trying to read error response body.")
                if hasattr(e.response, '_content') and e.response._content is not None:
                    try:
                        response_text_snippet = e.response.text[:200]
                    except Exception:
                        pass
            except Exception as read_exc:
                logger.warning(
                    f"Download {self.download_id} - Error reading response body for HTTPStatusError: {type(read_exc).__name__} - {read_exc}")
                response_text_snippet = f"[Error reading response body: {type(read_exc).__name__}]"
            logger.error(
                f"Download {self.download_id} HTTP Error: {e.response.status_code} for URL {e.request.url} - Snippet: {response_text_snippet}")
            if not self._is_cancelled:
                self.status_changed.emit(err_msg_for_ui)
                self.db.update_status(self.download_id, f"Error HTTP {e.response.status_code}")
        except httpx.RequestError as e:
            err_msg = f"Network Error: {type(e).__name__}"
            logger.error(f"Download {self.download_id} Network Error: {e} for URL {self.url}")
            if not self._is_cancelled:
                self.status_changed.emit(err_msg)
                self.db.update_status(self.download_id, "Error (Network)")
        except IOError as e:
            err_msg = f"File Error: {e.strerror}"
            logger.error(f"Download {self.download_id} File Error: {e}")
            if not self._is_cancelled:
                self.status_changed.emit(err_msg)
                self.db.update_status(self.download_id, "Error (File)")
        except Exception as e:
            err_msg = f"Error: {type(e).__name__}"
            logger.error(f"Download {self.download_id} Generic Error: {e}", exc_info=True)
            if not self._is_cancelled:
                self.status_changed.emit(err_msg)
                self.db.update_status(self.download_id, "Error (General)")
        finally:
            self._is_running = False

            if self.httpx_response:
                try:
                    self.httpx_response.close()
                except Exception as e_close:
                    logger.debug(f"Exception closing httpx_response for {self.download_id}: {e_close}")
                self.httpx_response = None
            if self.httpx_client:
                try:
                    self.httpx_client.close()
                except Exception as e_close:
                    logger.debug(f"Exception closing httpx_client for {self.download_id}: {e_close}")
                self.httpx_client = None

            current_status_in_db = self.db.get_download_by_id(self.download_id).get(
                'status') if self.db.get_download_by_id(self.download_id) else None
            if self._is_cancelled or current_status_in_db == STATUS_COMPLETE:
                self.resume_helper.delete()


class QueueSchedulerThread(QThread):
    start_queue_signal = Signal(int)
    stop_queue_signal = Signal(int)

    def __init__(self, db_manager: DatabaseManager, interval_sec=60, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.interval = interval_sec
        self._running = True
        self.last_triggered = {}

    def stop(self):
        self._running = False
        self.wait(self.interval * 1000 + 500)

    def _should_run_today(self, queue_days: list) -> bool:
        if not queue_days:
            return True

        today = QDate.currentDate()
        day_map_to_int = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}

        if len(queue_days) == 1 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", queue_days[0]):
            return today.toString("yyyy-MM-dd") == queue_days[0]

        current_weekday_int = today.dayOfWeek() - 1
        for day_name in queue_days:
            if day_map_to_int.get(day_name) == current_weekday_int:
                return True
        return False

    def run(self):
        while self._running:
            now_dt = QDateTime.currentDateTime()
            queues = self.db.get_all_queues()

            for q_data in queues:
                if not q_data.get("enabled"):
                    continue

                queue_id = q_data["id"]
                start_time_str = q_data.get("start_time")
                stop_time_str = q_data.get("stop_time")
                queue_days_list = q_data.get("days", [])

                if not self._should_run_today(queue_days_list):
                    continue

                queue_trigger = self.last_triggered.setdefault(queue_id, {})

                # === Start Time: Trigger ONLY if current time matches exactly
                if start_time_str:
                    start_qtime = QTime.fromString(start_time_str, "HH:mm")
                    now_time = now_dt.time()

                    if now_time.hour() == start_qtime.hour() and now_time.minute() == start_qtime.minute():
                        last_start_trigger = queue_trigger.get("last_start_action_time")
                        if not last_start_trigger or last_start_trigger.date() != now_dt.date():
                            logger.debug(f"[Scheduler] Starting queue ID: {queue_id} (Name: {q_data['name']})")
                            self.start_queue_signal.emit(queue_id)
                            queue_trigger["last_start_action_time"] = now_dt

                # === Stop Time: Same logic
                if stop_time_str:
                    stop_qtime = QTime.fromString(stop_time_str, "HH:mm")
                    now_time = now_dt.time()

                    if now_time.hour() == stop_qtime.hour() and now_time.minute() == stop_qtime.minute():
                        last_stop_trigger = queue_trigger.get("last_stop_action_time")
                        if not last_stop_trigger or last_stop_trigger.date() != now_dt.date():
                            logger.debug(f"[Scheduler] Stopping queue ID: {queue_id} (Name: {q_data['name']})")
                            self.stop_queue_signal.emit(queue_id)
                            queue_trigger["last_stop_action_time"] = now_dt

            # Sleep loop
            count = 0
            while self._running and count < self.interval:
                self.msleep(1000)
                count += 1


class MetadataFetcher(QThread):
    result = Signal(object)
    error = Signal(str)
    timeout = Signal()

    def __init__(self, url, auth=None, max_seconds=10, custom_headers=None):
        super().__init__()
        self.original_url = url.strip()
        self.auth = auth
        self.final_url = self.original_url
        self.max_seconds = max_seconds
        self.custom_headers = custom_headers if custom_headers is not None else {}

    def run(self):
        try:
            metadata = self._fetch_metadata_attempt(self.original_url)
            if metadata:
                self.result.emit(metadata)
                return
        except httpx.TimeoutException:
            logging.error(f"[MetadataFetcher] Timeout for {self.original_url}")  # Use logging consistently
        except Exception as e:
            logging.error(f"[MetadataFetcher] Error with {self.original_url}: {e}")

        if self.original_url.startswith("https://"):
            fallback_url = self.original_url.replace("https://", "http://", 1)
            logging.debug(f"[MetadataFetcher] Trying HTTP fallback: {fallback_url}")
            try:
                metadata = self._fetch_metadata_attempt(fallback_url)
                if metadata:
                    self.result.emit(metadata)
                    return
            except httpx.TimeoutException:
                logging.error(f"[MetadataFetcher] Timeout for HTTP fallback {fallback_url}")
            except Exception as e:
                logging.error(f"[MetadataFetcher] Error with HTTP fallback {fallback_url}: {e}")

        logging.debug(f"[MetadataFetcher] All attempts failed for {self.original_url}. Emitting timeout.")
        self.timeout.emit()

    def _fetch_metadata_attempt(self, url_to_fetch):
        request_headers = {"User-Agent": "PIDM/1.0 Mozilla/5.0"}
        if self.custom_headers:
            request_headers.update(self.custom_headers)

        auth_param = tuple(self.auth) if self.auth and isinstance(self.auth, (list, tuple)) and len(
            self.auth) == 2 else None

        try:
            transport = httpx.HTTPTransport(retries=1)
            with httpx.Client(auth=auth_param, follow_redirects=True, timeout=self.max_seconds,
                              transport=transport) as client:
                logging.debug(
                    f"[MetadataFetcher] Sending HEAD request to: {url_to_fetch} with headers: {request_headers}")
                response = client.head(url_to_fetch, headers=request_headers)
                response.raise_for_status()
                logging.debug(f"[MetadataFetcher] HEAD response from {response.url} headers: {dict(response.headers)}")

                content_length_head = response.headers.get("Content-Length")
                if content_length_head and int(content_length_head) > 0:
                    logging.debug(f"[MetadataFetcher] Using HEAD data for {url_to_fetch}")
                    return self._parse_headers(response.headers, str(response.url))

                logging.warning(
                    f"[MetadataFetcher] HEAD lacked positive Content-Length (got: {content_length_head}), trying partial GET for {url_to_fetch}")

                headers_for_get = request_headers.copy()
                headers_for_get["Range"] = "bytes=0-0"

                logging.debug(
                    f"[MetadataFetcher] Sending GET (range 0-0) request to: {url_to_fetch} with headers: {headers_for_get}")
                response_get = client.get(url_to_fetch, headers=headers_for_get, stream=True)
                response_get.raise_for_status()
                logging.debug(
                    f"[MetadataFetcher] GET (range 0-0) response from {response_get.url} headers: {dict(response_get.headers)}")

                try:
                    for chunk in response_get.iter_bytes(chunk_size=1):
                        break
                finally:
                    response_get.close()

                return self._parse_headers(response_get.headers, str(response_get.url))

        except httpx.HTTPStatusError as e:
            logging.error(
                f"[MetadataFetcher] HTTPStatusError for {url_to_fetch}: {e.response.status_code} - {e.request.url}")
            if e.response.status_code == 401:
                self.error.emit(self.tr("Authentication failed (401). Please check credentials."))
            elif e.response.status_code == 403:
                self.error.emit(self.tr(f"Access denied (403) for {e.request.url}"))
            elif e.response.status_code == 404:
                self.error.emit(self.tr(f"File not found (404) at {e.request.url}"))
            else:
                self.error.emit(self.tr(f"Server error: {e.response.status_code} for {e.request.url}"))
            return None
        except httpx.TimeoutException:
            logging.error(f"[MetadataFetcher] Timeout during metadata fetch for {url_to_fetch}")
            raise
        except httpx.RequestError as e:
            logging.error(f"[MetadataFetcher] RequestError for {url_to_fetch}: {type(e).__name__} - {e.request.url}")
            self.error.emit(self.tr(f"Network error ({type(e).__name__}) for {e.request.url}"))
            return None
        except Exception as e:
            logging.error(f"[MetadataFetcher] Unknown error during metadata fetch for {url_to_fetch}: {e}")
            self.error.emit(self.tr(f"Fetch error: {type(e).__name__}"))
            return None

    def _parse_headers(self, response_headers, effective_url):
        content_type = response_headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
        size_val = 0
        content_length = response_headers.get("Content-Length")
        content_range = response_headers.get("Content-Range")

        if content_range and '/' in content_range:
            try:
                total_size_str = content_range.split('/')[-1]
                if total_size_str != '*':
                    size_val = int(total_size_str)
            except ValueError:
                logging.warning(f"[MetadataFetcher] Could not parse size from Content-Range: {content_range}")
                pass

        if size_val == 0 and content_length:
            try:
                size_val = int(content_length)
            except ValueError:
                logging.warning(f"[MetadataFetcher] Could not parse size from Content-Length: {content_length}")
                pass

        filename_from_disposition = ""
        content_disposition = response_headers.get("Content-Disposition")
        if content_disposition:
            match_utf8 = re.search(
                r"filename\*=\s*UTF-8''((?:%[0-9A-Fa-f]{2}|[A-Za-z0-9_.~!*'()-])+)|\bfilename\*=\s*([^';\s]+)",
                content_disposition)
            if match_utf8:
                filename_value = match_utf8.group(1) or match_utf8.group(2)
                try:
                    if match_utf8.group(1):
                        filename_from_disposition = QUrl.fromPercentEncoding(filename_value.encode('ascii')).strip('" ')
                    else:
                        filename_from_disposition = QUrl.fromPercentEncoding(filename_value.encode('latin-1')).strip(
                            '" ')
                except Exception as e_decode:
                    logging.warning(
                        f"[MetadataFetcher] Could not decode filename from Content-Disposition (utf8*): {filename_value}, error: {e_decode}")
                    filename_from_disposition = filename_value.strip('" ')  # Use raw if decoding fails

            if not filename_from_disposition:
                match_simple = re.search(r'filename="([^"]+)"', content_disposition)
                if match_simple:
                    filename_from_disposition = match_simple.group(1)
                else:
                    match_no_quotes = re.search(r'filename=([^;\s]+)', content_disposition)
                    if match_no_quotes:
                        filename_from_disposition = match_no_quotes.group(1).strip('" ')

        if not filename_from_disposition:
            try:
                parsed_url = QUrl(effective_url)
                parsed_url.setQuery("")
                filename_from_url = Path(parsed_url.path()).name
                if filename_from_url:
                    filename_from_disposition = QUrl.fromPercentEncoding(
                        filename_from_url.encode('utf-8')).strip()
                else:
                    filename_from_disposition = "downloaded_file"
            except Exception as e_url_parse:
                logging.warning(f"[MetadataFetcher] Could not parse filename from URL {effective_url}: {e_url_parse}")
                filename_from_disposition = "downloaded_file"

        if "." not in filename_from_disposition and content_type != "application/octet-stream":
            ext = mimetypes.guess_extension(content_type, strict=False)
            if ext:
                filename_from_disposition += ext

        filename_from_disposition = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', filename_from_disposition)
        filename_from_disposition = filename_from_disposition[:200]

        return {
            "content_length": size_val,
            "content_type": content_type,
            "filename": filename_from_disposition,
            "final_url": effective_url
        }


class UpdateCheckThread(QThread):
    update_available = Signal(str, str)  # version, url

    def __init__(self, current_version: str, parent=None):
        super().__init__(parent)
        self.current_version = current_version
        self.repo_api_url = "https://api.github.com/repos/saeedmasoudie/PIDM/releases/latest"

    def run(self):
        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.get(self.repo_api_url)
                if response.status_code != 200:
                    return

                data = response.json()
                latest_version = data["tag_name"].lstrip("v")
                release_url = data.get("html_url", "https://github.com/saeedmasoudie/PIDM/releases")

                if latest_version > self.current_version:
                    self.update_available.emit(latest_version, release_url)

        except httpx.RequestError:
            # Fail silently on network errors
            pass


class ProxyServer(QObject):
    link_received = Signal(dict)

    def __init__(self, start_port=49152, max_tries=10):
        super().__init__()
        self.server = None
        self.thread = None
        self.port = None
        self._start_server(start_port, max_tries)

    def _start_server(self, start_port, max_tries):
        for port_to_try in range(start_port, start_port + max_tries):
            try:
                handler = self._make_handler()
                self.server = HTTPServer(("127.0.0.1", port_to_try), handler)
                self.port = port_to_try
                self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
                self.thread.start()
                logger.info(f"[ProxyServer] Listening on http://127.0.0.1:{self.port}")
                return
            except OSError:
                logger.warning(f"[ProxyServer] Port {port_to_try} is already in use. Trying next one.")
                continue
        logger.error("[ProxyServer] FATAL: Failed to bind to any port in the range.")
        self.server = None

    def _make_handler(self):
        outer_self = self

        class Handler(BaseHTTPRequestHandler):
            def _set_headers(self, status=200):
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def do_OPTIONS(self):
                self._set_headers()

            def do_GET(self):
                if self.path == '/api/ping':
                    self._set_headers(200)
                    self.wfile.write(b'{"status":"pidm_active"}')
                else:
                    self._set_headers(404)
                    self.wfile.write(b'{"error":"not_found"}')

            def do_POST(self):
                if self.path != "/api/download":
                    self._set_headers(404)
                    self.wfile.write(b'{"error":"invalid path"}')
                    return

                content_length = int(self.headers.get("Content-Length", 0))
                raw_data = self.rfile.read(content_length)

                try:
                    data = json.loads(raw_data)
                    if "url" in data:
                        download_info = {
                            "url": data.get("url"), "cookies": data.get("cookies"),
                            "referrer": data.get("referrer"), "userAgent": data.get("userAgent")
                        }
                        outer_self.link_received.emit(download_info)
                        self._set_headers(200)
                        self.wfile.write(b'{"status":"ok", "message":"Download info received"}')
                    else:
                        self._set_headers(400)
                        self.wfile.write(b'{"error":"missing url in payload"}')
                except json.JSONDecodeError:
                    self._set_headers(400); self.wfile.write(b'{"error":"invalid json"}')
                except Exception as e:
                    self._set_headers(500); self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))

            def log_message(self, format, *args):
                return

        return Handler


class CredentialDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Authentication Required"))
        self.setModal(True)

        layout = QFormLayout(self)
        self.username_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)

        layout.addRow(self.tr("Username:"), self.username_input)
        layout.addRow(self.tr("Password:"), self.password_input)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_credentials(self):
        if self.result() == QDialog.Accepted:
            return self.username_input.text(), self.password_input.text()
        return None, None


class UrlInputDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Enter Download URL"))
        self.setMinimumWidth(450)
        self.url = None
        self.metadata = None
        self.auth_tuple = None
        self.fetcher_thread = None

        layout = QVBoxLayout(self)
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText(self.tr("Enter the download URL here..."))
        layout.addWidget(QLabel(self.tr("URL:")))
        layout.addWidget(self.url_input)

        self.auth_checkbox = QCheckBox(self.tr("This download requires authentication (username/password)"))
        layout.addWidget(self.auth_checkbox)

        self.status_label = QLabel()
        layout.addWidget(self.status_label)

        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.button(QDialogButtonBox.Ok).setText(self.tr("Next"))
        self.button_box.accepted.connect(self.handle_next_clicked)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

        self.button_box.button(QDialogButtonBox.Ok).setEnabled(False)
        self.url_input.textChanged.connect(
            lambda text: self.button_box.button(QDialogButtonBox.Ok).setEnabled(bool(text.strip()))
        )

    def handle_next_clicked(self):
        url_text = self.url_input.text().strip()
        if not url_text:
            QMessageBox.warning(self, self.tr("Invalid URL"), self.tr("Please enter a valid URL."))
            return

        self.auth_tuple = None
        if self.auth_checkbox.isChecked():
            cred_dialog = CredentialDialog(self)
            if cred_dialog.exec() == QDialog.Accepted:
                self.auth_tuple = cred_dialog.get_credentials()
                if not self.auth_tuple or not self.auth_tuple[0]:
                    self.auth_tuple = None
            else:
                return

        self.url = url_text

        # Basic metadata guess — actual MetadataFetcher runs in MainWindow
        filename_from_url = Path(QUrl(self.url).path()).name or "unknown_file"
        self.metadata = {
            "content_length": 0,
            "content_type": "application/octet-stream",
            "filename": filename_from_url,
            "final_url": self.url
        }

        self.accept()  # Let the parent handle showing NewDownloadDialog


    def get_url_and_metadata(self):
        if self.metadata and "final_url" not in self.metadata:
            self.metadata["final_url"] = self.url
        return self.url, self.metadata, self.auth_tuple


class SelectQueueDialog(QDialog):

    def __init__(self, db_manager: DatabaseManager, settings_manager: SettingsManager, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Select a Queue"))
        self.setMinimumWidth(350)

        self.db = db_manager
        self.settings = settings_manager

        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        layout.addWidget(QLabel(self.tr("Please select a queue for this download:")))

        self.queue_combo = QComboBox()
        self.load_queues()
        layout.addWidget(self.queue_combo)

        self.remember_queue_checkbox = QCheckBox(self.tr("Remember this choice for next time"))
        layout.addWidget(self.remember_queue_checkbox)

        if self.settings.get("remembered_queue_id_for_later"):
            self.remember_queue_checkbox.setChecked(True)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def load_queues(self):
        self.queue_combo.clear()
        all_queues = self.db.get_all_queues()

        if not all_queues:
            self.queue_combo.addItem(self.tr("No queues available"), None)
            self.queue_combo.setEnabled(False)
            ok_button = self.findChild(QDialogButtonBox).button(QDialogButtonBox.Ok)
            if ok_button:
                ok_button.setEnabled(False)
        else:
            for q in all_queues:
                self.queue_combo.addItem(q["name"], q["id"])

            remembered_id = self.settings.get("remembered_queue_id_for_later")
            if remembered_id is not None:
                idx = self.queue_combo.findData(remembered_id)
                if idx != -1:
                    self.queue_combo.setCurrentIndex(idx)

    def get_selected_queue_info(self) -> tuple[int | None, bool]:
        queue_id = self.queue_combo.currentData()
        remember_choice = self.remember_queue_checkbox.isChecked()
        return queue_id, remember_choice


class NewDownloadDialog(QDialog):
    download_added = Signal()

    def __init__(self, settings_manager: SettingsManager, database_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Add New Download"))
        self.setMinimumSize(600, 320)

        # --- Instance Variables ---
        self.settings = settings_manager
        self.db = database_manager
        self.fetched_metadata = {}
        self.guessed_filename = "downloaded_file"
        self.final_download_id = None
        self.accepted_action = None
        self.auth_tuple = None
        self.custom_headers_for_download = None

        apply_standalone_style(self, self.settings)

        # --- UI Setup ---
        form_layout = QFormLayout()
        form_layout.setHorizontalSpacing(15)
        form_layout.setVerticalSpacing(10)

        self.url_display = QLineEdit()
        self.url_display.setReadOnly(True)
        self.description_input = QLineEdit()
        self.description_input.setPlaceholderText(self.tr("Optional description..."))

        self.save_path_combo = QComboBox()
        self.save_path_combo.setEditable(True)
        self.save_path_combo.setMinimumWidth(300)
        self.save_path_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.save_path_combo.activated.connect(self._on_save_path_selected)

        recent_paths = self.settings.get("recent_paths", [])
        if recent_paths: self.save_path_combo.addItems(recent_paths)
        default_dl_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
        if not self.save_path_combo.currentText(): self.save_path_combo.setCurrentText(default_dl_dir)

        browse_button = QPushButton(self.tr("Browse..."))
        browse_button.setFixedWidth(100)
        browse_button.clicked.connect(self.select_save_path)

        save_path_layout = QHBoxLayout()
        save_path_layout.addWidget(self.save_path_combo, 1)
        save_path_layout.addWidget(browse_button)

        self.remember_path_checkbox = QCheckBox(self.tr("Remember this save directory as default"))

        # <<< FIX: Queue selection widgets are removed from this dialog
        form_layout.addRow(self.tr("URL:"), self.url_display)
        form_layout.addRow(self.tr("Save As:"), save_path_layout)
        form_layout.addRow("", self.remember_path_checkbox)
        form_layout.addRow(self.tr("Description:"), self.description_input)

        # (The rest of the UI setup is the same as before)
        preview_widget = QWidget()
        preview_layout = QVBoxLayout(preview_widget)
        preview_widget.setFixedWidth(150)
        self.file_icon_label = QLabel()
        self.file_icon_label.setFixedSize(48, 48)
        self.file_icon_label.setAlignment(Qt.AlignCenter)
        self.file_icon_label.setScaledContents(False)
        self.file_name_preview_label = QLabel(self.tr("Filename: N/A"))
        self.file_name_preview_label.setAlignment(Qt.AlignCenter)
        self.file_name_preview_label.setWordWrap(True)
        self.file_size_preview_label = QLabel(self.tr("Size: N/A"))
        self.file_size_preview_label.setAlignment(Qt.AlignCenter)
        icon_wrapper = QHBoxLayout()
        icon_wrapper.addStretch()
        icon_wrapper.addWidget(self.file_icon_label)
        icon_wrapper.addStretch()
        preview_layout.addLayout(icon_wrapper)
        preview_layout.addWidget(self.file_name_preview_label)
        preview_layout.addWidget(self.file_size_preview_label)
        preview_layout.addStretch()
        top_layout = QHBoxLayout()
        top_layout.addLayout(form_layout, stretch=3)
        top_layout.addWidget(preview_widget, stretch=1)
        button_box = QDialogButtonBox()
        self.download_later_btn = button_box.addButton(self.tr("Download Later"), QDialogButtonBox.ActionRole)
        self.download_now_btn = button_box.addButton(self.tr("Download Now"), QDialogButtonBox.AcceptRole)
        cancel_btn = button_box.addButton(QDialogButtonBox.Cancel)
        self.download_later_btn.clicked.connect(self._handle_download_later)
        self.download_now_btn.clicked.connect(self._handle_download_now)
        cancel_btn.clicked.connect(self.reject)
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(12)
        main_layout.addLayout(top_layout)
        main_layout.addSpacing(10)
        main_layout.addWidget(button_box)

    def _on_save_path_selected(self, index: int):
        """When a user selects a recent path, combine it with the current filename."""
        selected_path_str = self.save_path_combo.itemText(index)
        path_obj = Path(selected_path_str)

        if path_obj.is_dir():
            current_filename = self.fetched_metadata.get("filename") or self.guessed_filename
            new_full_path = str(path_obj / current_filename)
            self.save_path_combo.setCurrentText(new_full_path)

    def load_queues_into_combo(self):
        self.queue_combo.clear()
        self.queue_combo.addItem(self.tr("None (Start Immediately)"), None)
        queues = self.db.get_all_queues()
        for q in queues:
            self.queue_combo.addItem(q["name"], q["id"])

        remembered_queue_id = self.settings.get("remembered_queue_id")
        if remembered_queue_id is not None:
            idx = self.queue_combo.findData(remembered_queue_id)
            if idx != -1:
                self.queue_combo.setCurrentIndex(idx)
                self.remember_queue_checkbox.setChecked(True)

    def fetch_metadata_async(self, url: str, auth: tuple | None = None, custom_headers=None):
        self.custom_headers_for_download = custom_headers
        self._metadata_fetcher = MetadataFetcher(url, auth, custom_headers=custom_headers)
        self._metadata_fetcher.result.connect(self.update_metadata_fields)
        self._metadata_fetcher.error.connect(self._handle_metadata_error)
        self._metadata_fetcher.timeout.connect(self._handle_metadata_timeout)
        QTimer.singleShot(0, self._metadata_fetcher.start)

    def _handle_metadata_error(self, error_message):
        QMessageBox.warning(self, self.tr("Metadata Error"),
                            self.tr("Could not fetch download information:\n{0}").format(error_message))
        logger.error(f"Metadata Error Slot Called: {error_message}")

    def _handle_metadata_timeout(self):
        QMessageBox.warning(self, self.tr("Metadata Timeout"),
                            self.tr("Fetching download information timed out."))
        logger.warning("Metadata Timeout Slot Called")

    def update_metadata_fields(self, metadata: dict):
        logger.debug(f"NewDownloadDialog: Updating metadata fields with: {metadata}")
        self.fetched_metadata = metadata
        filename = metadata.get("filename", self.guessed_filename)
        size_bytes = metadata.get("content_length", 0)

        self.file_name_preview_label.setText(self.tr("Filename: ") + filename)
        self.file_size_preview_label.setText(self.tr("Size: ") + format_size(size_bytes))

        icon = get_system_icon_for_file(filename)
        pixmap = icon.pixmap(48, 48)
        self.file_icon_label.setPixmap(
            pixmap if not pixmap.isNull() else QIcon(get_asset_path("assets/icons/file-x.svg")).pixmap(48, 48)
        )

        current_path_in_combo = self.save_path_combo.currentText().strip()
        if not current_path_in_combo or \
                Path(current_path_in_combo).is_dir() or \
                Path(current_path_in_combo).name == "downloaded_file":
            default_save_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
            self.save_path_combo.setCurrentText(str(Path(default_save_dir) / filename))

    def set_initial_data(self, url: str, metadata: dict | None, auth: tuple | None):
        self.url_display.setText(url)
        self.auth_tuple = auth
        self.fetched_metadata = metadata or {}
        filename = self.fetched_metadata.get("filename") or guess_filename_from_url(url)
        self.guessed_filename = filename
        size_bytes = self.fetched_metadata.get("content_length", 0)
        icon = get_system_icon_for_file(filename)
        pixmap = icon.pixmap(48, 48)
        self.file_icon_label.setPixmap(pixmap if not pixmap.isNull() else QIcon(get_asset_path("assets/icons/file-x.svg")).pixmap(48, 48))
        self.file_name_preview_label.setText(self.tr("Filename: ") + filename)
        self.file_size_preview_label.setText(self.tr("Size: ") + format_size(size_bytes))
        current_path_in_combo = self.save_path_combo.currentText().strip()
        if not current_path_in_combo or Path(current_path_in_combo).is_dir():
            default_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
            self.save_path_combo.setCurrentText(str(Path(default_dir) / filename))

    def select_save_path(self):
        current_path_str = self.save_path_combo.currentText().strip()
        current_path_obj = Path(current_path_str if current_path_str else ".")

        start_dir = str(current_path_obj.parent) if current_path_obj.name else \
            self.settings.get("default_download_directory", str(Path.home() / "Downloads"))

        filename_suggestion = current_path_obj.name or \
                              self.fetched_metadata.get("filename") or \
                              self.guessed_filename

        if not Path(start_dir).is_dir():
            start_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
            if not Path(start_dir).is_dir():
                start_dir = str(Path.home() / "Downloads")
                Path(start_dir).mkdir(parents=True, exist_ok=True)

        target_path_suggestion = str(Path(start_dir) / filename_suggestion)

        ext = Path(filename_suggestion).suffix
        mime_type = self.fetched_metadata.get("content_type", "application/octet-stream")
        if not ext and mime_type != "application/octet-stream":
            guessed_ext = mimetypes.guess_extension(mime_type)
            if guessed_ext:
                ext = guessed_ext

        file_filter = f"{mime_type} (*{ext})" if ext else self.tr("All Files (*)")

        selected_path, _ = QFileDialog.getSaveFileName(
            self,
            self.tr("Save File As"),
            target_path_suggestion,
            file_filter
        )

        if selected_path:
            self.save_path_combo.setCurrentText(selected_path)

    def _prepare_download_data(self, status_on_add: str, queue_id: int | None) -> dict | None:
        """This method now accepts the queue_id as a parameter."""
        download_path_str = self.save_path_combo.currentText().strip()
        if not download_path_str:
            QMessageBox.warning(self, self.tr("Save Path Required"), self.tr("Please specify where to save the file."))
            return None

        if self.remember_path_checkbox.isChecked():
            self.settings.set("default_download_directory", str(Path(download_path_str).parent))
        self.settings.add_recent_path(str(Path(download_path_str).parent))

        queue_pos = 0
        if queue_id is not None:
            items_in_q = self.db.get_downloads_in_queue(queue_id)
            queue_pos = max((d.get("queue_position", 0) for d in items_in_q), default=-1) + 1

        data_for_db = {
            "url": self.fetched_metadata.get("final_url", self.url_display.text()),
            "file_name": Path(download_path_str).name,
            "save_path": download_path_str,
            "description": self.description_input.text().strip(),
            "status": status_on_add,
            "queue_id": queue_id,
            "queue_position": queue_pos,
            "total_size": self.fetched_metadata.get("content_length", 0),
            "referrer": self.custom_headers_for_download.get("Referer") if self.custom_headers_for_download else None,
            "custom_headers": self.custom_headers_for_download,
            "auth": self.auth_tuple,
        }
        return data_for_db

    def _handle_download_later(self):
        self.accepted_action = "later"

        # <<< FIX: Create and show the new queue selection dialog
        queue_dialog = SelectQueueDialog(self.db, self.settings, self)
        if queue_dialog.exec() != QDialog.Accepted:
            return  # User cancelled the queue selection

        selected_queue_id, remember_choice = queue_dialog.get_selected_queue_info()

        if selected_queue_id is None:
            QMessageBox.warning(self, self.tr("No Queue Selected"),
                                self.tr("You must select a queue to use 'Download Later'."))
            return

        # Handle the "remember" checkbox setting
        if remember_choice:
            self.settings.set("remembered_queue_id_for_later", selected_queue_id)
        else:
            # If they uncheck it, forget the setting
            self.settings.set("remembered_queue_id_for_later", None)

        # Now that we have a queue_id, prepare the payload with QUEUED status
        db_payload = self._prepare_download_data(STATUS_QUEUED, selected_queue_id)
        if not db_payload: return

        self.final_download_id = self.db.add_download(db_payload)
        if self.final_download_id == -1:
            QMessageBox.warning(self, self.tr("Error Adding Download"),
                                self.tr(
                                    "This download (URL and save path) might already exist or another error occurred."))
            return

        self.download_added.emit()
        super().accept()

    def _handle_download_now(self):
        self.accepted_action = "now"

        # <<< FIX: "Download Now" is now simple: it never uses a queue.
        db_payload = self._prepare_download_data(STATUS_CONNECTING, queue_id=None)
        if not db_payload: return

        self.final_download_id = self.db.add_download(db_payload)
        if self.final_download_id == -1:
            QMessageBox.warning(self, self.tr("Error Adding Download"),
                                self.tr(
                                    "This download (URL and save path) might already exist or another error occurred."))
            return

        self.download_added.emit()
        super().accept()

    def get_final_download_data(self) -> tuple[int | None, str | None]:
        if self.result() == QDialog.Accepted and self.final_download_id is not None:
            return self.final_download_id, self.accepted_action
        return None, None


class NewBatchDownloadDialog(QDialog):
    def __init__(self, settings: SettingsManager, db: DatabaseManager, urls: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Batch Downloads")
        self.setMinimumSize(750, 500)

        self.settings = settings
        self.db = db
        self.final_ids = []
        self.download_rows = []
        apply_standalone_style(self, self.settings)

        layout = QVBoxLayout(self)

        # Table setup (6 columns: checkbox, url, filename, size, type, status)
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["✔", "URL", "Filename", "Size", "Type", "Status"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setColumnWidth(0, 40)  # Checkbox
        layout.addWidget(self.table)

        for url in urls:
            self._add_row(url.strip())

        # Save path & browse
        layout.addWidget(QLabel("Save Directory:"))
        path_layout = QHBoxLayout()
        self.save_path_combo = QComboBox()
        self.save_path_combo.setEditable(True)
        default_path = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
        self.save_path_combo.setCurrentText(default_path)
        for p in self.settings.get("recent_paths", []):
            self.save_path_combo.addItem(p)

        browse_btn = QPushButton("Browse")
        browse_btn.clicked.connect(self.browse_path)
        path_layout.addWidget(self.save_path_combo, 1)
        path_layout.addWidget(browse_btn)
        layout.addLayout(path_layout)

        # Queue
        layout.addWidget(QLabel("Add to Queue:"))
        self.queue_combo = QComboBox()
        self.queue_combo.addItem("None (Start Immediately)", None)
        for q in self.db.get_all_queues():
            self.queue_combo.addItem(q["name"], q["id"])
        layout.addWidget(self.queue_combo)

        self.remember_path_checkbox = QCheckBox("Remember this directory")
        layout.addWidget(self.remember_path_checkbox)

        # Buttons
        btns = QDialogButtonBox()
        self.btn_later = btns.addButton("Download Later", QDialogButtonBox.ActionRole)
        self.btn_now = btns.addButton("Download Now", QDialogButtonBox.AcceptRole)
        btns.addButton(QDialogButtonBox.Cancel)
        layout.addWidget(btns)

        self.btn_later.clicked.connect(lambda: self._accept("later"))
        self.btn_now.clicked.connect(lambda: self._accept("now"))
        btns.rejected.connect(self.reject)

    def _add_row(self, url):
        row = self.table.rowCount()
        self.table.insertRow(row)

        # Column 0: Checkbox
        check_item = QTableWidgetItem()
        check_item.setCheckState(Qt.Checked)
        check_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        self.table.setItem(row, 0, check_item)

        # Columns 1-5: Info
        self.table.setItem(row, 1, QTableWidgetItem(url))
        self.table.setItem(row, 2, QTableWidgetItem("Fetching..."))
        self.table.setItem(row, 3, QTableWidgetItem("..."))
        self.table.setItem(row, 4, QTableWidgetItem("..."))
        status_item = QTableWidgetItem("Fetching...")
        self.table.setItem(row, 5, status_item)

        self.download_rows.append({
            "url": url,
            "metadata": None,
            "row_index": row,
            "status_item": status_item,
        })

        self._fetch_metadata_for_row(row, url)

    def _fetch_metadata_for_row(self, row, url):
        thread = MetadataFetcher(url, auth=None)
        thread.result.connect(lambda meta: self._set_metadata(row, meta))
        thread.timeout.connect(lambda: self._set_error(row))
        thread.error.connect(lambda msg: self._set_error(row))
        thread.start()

    def _set_metadata(self, row, meta):
        self.table.item(row, 2).setText(meta.get("filename", "unknown"))
        self.table.item(row, 3).setText(format_size(meta.get("content_length", 0)))
        self.table.item(row, 4).setText(meta.get("content_type", ""))
        self.table.item(row, 5).setText("Ready")
        self.download_rows[row]["metadata"] = meta

    def _set_error(self, row):
        self.table.item(row, 2).setText("Error")
        self.table.item(row, 3).setText("N/A")
        self.table.item(row, 4).setText("Unknown")
        error_item = self.table.item(row, 5)
        error_item.setText("Failed")
        error_item.setForeground(QColor("red"))

    def browse_path(self):
        path = QFileDialog.getExistingDirectory(self, "Select Save Directory", self.save_path_combo.currentText())
        if path:
            self.save_path_combo.setCurrentText(path)

    def _accept(self, how):
        self.accepted_action = how
        self._process_downloads(STATUS_CONNECTING if how == "now" else STATUS_QUEUED)
        self.accept()

    def _process_downloads(self, status):
        self.final_ids = []
        save_dir = self.save_path_combo.currentText().strip()
        queue_id = self.queue_combo.currentData()

        if self.remember_path_checkbox.isChecked():
            self.settings.set("default_download_directory", save_dir)

        self.settings.add_recent_path(save_dir)

        for row in self.download_rows:
            row_idx = row["row_index"]
            if self.table.item(row_idx, 0).checkState() != Qt.Checked:
                continue

            meta = row["metadata"]
            if not meta:
                continue

            filename = self.table.item(row_idx, 2).text()
            full_path = str(Path(save_dir) / filename)
            payload = {
                "url": meta.get("final_url", row["url"]),
                "file_name": filename,
                "save_path": full_path,
                "description": "",
                "status": status,
                "queue_id": queue_id,
                "queue_position": 0,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_size": meta.get("content_length", 0),
                "progress": 0, "speed": "", "eta": "", "bytes_downloaded": 0,
            }

            if queue_id is not None:
                items = self.db.get_downloads_in_queue(queue_id)
                payload["queue_position"] = max((d.get("queue_position", 0) for d in items), default=0) + 1

            dl_id = self.db.add_download(payload)
            if dl_id != -1:
                self.final_ids.append(dl_id)

    def get_results(self):
        return self.final_ids, self.accepted_action


class QueueSelectionDialog(QDialog):
    def __init__(self, db_manager: DatabaseManager, settings_manager: SettingsManager, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Select Queue"))
        self.setMinimumWidth(350)

        self.db = db_manager
        self.settings = settings_manager

        self.selected_queue_id = None
        self.remember_choice = False

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(self.tr("Choose a queue to add the download to:")))

        self.queue_combo = QComboBox()
        self.load_queues()
        layout.addWidget(self.queue_combo)

        self.remember_checkbox = QCheckBox(self.tr("Remember this queue for future 'Download Later' actions"))
        remembered_id = self.settings.get("remembered_queue_id")
        if remembered_id is not None:
            idx = self.queue_combo.findData(remembered_id)
            if idx != -1:
                self.queue_combo.setCurrentIndex(idx)
                self.remember_checkbox.setChecked(True)
        layout.addWidget(self.remember_checkbox)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def load_queues(self):
        self.queue_combo.clear()
        queues = self.db.get_all_queues()
        if not queues:
            main_queue_id = self.db.add_queue("Main Queue", enabled=1)
            if main_queue_id != -1:
                self.queue_combo.addItem("Main Queue", main_queue_id)
        else:
            for q in queues:
                self.queue_combo.addItem(q["name"], q["id"])

    def accept(self):
        self.selected_queue_id = self.queue_combo.currentData()
        self.remember_choice = self.remember_checkbox.isChecked()

        if self.remember_choice and self.selected_queue_id is not None:
            self.settings.set("remembered_queue_id", self.selected_queue_id)
        elif self.remember_choice and self.selected_queue_id is None:
            pass

        super().accept()

    def get_selection(self) -> tuple[int | None, bool]:
        if self.result() == QDialog.Accepted:
            return self.selected_queue_id, self.remember_choice
        return None, False


class LogViewerDialog(QDialog):
    def __init__(self, log_file_path: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Application Logs"))
        self.resize(850, 600)

        self.log_file_path = log_file_path
        self.auto_refresh_enabled = False

        layout = QVBoxLayout(self)

        # --- Top: Search bar ---
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel(self.tr("Search:")))

        self.search_input = QLineEdit(self)
        self.search_input.setPlaceholderText(self.tr("Filter log content..."))
        self.search_input.textChanged.connect(self.filter_logs)
        search_layout.addWidget(self.search_input)

        self.auto_refresh_checkbox = QCheckBox(self.tr("Auto-refresh"))
        self.auto_refresh_checkbox.stateChanged.connect(self.toggle_auto_refresh)
        search_layout.addWidget(self.auto_refresh_checkbox)

        layout.addLayout(search_layout)

        # --- Log Viewer ---
        self.log_edit = QTextEdit(self)
        self.log_edit.setReadOnly(True)
        self.log_edit.setLineWrapMode(QTextEdit.NoWrap)
        layout.addWidget(self.log_edit)

        # --- Bottom Buttons ---
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.refresh_button = QPushButton(self.tr("Refresh"))
        self.refresh_button.clicked.connect(self.load_logs)
        btn_layout.addWidget(self.refresh_button)

        self.export_button = QPushButton(self.tr("Save As..."))
        self.export_button.clicked.connect(self.export_log)
        btn_layout.addWidget(self.export_button)

        layout.addLayout(btn_layout)

        # Timer for auto-refresh
        self.refresh_timer = QTimer(self)
        self.refresh_timer.setInterval(5000)  # 5 seconds
        self.refresh_timer.timeout.connect(self.load_logs)

        # Load logs on init
        self.load_logs()

    def load_logs(self):
        if os.path.exists(self.log_file_path):
            with open(self.log_file_path, "r", encoding="utf-8") as f:
                self.full_log_content = f.read()
            self.filter_logs()
        else:
            self.full_log_content = ""
            self.log_edit.setPlainText(self.tr("Log file not found."))

    def filter_logs(self):
        query = self.search_input.text().lower().strip()
        if not query:
            self.log_edit.setPlainText(self.full_log_content)
        else:
            filtered_lines = [
                line for line in self.full_log_content.splitlines()
                if query in line.lower()
            ]
            self.log_edit.setPlainText("\n".join(filtered_lines))

    def toggle_auto_refresh(self, checked):
        self.auto_refresh_enabled = checked
        if checked:
            self.refresh_timer.start()
        else:
            self.refresh_timer.stop()

    def export_log(self):
        path, _ = QFileDialog.getSaveFileName(
            self,
            self.tr("Save Log As"),
            "pidm-log.txt",
            self.tr("Text Files (*.txt);;All Files (*)")
        )
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.log_edit.toPlainText())


class DownloadInfoWindow(QWidget):
    def __init__(self, worker: DownloadWorker, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Download Info"))
        self.setFixedSize(400, 200)

        self.worker = worker

        layout = QVBoxLayout(self)

        self.status_label = QLabel(self.tr("Status: Connecting"))
        self.progress_bar = QProgressBar()
        self.speed_label = QLabel(self.tr("Speed: 0 KB/s"))
        self.eta_label = QLabel(self.tr("ETA: --"))
        self.transferred_label = QLabel(self.tr("Transferred: 0 B / 0 B"))

        layout.addWidget(self.status_label)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.speed_label)
        layout.addWidget(self.eta_label)
        layout.addWidget(self.transferred_label)

        self.worker.progress_updated.connect(self._update_display)
        self.worker.status_changed.connect(lambda msg: self.status_label.setText(self.tr("Status: ") + self.tr(msg)))

        self._update_display(
            int((worker._downloaded_bytes / worker._total_size) * 100) if worker._total_size > 0 else 0,
            "0 KB/s", "--", worker._downloaded_bytes, worker._total_size
        )

    @Slot(int, str, str, int, int)
    def _update_display(self, percent, speed_str, eta_str, downloaded, total):
        self.progress_bar.setValue(percent if percent >= 0 else 0)
        if total == 0 and percent == -1:
            self.progress_bar.setRange(0, 0)
            self.progress_bar.setFormat(self.tr("Downloading..."))
        else:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setFormat(f"{percent}%")

        self.speed_label.setText(self.tr("Speed: ") + speed_str)
        self.eta_label.setText(self.tr("ETA: ") + eta_str)
        self.transferred_label.setText(
            f"{self.tr('Transferred')}: {format_size(downloaded)} / {format_size(total) if total > 0 else self.tr('Unknown')}")


class PropertiesDialog(QDialog):
    def __init__(self, download_data: dict, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Download Properties"))
        self.setMinimumWidth(550)
        self.download_data = download_data.copy()
        self.db = db_manager

        layout = QVBoxLayout(self)

        # --- Icon ---
        icon_label = QLabel()
        icon = get_system_icon_for_file(download_data.get("file_name", "unknown_file.dat"))
        icon_label.setPixmap(icon.pixmap(48, 48))
        icon_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(icon_label)

        form_layout = QFormLayout()
        form_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        # --- File Name (Read-only, derived from save_path change) ---
        self.file_name_label = QLabel(download_data.get('file_name', self.tr("N/A")))
        form_layout.addRow(self.tr("<b>File Name:</b>"), self.file_name_label)

        # --- Description (Editable) ---
        self.description_edit = QLineEdit(download_data.get('description', ''))
        form_layout.addRow(self.tr("<b>Description:</b>"), self.description_edit)

        # --- Status (Read-only) ---
        form_layout.addRow(self.tr("<b>Status:</b>"), QLabel(self.tr(download_data.get('status', self.tr("N/A")))))

        # --- Size (Read-only) ---
        size_bytes = download_data.get("total_size", 0)
        form_layout.addRow(self.tr("<b>Size:</b>"), QLabel(format_size(size_bytes)))

        # --- Downloaded (Read-only) ---
        downloaded_bytes = download_data.get("bytes_downloaded", 0)
        form_layout.addRow(self.tr("<b>Downloaded:</b>"), QLabel(format_size(downloaded_bytes)))

        # --- Speed & ETA (Read-only, if available) ---
        speed = download_data.get('speed')
        if speed:
            form_layout.addRow(self.tr("<b>Speed:</b>"), QLabel(speed))

        eta = download_data.get('eta')
        if eta:
            form_layout.addRow(self.tr("<b>ETA:</b>"), QLabel(eta))

        # --- Save Path (Editable) ---
        self.save_path_edit = QLineEdit(download_data.get('save_path', ''))
        browse_btn = QPushButton(self.tr("Browse..."))
        browse_btn.clicked.connect(self._browse_save_path)
        path_layout = QHBoxLayout()
        path_layout.addWidget(self.save_path_edit, 1)
        path_layout.addWidget(browse_btn)
        form_layout.addRow(self.tr("<b>Save Path:</b>"), path_layout)

        # --- URL (Read-only Link) ---
        url_text = download_data.get("url", "")
        url_label_text = f'<a href="{url_text}">{url_text}</a>' if url_text else self.tr("N/A")
        url_label = QLabel(url_label_text)
        url_label.setOpenExternalLinks(True)
        url_label.setWordWrap(True)
        form_layout.addRow(self.tr("<b>URL:</b>"), url_label)

        # --- Referrer URL (Read-only Link, if exists) ---
        referrer_url = download_data.get("referrer")
        if referrer_url:
            referrer_label_text = f'<a href="{referrer_url}">{referrer_url}</a>'
            referrer_label = QLabel(referrer_label_text)
            referrer_label.setOpenExternalLinks(True)
            referrer_label.setWordWrap(True)
            form_layout.addRow(self.tr("<b>Referrer:</b>"), referrer_label)

        # --- Queue (Read-only, if part of a queue) ---
        queue_id = download_data.get("queue_id")
        if queue_id is not None:
            queue_data = self.db.get_queue_by_id(queue_id)
            queue_name = queue_data.get("name", self.tr("Unknown Queue")) if queue_data else self.tr("N/A")
            form_layout.addRow(self.tr("<b>Queue:</b>"), QLabel(queue_name))

        # --- Added On (Read-only) ---
        created_at_str = download_data.get('created_at', '')
        created_dt = QDateTime.fromString(created_at_str, Qt.ISODate)
        if not created_dt.isValid() and created_at_str:
            created_dt = QDateTime.fromString(created_at_str, "yyyy-MM-dd HH:mm:ss")
        form_layout.addRow(self.tr("<b>Added On:</b>"), QLabel(
            created_dt.toString("yyyy-MM-dd hh:mm:ss ap") if created_dt.isValid() else self.tr("N/A")))

        # --- Completed On (Read-only, if completed) ---
        finished_at_str = download_data.get('finished_at')
        if finished_at_str:
            finished_dt = QDateTime.fromString(finished_at_str, Qt.ISODate)
            if not finished_dt.isValid() and finished_at_str:
                finished_dt = QDateTime.fromString(finished_at_str, "yyyy-MM-dd HH:mm:ss")
            form_layout.addRow(self.tr("<b>Completed On:</b>"), QLabel(
                finished_dt.toString("yyyy-MM-dd hh:mm:ss ap") if finished_dt.isValid() else self.tr("N/A")))

        layout.addLayout(form_layout)
        layout.addStretch()

        # --- Buttons ---
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _browse_save_path(self):
        current_path_str = self.save_path_edit.text()
        current_path = Path(current_path_str)

        if current_path.is_file():
            dir_path = str(current_path.parent)
            file_name_suggestion = current_path.name
        elif current_path.is_dir() or not current_path_str:
            dir_path = current_path_str or str(Path.home() / "Downloads")
            file_name_suggestion = self.download_data.get("file_name", "download")
        else:
            dir_path = str(current_path.parent)
            file_name_suggestion = current_path.name

        if not Path(dir_path).exists():
            dir_path = str(Path.home() / "Downloads")

        target_path = str(Path(dir_path) / file_name_suggestion)

        new_path_str, _ = QFileDialog.getSaveFileName(self, self.tr("Change Save Location"), target_path)
        if new_path_str:
            self.save_path_edit.setText(new_path_str)
            self.file_name_label.setText(Path(new_path_str).name)

    def accept(self):
        new_path = self.save_path_edit.text()
        new_desc = self.description_edit.text()

        original_data = self.db.get_download_by_id(self.download_data["id"])
        if not original_data:
            QMessageBox.warning(self, self.tr("Error"),
                                self.tr("Could not find the original download data. It might have been deleted."))
            super().reject()
            return

        changed_fields = {}

        if new_path != original_data.get("save_path"):
            changed_fields["save_path"] = new_path
            changed_fields["file_name"] = Path(new_path).name

        if new_desc != original_data.get("description", ""):
            changed_fields["description"] = new_desc

        if changed_fields:
            try:
                set_clauses = []
                params = []
                for key, value in changed_fields.items():
                    set_clauses.append(f"{key} = ?")
                    params.append(value)

                if set_clauses:
                    params.append(self.download_data["id"])
                    query = f"UPDATE downloads SET {', '.join(set_clauses)} WHERE id = ?"
                    self.db.conn.execute(query, tuple(params))
                    self.db.conn.commit()

                    for key, value in changed_fields.items():
                        self.download_data[key] = value
            except Exception as e:
                QMessageBox.critical(self, self.tr("Database Error"),
                                     self.tr("Failed to update download properties:\n{0}").format(str(e)))
                print(f"Error updating database: {e}")
                return

        super().accept()

    def get_updated_data(self):
        return self.download_data


class QueueSettingsDialog(QDialog):
    def __init__(self, queue_id: int, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.queue_id = queue_id
        self.queue_data = self.db.get_queue_by_id(queue_id)

        if not self.queue_data:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Queue not found."))
            self.queue_data = {"id": -1, "name": self.tr("Error - Queue Not Found"), "days": [], "max_concurrent": 1,
                               "enabled": 0}
            self.setWindowTitle(self.tr("Error - Queue Not Found"))
        else:
            self.setWindowTitle(self.tr(f"Edit Queue - {self.queue_data['name']}"))

        self.resize(550, 450)
        self.downloads_in_queue = self.db.get_downloads_in_queue(self.queue_id) if self.queue_id != -1 else []
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        tabs = QTabWidget(self)

        general_tab = QWidget()
        general_layout = QFormLayout(general_tab)

        self.queue_name_edit = QLineEdit(self.queue_data.get("name", ""))
        general_layout.addRow(self.tr("Queue Name:"), self.queue_name_edit)

        self.max_concurrent_input = QSpinBox()
        self.max_concurrent_input.setMinimum(1)
        self.max_concurrent_input.setMaximum(99)
        self.max_concurrent_input.setValue(self.queue_data.get("max_concurrent", 1))
        general_layout.addRow(self.tr("Max Concurrent Downloads:"), self.max_concurrent_input)

        self.pause_between_input = QSpinBox()
        self.pause_between_input.setSuffix(self.tr(" seconds"))
        self.pause_between_input.setMinimum(0)
        self.pause_between_input.setMaximum(3600)
        self.pause_between_input.setValue(self.queue_data.get("pause_between", 0))
        general_layout.addRow(self.tr("Pause between downloads:"), self.pause_between_input)

        tabs.addTab(general_tab, self.tr("General"))

        schedule_tab = QWidget()
        schedule_layout_outer = QVBoxLayout(schedule_tab)

        self.enable_schedule_checkbox = QCheckBox(self.tr("Enable Schedule for this Queue"))
        self.enable_schedule_checkbox.setChecked(bool(self.queue_data.get("enabled", 0)))
        schedule_layout_outer.addWidget(self.enable_schedule_checkbox)

        self.schedule_fields_widget = QWidget()
        schedule_form_layout = QFormLayout(self.schedule_fields_widget)

        self.start_time_edit = QTimeEdit()
        self.start_time_edit.setDisplayFormat("HH:mm")
        start_qtime = QTime.fromString(self.queue_data.get("start_time", "00:00"), "HH:mm")
        self.start_time_edit.setTime(start_qtime if start_qtime.isValid() else QTime(0, 0))
        schedule_form_layout.addRow(self.tr("Start Time:"), self.start_time_edit)

        self.stop_time_checkbox = QCheckBox(self.tr("Enable Stop Time"))
        self.stop_time_edit = QTimeEdit()
        self.stop_time_edit.setDisplayFormat("HH:mm")

        stop_time_str = self.queue_data.get("stop_time")
        if stop_time_str:
            self.stop_time_checkbox.setChecked(True)
            stop_qtime = QTime.fromString(stop_time_str, "HH:mm")
            self.stop_time_edit.setTime(stop_qtime if stop_qtime.isValid() else QTime(23, 59))
        else:
            self.stop_time_checkbox.setChecked(False)
            self.stop_time_edit.setTime(QTime(23, 59))

        self.stop_time_edit.setEnabled(self.stop_time_checkbox.isChecked())
        self.stop_time_checkbox.toggled.connect(self.stop_time_edit.setEnabled)

        stop_time_layout = QHBoxLayout()
        stop_time_layout.addWidget(self.stop_time_checkbox)
        stop_time_layout.addWidget(self.stop_time_edit)
        schedule_form_layout.addRow(self.tr("Stop Time:"), stop_time_layout)

        days_group_box = QWidget()
        days_group_layout = QVBoxLayout(days_group_box)

        self.radio_one_time = QRadioButton(self.tr("Run once on this date:"))
        self.date_picker = QDateEdit(QDate.currentDate())
        self.date_picker.setCalendarPopup(True)

        self.radio_daily = QRadioButton(self.tr("Repeat on selected days:"))
        self.days_checkboxes_widget = QWidget()
        days_grid = QGridLayout(self.days_checkboxes_widget)
        self.day_checkboxes = {}
        day_keys = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        for i, day_key in enumerate(day_keys):
            chk = QCheckBox(self.tr(day_key))
            self.day_checkboxes[day_key] = chk
            days_grid.addWidget(chk, i // 4, i % 4)

        days_group_layout.addWidget(self.radio_one_time)
        days_group_layout.addWidget(self.date_picker)
        days_group_layout.addWidget(self.radio_daily)
        days_group_layout.addWidget(self.days_checkboxes_widget)

        schedule_form_layout.addRow(self.tr("Run On:"), days_group_box)

        self.radio_one_time.toggled.connect(lambda checked: self.date_picker.setEnabled(checked))
        self.radio_one_time.toggled.connect(lambda checked: self.days_checkboxes_widget.setDisabled(checked))
        self.radio_daily.toggled.connect(lambda checked: self.days_checkboxes_widget.setEnabled(checked))
        self.radio_daily.toggled.connect(lambda checked: self.date_picker.setDisabled(checked))

        self.schedule_fields_widget.setEnabled(self.enable_schedule_checkbox.isChecked())
        self.enable_schedule_checkbox.toggled.connect(self.schedule_fields_widget.setEnabled)

        queue_schedule_days = self.queue_data.get("days", [])
        if queue_schedule_days and re.fullmatch(r"\d{4}-\d{2}-\d{2}", queue_schedule_days[0]):
            self.radio_one_time.setChecked(True)
            self.date_picker.setDate(QDate.fromString(queue_schedule_days[0], "yyyy-MM-dd"))
        else:
            self.radio_daily.setChecked(True)
            if queue_schedule_days:
                for day_key in queue_schedule_days:
                    if day_key in self.day_checkboxes:
                        self.day_checkboxes[day_key].setChecked(True)

        schedule_layout_outer.addWidget(self.schedule_fields_widget)
        tabs.addTab(schedule_tab, self.tr("Schedule"))

        files_tab = QWidget()
        files_layout = QVBoxLayout(files_tab)

        self.files_table = QTableWidget(0, 2)
        self.files_table.setHorizontalHeaderLabels([self.tr("ID"), self.tr("File Name")])
        self.files_table.setColumnHidden(0, True)
        self.files_table.horizontalHeader().setStretchLastSection(True)
        self.files_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.files_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.files_table.setDragDropMode(QAbstractItemView.InternalMove)
        self.files_table.setDropIndicatorShown(True)
        self.files_table.setDragEnabled(True)

        sorted_downloads = sorted(self.downloads_in_queue, key=lambda x: x.get("queue_position", 0))
        for i, d_item in enumerate(sorted_downloads):
            self.files_table.insertRow(i)
            id_widget_item = QTableWidgetItem(str(d_item["id"]))
            self.files_table.setItem(i, 0, id_widget_item)
            self.files_table.setItem(i, 1, QTableWidgetItem(d_item["file_name"]))

        files_layout.addWidget(self.files_table)

        btn_layout = QHBoxLayout()
        self.up_btn = QPushButton(QIcon.fromTheme("go-up"), self.tr("Move Up"))
        self.down_btn = QPushButton(QIcon.fromTheme("go-down"), self.tr("Move Down"))
        self.remove_from_queue_btn = QPushButton(QIcon.fromTheme("list-remove"), self.tr("Remove from Queue"))

        btn_layout.addWidget(self.up_btn)
        btn_layout.addWidget(self.down_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.remove_from_queue_btn)
        files_layout.addLayout(btn_layout)

        self.up_btn.clicked.connect(self._move_selected_up_in_table)
        self.down_btn.clicked.connect(self._move_selected_down_in_table)
        self.remove_from_queue_btn.clicked.connect(self._remove_selected_from_queue_table)

        tabs.addTab(files_tab, self.tr("Files in Queue"))

        main_layout.addWidget(tabs)

        dialog_buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        dialog_buttons.accepted.connect(self.accept)
        dialog_buttons.rejected.connect(self.reject)
        main_layout.addWidget(dialog_buttons)

    def _move_selected_up_in_table(self):
        current_row = self.files_table.currentRow()
        if current_row > 0:
            self.files_table.insertRow(current_row - 1)
            for col in range(self.files_table.columnCount()):
                self.files_table.setItem(current_row - 1, col, self.files_table.takeItem(current_row + 1, col))
            self.files_table.removeRow(current_row + 1)
            self.files_table.selectRow(current_row - 1)

    def _move_selected_down_in_table(self):
        current_row = self.files_table.currentRow()
        if 0 <= current_row < self.files_table.rowCount() - 1:
            self.files_table.insertRow(current_row + 2)
            for col in range(self.files_table.columnCount()):
                self.files_table.setItem(current_row + 2, col, self.files_table.takeItem(current_row, col))
            self.files_table.removeRow(current_row)
            self.files_table.selectRow(current_row + 1)

    def _remove_selected_from_queue_table(self):
        current_row = self.files_table.currentRow()
        if current_row >= 0:
            download_id_item = self.files_table.item(current_row, 0)
            if download_id_item:
                self.files_table.removeRow(current_row)

    def get_queue_settings_data(self) -> dict:
        schedule_days_list = []
        if self.enable_schedule_checkbox.isChecked():
            if self.radio_one_time.isChecked():
                schedule_days_list.append(self.date_picker.date().toString("yyyy-MM-dd"))
            elif self.radio_daily.isChecked():
                for day_key, chk_box in self.day_checkboxes.items():
                    if chk_box.isChecked():
                        schedule_days_list.append(day_key)

        stop_time_val = self.stop_time_edit.time().toString(
            "HH:mm") if self.stop_time_checkbox.isChecked() and self.enable_schedule_checkbox.isChecked() else ""

        return {
            "id": self.queue_id,
            "name": self.queue_name_edit.text().strip(),
            "max_concurrent": self.max_concurrent_input.value(),
            "pause_between": self.pause_between_input.value(),
            "enabled": 1 if self.enable_schedule_checkbox.isChecked() else 0,
            "start_time": self.start_time_edit.time().toString(
                "HH:mm") if self.enable_schedule_checkbox.isChecked() else "",
            "stop_time": stop_time_val,
            "days": schedule_days_list
        }

    def get_file_order_and_removals(self) -> tuple[list[int], list[int]]:
        current_ids_in_table = []
        for row in range(self.files_table.rowCount()):
            id_item = self.files_table.item(row, 0)
            if id_item:
                current_ids_in_table.append(int(id_item.text()))

        original_ids = {d["id"] for d in self.downloads_in_queue}
        ids_to_remove = list(original_ids - set(current_ids_in_table))

        return current_ids_in_table, ids_to_remove


# --- BEGIN PIDM CLASS ---
class PIDM(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(self.tr("Python Internet Download Manager (PIDM)"))
        self.setWindowIcon(QIcon(get_asset_path("assets/icons/pidm_icon.ico")))
        self.resize(900, 550)
        self.app_version = "1.1.1"
        self.settings = SettingsManager()
        self.db = DatabaseManager()
        self.is_quitting = False

        if not self.db.get_queue_by_name("Main Queue"):
            self.db.add_queue("Main Queue", enabled=1, max_concurrent=self.settings.get("default_max_concurrent", 3))

        self.active_workers = {}
        self.download_id_to_row_map = {}

        self.category_filters = {
            self.tr("Compressed"): [".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso", ".cab", ".lz", ".lzma", ".ace", ".arj"],
            self.tr("Documents"): [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".rtf", ".odt", ".ods", ".csv", ".epub"],
            self.tr("Music"): [".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a", ".alac", ".aiff"],
            self.tr("Videos"): [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mpeg", ".mpg", ".3gp", ".ts", ".m4v"],
            self.tr("Programs"): [".exe", ".msi", ".dmg", ".pkg", ".sh", ".bat", ".apk", ".jar", ".bin", ".app", ".deb", ".rpm"]
        }

        self.init_ui()
        self.apply_theme(self.settings.get("theme", "dark"))
        self.init_category_tree()
        self.load_downloads_from_db()
        self.update_action_buttons_state()

        self.scheduler_thread = QueueSchedulerThread(self.db)
        self.scheduler_thread.start_queue_signal.connect(self.handle_start_queue_by_id)
        self.scheduler_thread.stop_queue_signal.connect(self.handle_stop_queue_by_id)
        self.scheduler_thread.start()

        self.setup_tray_icon()


    def init_ui(self):
        toolbar = QToolBar(self.tr("Main Toolbar"))
        toolbar.setIconSize(QSize(24, 24))
        toolbar.setToolButtonStyle(Qt.ToolButtonTextUnderIcon)
        self.addToolBar(toolbar)

        self.add_url_action = QAction(self.tr("&Add URL"), self)
        self.add_url_action.triggered.connect(self.handle_new_download_dialog)
        toolbar.addAction(self.add_url_action)

        self.resume_action = QAction(self.tr("&Resume"), self)
        self.resume_action.triggered.connect(self.handle_resume_selected)
        toolbar.addAction(self.resume_action)

        self.pause_action = QAction(self.tr("&Pause"), self)
        self.pause_action.triggered.connect(self.handle_pause_selected)
        toolbar.addAction(self.pause_action)

        toolbar.addSeparator()

        self.start_queue_action = QAction(self.tr("Start &Queue"), self)
        self.start_queue_action.triggered.connect(self.handle_start_selected_queue_from_toolbar)
        toolbar.addAction(self.start_queue_action)

        self.stop_queue_action = QAction(self.tr("S&top Queue"), self)
        self.stop_queue_action.triggered.connect(self.handle_stop_selected_queue_from_toolbar)
        toolbar.addAction(self.stop_queue_action)

        toolbar.addSeparator()

        self.remove_entry_action = QAction(self.tr("&Remove Entry"), self)
        self.remove_entry_action.triggered.connect(self.handle_delete_selected_entry)
        toolbar.addAction(self.remove_entry_action)

        self.cancel_dl_action = QAction(self.tr("&Cancel D/L"), self)
        self.cancel_dl_action.triggered.connect(self.handle_cancel_selected_download)
        toolbar.addAction(self.cancel_dl_action)

        self.stop_all_action = QAction(self.tr("Stop A&ll"), self)
        self.stop_all_action.triggered.connect(self.handle_stop_all_downloads)
        toolbar.addAction(self.stop_all_action)

        toolbar.addSeparator()

        self.report_bug_action = QAction(self.tr("Report Bugs"), self)
        self.report_bug_action.triggered.connect(
            lambda: QDesktopServices.openUrl(QUrl("https://github.com/saeedmasoudie/PIDM/issues"))
        )
        self.report_bug_action.setToolTip(self.tr("Report a bug or suggest a feature"))
        toolbar.addAction(self.report_bug_action)

        self.init_menubar()

        self.category_tree = QTreeWidget()
        self.category_tree.setHeaderHidden(True)
        self.category_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.category_tree.customContextMenuRequested.connect(self.show_queue_context_menu)
        self.category_tree.itemDoubleClicked.connect(self.handle_queue_double_click_edit)
        self.category_tree.currentItemChanged.connect(self.on_category_tree_selection_changed)

        self.download_table = QTableWidget()
        self.download_table.setColumnCount(9)
        self.download_table.setHorizontalHeaderLabels([
            self.tr("ID"), self.tr("File Name"), self.tr("Size"), self.tr("Status"),
            self.tr("Progress"), self.tr("Speed"), self.tr("Time Left"),
            self.tr("Date Added"), self.tr("Save Path")
        ])
        self.download_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.download_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.download_table.customContextMenuRequested.connect(self.show_download_context_menu)
        self.download_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.download_table.setSelectionMode(QTableWidget.SingleSelection)
        self.download_table.itemSelectionChanged.connect(self.update_action_buttons_state)
        self.download_table.itemDoubleClicked.connect(
            self.handle_download_double_click_properties)
        self.download_table.verticalHeader().setVisible(False)

        header = self.download_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)

        self.download_table.setColumnHidden(0, True)
        self.download_table.setColumnWidth(1, 250);
        self.download_table.setColumnWidth(2, 100)
        self.download_table.setColumnWidth(3, 110);
        self.download_table.setColumnWidth(4, 150)
        self.download_table.setColumnWidth(5, 100);
        self.download_table.setColumnWidth(6, 100)
        self.download_table.setColumnWidth(7, 140)

        main_splitter = QSplitter(Qt.Horizontal)
        main_splitter.addWidget(self.category_tree)
        main_splitter.addWidget(self.download_table)
        main_splitter.setSizes([250, 750])

        central_widget = QWidget()
        main_layout = QHBoxLayout(central_widget)
        main_layout.addWidget(main_splitter)
        self.setCentralWidget(central_widget)

        self.check_for_updates_background()

    def init_menubar(self):
        menubar = self.menuBar()

        tasks_menu = menubar.addMenu(self.tr("&Tasks"))
        tasks_menu.addAction(self.add_url_action)
        tasks_menu.addAction(self.resume_action)
        tasks_menu.addAction(self.pause_action)
        tasks_menu.addAction(self.cancel_dl_action)
        tasks_menu.addAction(self.stop_all_action)
        tasks_menu.addSeparator()
        tasks_menu.addAction(self.remove_entry_action)
        tasks_menu.addSeparator()
        tasks_menu.addAction(self.start_queue_action)
        tasks_menu.addAction(self.stop_queue_action)

        file_menu = menubar.addMenu(self.tr("&File"))
        exit_action = QAction(self.tr("E&xit"), self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        options_menu = menubar.addMenu(self.tr("&Options"))
        options_menu.setObjectName("optionsMenu")

        self.theme_menu = QMenu(self.tr("&Theme"), self)
        self.theme_menu.setObjectName("themeMenu")
        options_menu.addMenu(self.theme_menu)

        theme_group = QActionGroup(self)
        theme_group.setExclusive(True)

        dark_action = QAction(self.tr("Dark"), self, checkable=True)
        dark_action.triggered.connect(lambda: self.apply_theme("dark"))
        self.theme_menu.addAction(dark_action)
        theme_group.addAction(dark_action)

        light_action = QAction(self.tr("Light"), self, checkable=True)
        light_action.triggered.connect(lambda: self.apply_theme("light"))
        self.theme_menu.addAction(light_action)
        theme_group.addAction(light_action)

        current_theme = self.settings.get("theme", "dark")
        if current_theme == "dark":
            dark_action.setChecked(True)
        else:
            light_action.setChecked(True)

        # --- Language Menu ---
        self.language_menu = QMenu(self.tr("Language"), self)
        options_menu.addMenu(self.language_menu)

        language_group = QActionGroup(self)
        language_group.setExclusive(True)

        self.lang_en_action = QAction("English", self, checkable=True)
        self.lang_en_action.triggered.connect(lambda: self.change_language("en"))
        self.language_menu.addAction(self.lang_en_action)
        language_group.addAction(self.lang_en_action)

        self.lang_fa_action = QAction("فارسی", self, checkable=True)
        self.lang_fa_action.triggered.connect(lambda: self.change_language("fa"))
        self.language_menu.addAction(self.lang_fa_action)
        language_group.addAction(self.lang_fa_action)

        current_lang = self.settings.get("language", "en")
        if current_lang == "fa":
            self.lang_fa_action.setChecked(True)
        else:
            self.lang_en_action.setChecked(True)

        speed_limit_action = QAction(self.tr("Global Speed Limiter"), self)
        speed_limit_action.triggered.connect(self.show_global_speed_limit_dialog)
        options_menu.addAction(speed_limit_action)

        view_logs_action = QAction(self.tr("View Logs"), self)
        view_logs_action.triggered.connect(self.show_logs_window)
        options_menu.addAction(view_logs_action)

        help_menu = menubar.addMenu(self.tr("&Help"))
        about_action = QAction(self.tr("&About PIDM"), self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)

    def setup_tray_icon(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            QMessageBox.critical(self, "Error", "System tray is not available.")
            return

        self.tray_icon = QSystemTrayIcon(QIcon(get_asset_path("assets/icons/pidm_icon.ico")), self)
        self.tray_icon.setToolTip(self.tr("PIDM - Python Internet Download Manager"))

        self.tray_menu = QMenu()
        tray_menu_style = """
        QMenu {
            background-color: #2b2b2b;
            color: white;
            border: 1px solid #444;
            padding: 5px;
            border-radius: 5px;
        }
        QMenu::item:selected {
            background-color: #3c3c3c;
        }
        """
        self.tray_menu.setStyleSheet(tray_menu_style)

        self.tray_action_show = QAction(self.tr("Show PIDM"), self)
        self.tray_action_exit = QAction(self.tr("Exit"), self)
        self.tray_action_show.triggered.connect(self.show)
        self.tray_action_exit.triggered.connect(self.perform_exit)

        self.tray_menu.addAction(self.tray_action_show)
        self.tray_menu.addSeparator()
        self.tray_menu.addAction(self.tray_action_exit)
        self.tray_icon.setContextMenu(self.tray_menu)
        self.tray_icon.activated.connect(self._on_tray_icon_activated)
        self.tray_icon.show()

    def _on_tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.Trigger:
            self.showNormal()
            self.raise_()
            self.activateWindow()

    def init_category_tree(self):
        self.category_tree.clear()

        all_downloads_root = QTreeWidgetItem([self.tr("All Downloads")])
        all_downloads_root.setData(0, Qt.UserRole, "all_downloads_filter")
        all_downloads_root.setIcon(0, QIcon.fromTheme("folder-download"))
        self.category_tree.addTopLevelItem(all_downloads_root)

        status_root = QTreeWidgetItem(all_downloads_root, [self.tr("By Status")])
        status_root.setIcon(0, QIcon.fromTheme("document-properties", QIcon("icons/status.png")))
        for status_key, display_name_func in [
            (STATUS_DOWNLOADING, lambda: self.tr("Downloading")), (STATUS_QUEUED, lambda: self.tr("Queued")),
            (STATUS_PAUSED, lambda: self.tr("Paused")), (STATUS_COMPLETE, lambda: self.tr("Finished")),
            (STATUS_ERROR, lambda: self.tr("Error/Incomplete"))
        ]:
            item = QTreeWidgetItem(status_root, [display_name_func()])
            item.setData(0, Qt.UserRole, status_key)
            item.setIcon(0,
                         QIcon.fromTheme(f"task-{status_key}", QIcon(f"icons/{status_key}.png")))

        categories_root = QTreeWidgetItem(all_downloads_root, [self.tr("By Category")])
        categories_root.setIcon(0, QIcon.fromTheme("folder-open", QIcon("icons/categories.png")))
        for cat_key in self.category_filters.keys():
            item = QTreeWidgetItem(categories_root, [self.tr(cat_key)])
            item.setData(0, Qt.UserRole, cat_key)
            item.setIcon(0, QIcon.fromTheme(f"folder-{cat_key.lower()}", QIcon(f"icons/{cat_key.lower()}.png")))

        queues_root = QTreeWidgetItem([self.tr("Queues")])
        queues_root.setData(0, Qt.UserRole, "queues_root_node")
        queues_root.setIcon(0, QIcon.fromTheme("document-multiple", QIcon("icons/queues.png")))
        self.category_tree.addTopLevelItem(queues_root)

        all_db_queues = self.db.get_all_queues()
        for queue_rec in all_db_queues:
            item = QTreeWidgetItem(queues_root, [self.tr(queue_rec["name"])])
            item.setData(0, Qt.UserRole, queue_rec["id"])
            item.setIcon(0, QIcon.fromTheme("network-server", QIcon("icons/queue.png")))

        self.category_tree.expandItem(all_downloads_root)
        self.category_tree.expandItem(queues_root)
        self.category_tree.setCurrentItem(all_downloads_root)

    def change_language(self, lang_code: str):
        if lang_code not in ["en", "fa"]:
            return

        self.settings.set("language", lang_code)
        logger.debug(f"[Lang] {lang_code.upper()} selected as the language preference.")

        reply = QMessageBox.question(self, self.tr("Restart Required"),
                                     self.tr("Language change will take effect after restart. Restart now?"),
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            QCoreApplication.quit()
            QProcess.startDetached(sys.executable, sys.argv)

    def show_logs_window(self):
        log_path = get_user_data_dir() / "logs" / "pidm.log"
        dlg = LogViewerDialog(str(log_path), self)
        dlg.exec()

    def _add_download_to_table(self, download_data: dict, row_idx: int = -1):
        if row_idx == -1:
            row_idx = self.download_table.rowCount()
            self.download_table.insertRow(row_idx)

        self.download_id_to_row_map[download_data['id']] = row_idx

        id_item = QTableWidgetItem(str(download_data['id']))
        id_item.setData(Qt.UserRole, download_data['id'])
        self.download_table.setItem(row_idx, 0, id_item)

        self.download_table.setItem(row_idx, 1, QTableWidgetItem(download_data['file_name']))

        total_size_str = format_size(download_data.get('total_size', 0))
        self.download_table.setItem(row_idx, 2, QTableWidgetItem(total_size_str))

        status_key = download_data['status']
        status_text = tr_status(status_key)
        self.download_table.setItem(row_idx, 3, QTableWidgetItem(status_text))

        progress_bar = QProgressBar(self.download_table)
        progress_bar.setProperty("status", download_data['status'])
        progress_value = download_data.get('progress', 0)
        current_status = download_data['status']

        if current_status == STATUS_QUEUED: progress_value = 0
        progress_bar.setValue(progress_value if progress_value >= 0 else 0)

        if download_data.get('total_size', 0) == 0 and current_status in [STATUS_DOWNLOADING, STATUS_CONNECTING,
                                                                          STATUS_RESUMING]:
            progress_bar.setRange(0, 0)
            progress_bar.setFormat(self.tr("Downloading..."))
        else:
            progress_bar.setRange(0, 100)
            progress_bar.setFormat(f"{progress_value}%" if progress_value >= 0 else self.tr("N/A"))

        progress_bar.setTextVisible(True)
        self.download_table.setCellWidget(row_idx, 4, progress_bar)
        self.style().unpolish(progress_bar)
        self.style().polish(progress_bar)

        self.download_table.setItem(row_idx, 5, QTableWidgetItem(download_data.get('speed', '')))
        self.download_table.setItem(row_idx, 6, QTableWidgetItem(download_data.get('eta', '')))

        created_at_str = download_data.get('created_at', '')
        created_dt = QDateTime.fromString(created_at_str, Qt.ISODate)
        if not created_dt.isValid() and created_at_str:
            created_dt = QDateTime.fromString(created_at_str, "yyyy-MM-dd HH:mm:ss")
        self.download_table.setItem(row_idx, 7, QTableWidgetItem(
            created_dt.toString("yyyy-MM-dd hh:mm") if created_dt.isValid() else self.tr("N/A")))

        self.download_table.setItem(row_idx, 8, QTableWidgetItem(download_data.get('save_path', '')))
        return row_idx

    def load_downloads_from_db(self):
        self.download_table.setRowCount(0)
        self.download_id_to_row_map.clear()

        for dl_id in list(self.active_workers.keys()):
            worker_info = self.active_workers.pop(dl_id, None)
            if worker_info and worker_info["worker"].isRunning():
                worker_info["worker"].cancel()
                worker_info["worker"].wait(500)

        all_downloads = self.db.load_all()
        for item_data in all_downloads:
            status = item_data['status']
            if status in [STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING]:
                item_data['status'] = STATUS_PAUSED
                self.db.update_status(item_data['id'], STATUS_PAUSED)
            self._add_download_to_table(item_data)

        self.on_category_tree_selection_changed(self.category_tree.currentItem(), None)
        self.update_action_buttons_state()

    @Slot(QTreeWidgetItem, QTreeWidgetItem)
    def on_category_tree_selection_changed(self, current_item: QTreeWidgetItem, previous_item: QTreeWidgetItem):
        if not current_item:
            return

        filter_key = current_item.data(0, Qt.UserRole)
        self.download_table.setRowCount(0)
        self.download_id_to_row_map.clear()

        all_db_downloads = self.db.load_all()

        filtered_downloads_list = []

        if filter_key == "all_downloads_filter":
            filtered_downloads_list = all_db_downloads
        elif filter_key in self.category_filters:
            extensions = self.category_filters[filter_key]
            filtered_downloads_list = [
                d for d in all_db_downloads
                if any(d["file_name"].lower().endswith(ext) for ext in extensions)
            ]
        elif filter_key in [STATUS_DOWNLOADING, STATUS_QUEUED, STATUS_PAUSED, STATUS_COMPLETE,
                            STATUS_ERROR]:
            if filter_key == STATUS_ERROR:
                filtered_downloads_list = [d for d in all_db_downloads if
                                           d["status"] == STATUS_ERROR or d["status"] == STATUS_INCOMPLETE or "Error" in
                                           d["status"]]
            else:
                filtered_downloads_list = [d for d in all_db_downloads if d["status"] == filter_key]
        elif isinstance(filter_key, int):
            filtered_downloads_list = self.db.get_downloads_in_queue(filter_key)

        for download_item in filtered_downloads_list:
            if download_item['status'] in [STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING] and download_item[
                'id'] not in self.active_workers:
                download_item['status'] = STATUS_PAUSED
            self._add_download_to_table(download_item)

        self.update_action_buttons_state()

    def get_current_filter_item(self):
        return self.category_tree.currentItem()

    def refresh_download_table_with_current_filter(self):
        current_item = self.get_current_filter_item()
        if current_item:
            self.on_category_tree_selection_changed(current_item, current_item)

    @Slot()
    def _get_selected_download_id(self) -> int | None:
        selected_rows = self.download_table.selectionModel().selectedRows()
        if not selected_rows: return None

        id_item = self.download_table.item(selected_rows[0].row(), 0)
        return int(id_item.text()) if id_item and id_item.text().isdigit() else None

    @Slot()
    def update_action_buttons_state(self):
        selected_dl_id = self._get_selected_download_id()

        can_resume_dl, can_pause_dl, can_cancel_dl, can_remove_dl = False, False, False, False
        can_start_q, can_stop_q = False, False

        if selected_dl_id is not None:
            can_remove_dl = True
            dl_data = self.db.get_download_by_id(selected_dl_id)
            if dl_data:
                status = dl_data['status']
                is_active = selected_dl_id in self.active_workers

                resumable_statuses = [STATUS_QUEUED, STATUS_PAUSED, STATUS_ERROR, STATUS_INCOMPLETE]
                if status in resumable_statuses or "Error" in status:
                    can_resume_dl = True

                if is_active and status in [STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING]:
                    can_pause_dl = True

                if is_active or status == STATUS_QUEUED:
                    can_cancel_dl = True

        current_tree_item = self.category_tree.currentItem()
        if current_tree_item:
            item_data = current_tree_item.data(0, Qt.UserRole)
            if isinstance(item_data, int):
                selected_queue_id = item_data
                active_in_this_q = any(self.db.get_download_by_id(dl_id).get("queue_id") == selected_queue_id and info[
                    "worker"].isRunning() and not info["worker"]._is_paused for dl_id, info in
                                       self.active_workers.items())
                pending_in_this_q = any(
                    d["status"] not in [STATUS_COMPLETE, STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING] for d
                    in self.db.get_downloads_in_queue(selected_queue_id))
                can_start_q = pending_in_this_q or any(
                    d["status"] == STATUS_PAUSED for d in self.db.get_downloads_in_queue(selected_queue_id))
                can_stop_q = active_in_this_q

        self.resume_action.setEnabled(can_resume_dl)
        self.pause_action.setEnabled(can_pause_dl)
        self.cancel_dl_action.setEnabled(can_cancel_dl)
        self.remove_entry_action.setEnabled(can_remove_dl)
        self.start_queue_action.setEnabled(can_start_q)
        self.stop_queue_action.setEnabled(can_stop_q)

        self._refresh_toolbar_icons()

    @Slot()
    def handle_new_download_dialog(self):
        url_dialog = UrlInputDialog(self)
        if url_dialog.exec() != QDialog.Accepted:
            return

        url, metadata, auth_tuple = url_dialog.get_url_and_metadata()
        if not url:
            QMessageBox.warning(self, self.tr("Error"), self.tr("A valid URL is required."))
            return

        new_dl_dialog = NewDownloadDialog(self.settings, self.db, self)
        new_dl_dialog.set_initial_data(url, metadata, auth_tuple)
        new_dl_dialog.fetch_metadata_async(url, auth_tuple, custom_headers=None)

        if new_dl_dialog.exec() == QDialog.Accepted:
            added_dl_id, accepted_how = new_dl_dialog.get_final_download_data()

            if added_dl_id:
                db_payload = self.db.get_download_by_id(added_dl_id)
                if not db_payload:
                    logger.error(f"Failed to retrieve new download {added_dl_id} from DB.")
                    return

                self._add_download_to_table(db_payload)
                self.download_table.scrollToBottom()

                if db_payload.get("status") == STATUS_CONNECTING:
                    self._start_download_worker(added_dl_id)
                elif accepted_how == "now" and db_payload.get("queue_id") is not None:
                    self.try_auto_start_from_queue(db_payload["queue_id"])

            self.update_action_buttons_state()

    def _start_download_worker(self, download_id: int):
        """
        The single, robust entry point for starting or resuming any download.
        It fetches all necessary data directly from the database.
        """
        if download_id in self.active_workers:
            logger.info(f"Download ID {download_id} worker already active.")
            return

        dl_entry = self.db.get_download_by_id(download_id)
        if not dl_entry:
            logger.error(f"Cannot start worker: No database entry found for download ID {download_id}")
            self.update_action_buttons_state()
            return

        # Deserialize headers and auth from the database record
        custom_headers = None
        auth_tuple = None

        if dl_entry.get("custom_headers_json"):
            try:
                custom_headers = json.loads(dl_entry["custom_headers_json"])
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Failed to decode custom_headers_json for download {download_id}")

        if dl_entry.get("auth_json"):
            try:
                # The auth tuple needs to be converted back to a tuple from the list JSON creates
                auth_data = json.loads(dl_entry["auth_json"])
                if isinstance(auth_data, list) and len(auth_data) == 2:
                    auth_tuple = tuple(auth_data)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Failed to decode auth_json for download {download_id}")

        logger.info(f"Starting worker for ID {download_id}")
        worker = DownloadWorker(
            download_id, self.db, dl_entry["url"], dl_entry["save_path"],
            initial_downloaded_bytes=dl_entry["bytes_downloaded"],
            total_file_size=dl_entry["total_size"],
            auth=auth_tuple,
            custom_headers=custom_headers,
            parent=self
        )

        worker.status_changed.connect(self._on_worker_status_changed)
        worker.progress_updated.connect(self._on_worker_progress_updated)
        worker.finished_successfully.connect(self._on_worker_finished_successfully)
        worker.total_size_known.connect(self._on_worker_total_size_known)
        worker.original_queue_id = dl_entry.get("queue_id")

        self.active_workers[download_id] = {"worker": worker}
        worker.start()
        self.update_action_buttons_state()

    @Slot(str)
    def _on_worker_status_changed(self, status_message: str):
        worker = self.sender()
        if not isinstance(worker, DownloadWorker): return
        download_id = worker.download_id

        row = self.download_id_to_row_map.get(download_id)
        if row is not None and row < self.download_table.rowCount():
            self.download_table.setItem(row, 3, QTableWidgetItem(self.tr(status_message)))
            progress_bar = self.download_table.cellWidget(row, 4)
            if isinstance(progress_bar, QProgressBar):
                progress_bar.setProperty("status", status_message)
                self.style().unpolish(progress_bar)
                self.style().polish(progress_bar)

                if status_message == STATUS_COMPLETE:
                    progress_bar.setRange(0, 100)
                    progress_bar.setValue(100)
                    progress_bar.setFormat("100%")
                elif status_message == STATUS_PAUSED:
                    progress_bar.setFormat(self.tr("Paused"))
                elif "Error" in status_message or status_message == STATUS_INCOMPLETE:
                    progress_bar.setFormat(self.tr("Error"))
                elif status_message == STATUS_QUEUED:
                    progress_bar.setFormat(self.tr("Queued"))
                    progress_bar.setValue(0)

        if status_message in [STATUS_COMPLETE, STATUS_ERROR, STATUS_CANCELLED]:
            if download_id in self.active_workers:
                del self.active_workers[download_id]
        self.update_action_buttons_state()

    def check_for_updates_background(self):
        self.update_thread = UpdateCheckThread(self.app_version, self)
        self.update_thread.update_available.connect(self._show_update_dialog)
        self.update_thread.start()

    @Slot(int, str, str, int, int)
    def _on_worker_progress_updated(self, percent: int, speed: str, eta: str, downloaded_bytes: int, total_bytes: int):
        worker = self.sender()
        if not isinstance(worker, DownloadWorker): return
        download_id = worker.download_id

        row = self.download_id_to_row_map.get(download_id)
        if row is not None and row < self.download_table.rowCount():
            progress_bar = self.download_table.cellWidget(row, 4)
            if isinstance(progress_bar, QProgressBar):
                if total_bytes == 0 and percent == -1:
                    progress_bar.setRange(0, 0);
                    progress_bar.setFormat(self.tr("Downloading..."))
                else:
                    progress_bar.setRange(0, 100);
                    progress_bar.setValue(max(0, percent))
                    progress_bar.setFormat(f"{max(0, percent)}%")

            self.download_table.setItem(row, 5, QTableWidgetItem(speed))
            self.download_table.setItem(row, 6, QTableWidgetItem(eta))

            current_size_item = self.download_table.item(row, 2)
            new_size_str = format_size(total_bytes)
            if total_bytes > 0 and current_size_item and current_size_item.text() != new_size_str:
                current_size_item.setText(new_size_str)

    @Slot(str, str)
    def _show_update_dialog(self, new_version: str, url: str):
        reply = QMessageBox.information(
            self,
            self.tr("Update Available"),
            self.tr(f"A new version {new_version} is available!\nWould you like to open the release page?"),
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            QDesktopServices.openUrl(QUrl(url))

    @Slot(int)
    def _on_worker_total_size_known(self, total_size_val: int):
        worker = self.sender()
        if not isinstance(worker, DownloadWorker): return
        download_id = worker.download_id

        row = self.download_id_to_row_map.get(download_id)
        if row is not None and row < self.download_table.rowCount():
            self.download_table.setItem(row, 2, QTableWidgetItem(format_size(total_size_val)))

    @Slot(str)
    def _on_worker_finished_successfully(self, save_path_str: str):
        worker = self.sender()
        if not isinstance(worker, DownloadWorker):
            return
        download_id = worker.download_id

        row = self.download_id_to_row_map.get(download_id)
        if row is not None and row < self.download_table.rowCount():
            self.download_table.setItem(row, 3, QTableWidgetItem(self.tr(STATUS_COMPLETE)))
            progress_bar = self.download_table.cellWidget(row, 4)
            if isinstance(progress_bar, QProgressBar):
                progress_bar.setProperty("status", STATUS_COMPLETE)
                self.style().unpolish(progress_bar)
                self.style().polish(progress_bar)
                progress_bar.setRange(0, 100)
                progress_bar.setValue(100)
                progress_bar.setFormat("100%")

        if download_id in self.active_workers:
            del self.active_workers[download_id]

        download_data = self.db.get_download_by_id(download_id)
        original_queue_id = getattr(worker, 'original_queue_id', None)

        if original_queue_id:
            logger.debug(f"[AutoStart] Attempting to continue queue ID {original_queue_id}")
            self.try_auto_start_from_queue(original_queue_id)
            self.on_category_tree_selection_changed(self.category_tree.currentItem(), None)
        else:
            QMessageBox.information(self, self.tr("Download Complete"),
                                    self.tr(f"File '{Path(save_path_str).name}' downloaded successfully."))

        self.update_action_buttons_state()

    def try_auto_start_from_queue(self, queue_id: int):
        queue_settings = self.db.get_queue_by_id(queue_id)
        if not queue_settings or not queue_settings.get("enabled", True):
            return

        max_concurrent = queue_settings.get("max_concurrent", 1)

        active_in_this_queue = sum(
            1 for wid, info in self.active_workers.items()
            if self.db.get_download_by_id(wid) and self.db.get_download_by_id(wid).get("queue_id") == queue_id and not
            info["worker"]._is_paused
        )

        resumable_statuses = [STATUS_QUEUED, STATUS_PAUSED, STATUS_ERROR, STATUS_INCOMPLETE]
        pending_downloads = [d for d in self.db.get_downloads_in_queue(queue_id) if
                             d['status'] in resumable_statuses or "Error" in d['status']]

        for dl_item in pending_downloads:
            if active_in_this_queue >= max_concurrent:
                break

            dl_id = dl_item["id"]

            if dl_id in self.active_workers:
                worker = self.active_workers[dl_id]["worker"]
                if worker._is_paused:
                    logger.debug(f"[AutoStart] Resuming paused worker ID {dl_id} from queue.")
                    worker.resume()
                    active_in_this_queue += 1
                continue

            if dl_item["status"] in resumable_statuses or "Error" in dl_item["status"]:
                logger.debug(f"[AutoStart] Starting new worker for ID {dl_id} from queue.")

                self._start_download_worker(dl_id)

                active_in_this_queue += 1

    @Slot()
    def handle_resume_selected(self):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return
        self.handle_resume_download_id_logic(dl_id)

    def handle_resume_download_id_logic(self, download_id: int):
        worker_info = self.active_workers.get(download_id)
        if worker_info and worker_info["worker"]._is_paused:
            worker_info["worker"].resume()
        elif not worker_info:
            dl_data = self.db.get_download_by_id(download_id)
            if not dl_data: return

            resumable_statuses = [STATUS_QUEUED, STATUS_PAUSED, STATUS_ERROR, STATUS_INCOMPLETE]
            if dl_data['status'] in resumable_statuses or "Error" in dl_data['status']:
                self._start_download_worker(download_id)
            else:
                QMessageBox.information(self, self.tr("Cannot Resume"),
                                        self.tr(f"Download is in status '{dl_data['status']}' and cannot be resumed."))

        self.update_action_buttons_state()

    @Slot()
    def handle_redownload_selected(self):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return

        dl_data = self.db.get_download_by_id(dl_id)
        if not dl_data: return

        reply = QMessageBox.question(
            self,
            self.tr("Confirm Redownload"),
            self.tr(
                "This will delete the existing partial file and restart the download from the beginning. Are you sure?"),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.No:
            return

        try:
            Path(dl_data["save_path"]).unlink(missing_ok=True)
            logger.info(f"Partial file for download {dl_id} deleted for redownload.")
        except OSError as e:
            logger.error(f"Error deleting partial file for redownload {dl_id}: {e}")
            QMessageBox.warning(self, self.tr("File Error"),
                                self.tr("Could not delete the partial file. Please check permissions."))

        self.db.reset_download_for_redownload(dl_id)
        self.refresh_download_table_with_current_filter()
        self.handle_resume_download_id_logic(dl_id)

    @Slot()
    def handle_pause_selected(self):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return

        worker_info = self.active_workers.get(dl_id)
        if worker_info and worker_info["worker"].isRunning() and not worker_info["worker"]._is_paused:
            worker_info["worker"].pause()
        elif not worker_info:
            dl_data = self.db.get_download_by_id(dl_id)
            if dl_data and dl_data['status'] == STATUS_QUEUED:
                self.db.update_status(dl_id, STATUS_PAUSED)
                row = self.download_id_to_row_map.get(dl_id)
                if row is not None and row < self.download_table.rowCount():
                    self.download_table.setItem(row, 3, QTableWidgetItem(self.tr(STATUS_PAUSED)))
                    pb = self.download_table.cellWidget(row, 4)
                    if isinstance(pb, QProgressBar):
                        pb.setProperty("status", STATUS_PAUSED)
                        self.style().unpolish(pb);
                        self.style().polish(pb)
                        pb.setFormat(self.tr("Paused"))

        self.update_action_buttons_state()

    @Slot()
    def handle_cancel_selected_download(self):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return

        reply = QMessageBox.question(self, self.tr("Cancel Download"),
                                     self.tr(f"Cancel download ID {dl_id} and delete partial file?"),
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No: return

        worker_info = self.active_workers.pop(dl_id, None)
        if worker_info:
            worker_info["worker"].cancel()
        else:
            dl_data = self.db.get_download_by_id(dl_id)
            if dl_data and dl_data["status"] != STATUS_COMPLETE:
                try:
                    Path(dl_data["save_path"]).unlink(missing_ok=True)
                except OSError as e:
                    logger.error(f"Error deleting file for cancelled non-active DL {dl_id}: {e}")

        self.db.delete_download(dl_id)
        self._remove_row_from_table_by_id(dl_id)
        self.update_action_buttons_state()

    @Slot()
    def handle_delete_selected_entry(self):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return
        dl_data = self.db.get_download_by_id(dl_id)
        if not dl_data: return

        file_path = Path(dl_data["save_path"])
        msg_remove_list = self.tr(f"Remove '{dl_data['file_name']}' from the list?")
        msg_delete_file_disk = ""
        should_prompt_file_delete = False

        if file_path.exists():
            if dl_data["status"] == STATUS_COMPLETE:
                msg_delete_file_disk = self.tr("\nAlso delete the downloaded file from disk?")
            else:
                msg_delete_file_disk = self.tr("\nAlso delete the partial/incomplete file from disk?")
            should_prompt_file_delete = True

        full_prompt = msg_remove_list + msg_delete_file_disk
        buttons = QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel

        reply = QMessageBox.question(self, self.tr("Confirm Removal"), full_prompt, buttons, QMessageBox.NoButton)

        if reply == QMessageBox.Cancel: return

        delete_from_disk_confirmed = (reply == QMessageBox.Yes and should_prompt_file_delete)
        remove_from_list_confirmed = (reply == QMessageBox.Yes)

        if not remove_from_list_confirmed and reply == QMessageBox.No and should_prompt_file_delete:
            reply_list_only = QMessageBox.question(self, self.tr("Remove from List Only?"), msg_remove_list,
                                                   QMessageBox.Yes | QMessageBox.No)
            if reply_list_only != QMessageBox.Yes: return

        worker_info = self.active_workers.pop(dl_id, None)
        if worker_info: worker_info["worker"].cancel()

        if delete_from_disk_confirmed and file_path.exists():
            try:
                file_path.unlink()
            except OSError as e:
                QMessageBox.warning(self, self.tr("File Deletion Error"), str(e))

        self.db.delete_download(dl_id)
        self._remove_row_from_table_by_id(dl_id)
        self.update_action_buttons_state()

    def _remove_row_from_table_by_id(self, download_id: int):
        row = self.download_id_to_row_map.pop(download_id, None)
        if row is not None and row < self.download_table.rowCount():
            self.download_table.removeRow(row)
            new_map = {}
            for r_idx in range(self.download_table.rowCount()):
                id_item = self.download_table.item(r_idx, 0)
                if id_item and id_item.text().isdigit():
                    new_map[int(id_item.text())] = r_idx
            self.download_id_to_row_map = new_map

    @Slot(QPoint)
    def show_queue_context_menu(self, position: QPoint):
        item = self.category_tree.itemAt(position)
        if not item: return

        item_data = item.data(0, Qt.UserRole)
        is_queues_root_node = (item_data == "queues_root_node")
        is_specific_queue = isinstance(item_data, int)

        menu = QMenu(self)
        if is_queues_root_node:
            menu.addAction(self.tr("Add New Queue..."), self.handle_add_new_queue_dialog)
        elif is_specific_queue:
            queue_id = item_data
            queue_downloads = self.db.get_downloads_in_queue(queue_id)
            has_resumable = any(
                d["status"] in [STATUS_QUEUED, STATUS_PAUSED, STATUS_ERROR, STATUS_INCOMPLETE]
                for d in queue_downloads
            )
            has_active = any(
                d["status"] == STATUS_DOWNLOADING
                for d in queue_downloads
            )

            start_action = menu.addAction(self.tr("Start This Queue"), lambda: self.handle_start_queue_by_id(queue_id))
            start_action.setEnabled(has_resumable)

            stop_action = menu.addAction(self.tr("Stop This Queue"), lambda: self.handle_stop_queue_by_id(queue_id))
            stop_action.setEnabled(has_active)

            menu.addAction(self.tr("Edit Queue Settings..."), lambda: self.handle_edit_queue_dialog(queue_id))
            menu.addSeparator()

            main_queue_record = self.db.get_queue_by_name("Main Queue")
            is_main_queue = main_queue_record and main_queue_record["id"] == queue_id

            del_action = menu.addAction(self.tr("Delete This Queue"), lambda: self.handle_delete_queue_action(queue_id))
            if is_main_queue:
                del_action.setEnabled(False)

        if menu.actions():
            menu.exec(self.category_tree.viewport().mapToGlobal(position))

    def handle_add_new_queue_dialog(self):
        name, ok = QInputDialog.getText(self, self.tr("Add New Queue"), self.tr("Enter name for the new queue:"))
        if ok and name.strip():
            new_queue_name = name.strip()
            if self.db.get_queue_by_name(new_queue_name):
                QMessageBox.warning(self, self.tr("Queue Exists"),
                                    self.tr(f"A queue named '{new_queue_name}' already exists."))
                return
            self.db.add_queue(new_queue_name, enabled=1, max_concurrent=self.settings.get("default_max_concurrent", 3))
            self.init_category_tree()

    @Slot(int)
    def handle_start_queue_by_id(self, queue_id: int):
        queue_data = self.db.get_queue_by_id(queue_id)
        if not queue_data:
            logger.error(f"[QueueStart] Error: Queue ID {queue_id} not found.")
            return

        logger.debug(f"[QueueStart] Attempting to start queue ID: {queue_id} (Name: {queue_data['name']})")
        self.try_auto_start_from_queue(queue_id)
        self.update_action_buttons_state()

    @Slot(int)
    def handle_stop_queue_by_id(self, queue_id: int):
        queue_data = self.db.get_queue_by_id(queue_id)
        if not queue_data:
            logger.error(f"[QueueStop] Error: Queue ID {queue_id} not found.")
            return

        logger.debug(f"[QueueStop] Stopping all active downloads in queue ID: {queue_id} (Name: {queue_data['name']})")
        for dl_id_iter, worker_info_iter in list(self.active_workers.items()):
            dl_entry = self.db.get_download_by_id(dl_id_iter)
            if dl_entry and dl_entry.get("queue_id") == queue_id:
                if worker_info_iter["worker"].isRunning() and not worker_info_iter["worker"]._is_paused:
                    worker_info_iter["worker"].pause()
        self.update_action_buttons_state()

    def handle_start_selected_queue_from_toolbar(self):
        current_tree_item = self.category_tree.currentItem()
        if current_tree_item:
            item_data = current_tree_item.data(0, Qt.UserRole)
            if isinstance(item_data, int):
                self.handle_start_queue_by_id(item_data)

    def handle_stop_selected_queue_from_toolbar(self):
        current_tree_item = self.category_tree.currentItem()
        if current_tree_item:
            item_data = current_tree_item.data(0, Qt.UserRole)
            if isinstance(item_data, int):
                self.handle_stop_queue_by_id(item_data)

    def handle_edit_queue_dialog(self, queue_id: int):
        dialog = QueueSettingsDialog(queue_id, self.db, self)
        if dialog.exec() == QDialog.Accepted:
            settings_data = dialog.get_queue_settings_data()
            ordered_dl_ids, removed_dl_ids = dialog.get_file_order_and_removals()

            self.db.update_queue(
                queue_id, settings_data["name"], settings_data["start_time"], settings_data["stop_time"],
                settings_data["days"], settings_data["enabled"], settings_data["max_concurrent"],
                settings_data["pause_between"]
            )

            for new_pos, dl_id_to_update in enumerate(ordered_dl_ids):
                self.db.update_download_queue_position(dl_id_to_update, new_pos + 1)
                self.db.update_download_queue_id(dl_id_to_update, queue_id, new_pos + 1)

            for dl_id_to_remove in removed_dl_ids:
                self.db.update_download_queue_id(dl_id_to_remove, None, 0)

            self.init_category_tree()
            self.on_category_tree_selection_changed(self.category_tree.currentItem(), None)
            QMessageBox.information(self, self.tr("Queue Updated"),
                                    self.tr(f"Queue '{settings_data['name']}' settings saved."))

    def handle_delete_queue_action(self, queue_id: int):
        queue_to_delete = self.db.get_queue_by_id(queue_id)
        if not queue_to_delete:
            QMessageBox.warning(self, self.tr("Error"), self.tr("Queue not found."))
            return

        main_queue_record = self.db.get_queue_by_name("Main Queue")
        if main_queue_record and main_queue_record["id"] == queue_id:
            QMessageBox.warning(self, self.tr("Cannot Delete"), self.tr("The 'Main Queue' cannot be deleted."))
            return

        reply = QMessageBox.question(
            self, self.tr("Delete Queue"),
            self.tr(f"Are you sure you want to delete the queue '{queue_to_delete['name']}'?\n"
                    "Downloads in this queue will NOT be deleted but will be unassigned from any queue."),
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.db.delete_queue(queue_id)
            self.init_category_tree()
            self.on_category_tree_selection_changed(self.category_tree.currentItem(), None)

    @Slot(QTreeWidgetItem, int)
    def handle_queue_double_click_edit(self, item: QTreeWidgetItem, column: int):
        item_data = item.data(0, Qt.UserRole)
        if isinstance(item_data, int):
            self.handle_edit_queue_dialog(item_data)

    @Slot(QTableWidgetItem)
    def handle_download_double_click_properties(self, item: QTableWidgetItem):
        if item is None: return
        dl_id = self._get_selected_download_id()
        if dl_id is not None:
            self.show_download_properties_dialog(dl_id)

    @Slot(QPoint)
    def show_download_context_menu(self, pos: QPoint):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return

        dl_data = self.db.get_download_by_id(dl_id)
        if not dl_data: return

        menu = QMenu(self)

        menu.addAction(self.resume_action)
        menu.addAction(self.pause_action)
        menu.addSeparator()

        redownload_action = QAction(self.tr("Redownload"), self)
        redownload_action.triggered.connect(self.handle_redownload_selected)

        is_error_state = "Error" in dl_data['status'] or dl_data['status'] == STATUS_INCOMPLETE
        is_complete_and_file_missing = (
                    dl_data['status'] == STATUS_COMPLETE and not Path(dl_data['save_path']).exists())

        redownload_action.setEnabled(is_error_state or is_complete_and_file_missing)
        menu.addAction(redownload_action)
        menu.addSeparator()

        menu.addAction(self.cancel_dl_action)
        menu.addAction(self.remove_entry_action)
        menu.addSeparator()

        change_q_menu = menu.addMenu(self.tr("Assign to Queue"))
        queues = self.db.get_all_queues()
        action_none_q = change_q_menu.addAction(self.tr("(No Queue - Download Immediately)"))
        action_none_q.triggered.connect(
            lambda checked=False, q_id_to_set=None: self.assign_download_to_queue(dl_id, q_id_to_set))

        current_dl_queue_id = dl_data.get("queue_id")
        if current_dl_queue_id is None: action_none_q.setChecked(True)

        for q_rec in queues:
            action = change_q_menu.addAction(self.tr(q_rec["name"]))
            action.setCheckable(True)
            if current_dl_queue_id == q_rec["id"]: action.setChecked(True)
            action.triggered.connect(
                lambda checked=False, q_id_to_set=q_rec["id"]: self.assign_download_to_queue(dl_id, q_id_to_set))

        menu.addSeparator()
        menu.addAction(self.tr("Open File"), lambda: self.open_downloaded_file(dl_id)).setEnabled(
            dl_data["status"] == STATUS_COMPLETE and Path(dl_data["save_path"]).exists())
        menu.addAction(self.tr("Open Containing Folder"), lambda: self.open_download_folder(dl_id)).setEnabled(
            Path(dl_data["save_path"]).parent.exists())
        menu.addSeparator()
        menu.addAction(self.tr("Properties..."), lambda: self.show_download_properties_dialog(dl_id))

        menu.exec(self.download_table.viewport().mapToGlobal(pos))

    def assign_download_to_queue(self, download_id: int, queue_id: int | None):
        new_pos = 0
        if queue_id is not None:
            items_in_new_q = self.db.get_downloads_in_queue(queue_id)
            new_pos = max(d.get("queue_position", 0) for d in items_in_new_q) + 1 if items_in_new_q else 1

        self.db.update_download_queue_id(download_id, queue_id, new_pos)

        if queue_id is None:
            dl_data = self.db.get_download_by_id(download_id)
            if dl_data and dl_data["status"] in [STATUS_QUEUED, STATUS_PAUSED]:
                self.db.update_status(download_id, STATUS_CONNECTING)
                self.handle_resume_download_id_logic(download_id)

        self.on_category_tree_selection_changed(self.category_tree.currentItem(), None)
        self.update_action_buttons_state()

    def open_downloaded_file(self, download_id: int):
        dl_data = self.db.get_download_by_id(download_id)
        if dl_data and Path(dl_data["save_path"]).exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(dl_data["save_path"]))
        else:
            QMessageBox.warning(self, self.tr("File Not Found"),
                                self.tr("The downloaded file could not be found at the specified path."))

    def open_download_folder(self, download_id: int):
        dl_data = self.db.get_download_by_id(download_id)
        if dl_data and Path(dl_data["save_path"]).exists():
            folder_path = str(Path(dl_data["save_path"]).parent)
            QDesktopServices.openUrl(QUrl.fromLocalFile(folder_path))
        elif dl_data:
            folder_path = str(Path(dl_data["save_path"]).parent)
            if Path(folder_path).is_dir():
                QDesktopServices.openUrl(QUrl.fromLocalFile(folder_path))
            else:
                QMessageBox.warning(self, self.tr("Folder Not Found"),
                                    self.tr("The containing folder could not be found."))
        else:
            QMessageBox.warning(self, self.tr("Error"), self.tr("Download data not found."))

    def show_download_properties_dialog(self, download_id: int):
        dl_data = self.db.get_download_by_id(download_id)
        if dl_data:
            dialog = PropertiesDialog(dl_data, self.db, self)
            if dialog.exec() == QDialog.Accepted:
                self.on_category_tree_selection_changed(self.category_tree.currentItem(), None)
        else:
            QMessageBox.warning(self, self.tr("Error"), self.tr("Could not load download properties."))

    def handle_stop_all_downloads(self):
        for dl_id_iter, worker_info_iter in list(self.active_workers.items()):
            if worker_info_iter["worker"].isRunning() and not worker_info_iter["worker"]._is_paused:
                worker_info_iter["worker"].pause()
        self.update_action_buttons_state()

    def apply_theme(self, theme_name: str):
        qss_path = Path(__file__).parent / "assets" / "themes" / f"{theme_name}.qss"

        if not qss_path.exists():
            QMessageBox.warning(self, self.tr("Theme Not Found"),
                                self.tr(f"The theme file '{theme_name}.qss' was not found."))
            return

        try:
            with open(qss_path, "r", encoding="utf-8") as f:
                qss = f.read()
                self.setStyleSheet(qss)
                self.settings.set("theme", theme_name)
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error Loading Theme"),
                                 self.tr(f"Failed to apply theme '{theme_name}': {e}"))
            return

        if hasattr(self, "theme_menu"):
            for action in self.theme_menu.actions():
                clean_name = action.text().lower().replace("&", "")
                action.setChecked(clean_name == theme_name.lower())

        self._refresh_toolbar_icons()

    def _refresh_toolbar_icons(self):
        theme = self.settings.get("theme", "dark")
        base_color = QColor("white") if theme == "dark" else QColor("black")
        disabled_color = QColor("#888888")

        def icon(path, enabled):
            return colored_icon_from_svg(path, base_color if enabled else disabled_color)

        self.add_url_action.setIcon(icon(get_asset_path("assets/icons/plus.svg"), self.add_url_action.isEnabled()))
        self.resume_action.setIcon(icon(get_asset_path("assets/icons/player-play.svg"), self.resume_action.isEnabled()))
        self.pause_action.setIcon(icon(get_asset_path("assets/icons/player-pause.svg"), self.pause_action.isEnabled()))
        self.start_queue_action.setIcon(icon(get_asset_path("assets/icons/player-track-next.svg"), self.start_queue_action.isEnabled()))
        self.stop_queue_action.setIcon(icon(get_asset_path("assets/icons/copy-x.svg"), self.stop_queue_action.isEnabled()))
        self.remove_entry_action.setIcon(icon(get_asset_path("assets/icons/trash-x.svg"), self.remove_entry_action.isEnabled()))
        self.cancel_dl_action.setIcon(icon(get_asset_path("assets/icons/cancel.svg"), self.cancel_dl_action.isEnabled()))
        self.stop_all_action.setIcon(icon(get_asset_path("assets/icons/hand-stop.svg"), self.stop_all_action.isEnabled()))
        self.report_bug_action.setIcon(icon(get_asset_path("assets/icons/bug.svg"), self.report_bug_action.isEnabled()))

    def show_options_dialog(self):
        QMessageBox.information(self, self.tr("Options"), self.tr("Options dialog is not yet implemented."))

    def show_global_speed_limit_dialog(self):
        current_limit_bps = DownloadWorker.global_speed_limit_bps
        current_kbps = current_limit_bps // 1024 if current_limit_bps else 0

        limit_kbps, ok = QInputDialog.getInt(
            self, self.tr("Global Speed Limit"),
            self.tr("Set global download speed limit (KB/s):\n(0 or empty to disable)"),
            value=current_kbps, minValue=0, maxValue=100 * 1024
        )
        if ok:
            DownloadWorker.set_global_speed_limit(limit_kbps)
            self.settings.set("global_speed_limit_kbps", limit_kbps)
            msg = self.tr("Global speed limit disabled.") if limit_kbps == 0 else self.tr(
                f"Global speed limit set to {limit_kbps} KB/s.")
            QMessageBox.information(self, self.tr("Speed Limit Updated"), msg)

    def show_about_dialog(self):
        about_dialog = QDialog(self)
        about_dialog.setWindowTitle(self.tr("About PIDM"))
        about_dialog.setMinimumWidth(420)

        layout = QVBoxLayout(about_dialog)

        # Description
        description = QLabel(self.tr(
            f"<h3>Python Internet Download Manager (PIDM)</h3>"
            f"<p><b>Version:</b> {self.app_version}</p>"
            "<p>PIDM is an open-source, Python-based download manager.</p>"
            "<p>It supports features like:</p>"
            "<ul>"
            "<li>Queue-based download management</li>"
            "<li>Multi-language support</li>"
            "<li>Scheduling & speed limiting</li>"
            "<li>Pause/resume & persistent downloads</li>"
            "</ul>"
        ))
        description.setWordWrap(True)
        layout.addWidget(description)

        footer = QLabel(self.tr(
            "<hr><p align='center'>Created with ❤️ by "
            "<a href='https://saeedmasoudie.ir'>Saeed Masoudi</a></p>"
        ))
        footer.setOpenExternalLinks(True)
        footer.setAlignment(Qt.AlignCenter)
        layout.addWidget(footer)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        update_button = QPushButton(self.tr("Check for Updates"))
        button_box.addButton(update_button, QDialogButtonBox.ActionRole)
        button_box.accepted.connect(about_dialog.accept)

        update_button.clicked.connect(self.check_for_updates_manual)

        layout.addWidget(button_box)
        about_dialog.exec()

    def check_for_updates_manual(self):
        self.thread = UpdateCheckThread(self.app_version, self)
        self.thread.update_available.connect(self._show_update_dialog)
        self.thread.start()

    def perform_exit(self):
        active_workers_running = any(
            w_info["worker"].isRunning() and not w_info["worker"]._is_paused
            for w_info in self.active_workers.values()
        )

        if active_workers_running:
            reply = QMessageBox.question(self, self.tr("Exit Confirmation"),
                                         self.tr("Active downloads will be paused. Are you sure you want to exit?"),
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.No:
                return

            for dl_id_iter in list(self.active_workers.keys()):
                worker_info = self.active_workers.get(dl_id_iter)
                if worker_info and worker_info["worker"].isRunning() and not worker_info["worker"]._is_paused:
                    worker_info["worker"].pause()
                    worker_info["worker"].wait(500)

        if hasattr(self, 'scheduler_thread') and self.scheduler_thread.isRunning():
            self.scheduler_thread.stop()
            if not self.scheduler_thread.wait(2000):
                logger.warning("Scheduler thread did not stop gracefully, terminating.")
                self.scheduler_thread.terminate()

        self.db.close()
        self.settings.save()
        self.is_quitting = True
        QApplication.quit()

    def closeEvent(self, event):
        if self.is_quitting:
            event.accept()
            return

        event.ignore()
        self.hide()

        if self.tray_icon and self.tray_icon.isVisible():
            self.tray_icon.showMessage(
                self.tr("PIDM Still Running"),
                self.tr("PIDM is still running in the system tray."),
                QSystemTrayIcon.Information,
                3000
            )


def tr_status(key: str) -> str:
    translations = {
        STATUS_QUEUED: QCoreApplication.translate("Status", "Queued"),
        STATUS_DOWNLOADING: QCoreApplication.translate("Status", "Downloading"),
        STATUS_PAUSED: QCoreApplication.translate("Status", "Paused"),
        STATUS_CANCELLED: QCoreApplication.translate("Status", "Cancelled"),
        STATUS_COMPLETE: QCoreApplication.translate("Status", "Complete"),
        STATUS_ERROR: QCoreApplication.translate("Status", "Error"),
        STATUS_CONNECTING: QCoreApplication.translate("Status", "Connecting"),
        STATUS_RESUMING: QCoreApplication.translate("Status", "Resuming"),
        STATUS_INCOMPLETE: QCoreApplication.translate("Status", "Incomplete")
    }
    return translations.get(key.lower() if isinstance(key, str) else key, str(key).capitalize())


def format_size(bytes_size):
    if not isinstance(bytes_size, (int, float)) or bytes_size < 0: return "0 B" if bytes_size == 0 else "Unknown"
    if bytes_size == 0: return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while bytes_size >= 1024 and i < len(units) - 1:
        bytes_size /= 1024.0
        i += 1
    return f"{bytes_size:.2f} {units[i]}"


def get_system_icon_for_file(filename: str) -> QIcon:
    file_info = QFileInfo(filename)
    icon_provider = QFileIconProvider()
    icon = icon_provider.icon(file_info)

    if icon and not icon.isNull():
        return icon

    return QIcon(get_asset_path("assets/icons/file-x.svg"))


def load_custom_font(font_path="assets/fonts/Vazirmatn-Regular.ttf"):
    script_dir = Path(__file__).parent
    full_font_path = script_dir / font_path

    if not full_font_path.exists():
        logger.error(f"Font file not found: {full_font_path}")
        return None

    font_id = QFontDatabase.addApplicationFont(str(full_font_path))
    if font_id != -1:
        font_families = QFontDatabase.applicationFontFamilies(font_id)
        if font_families:
            return QFont(font_families[0], 9)
    else:
        logger.error(f"Failed to load font: {full_font_path}")
    return None


def colored_icon_from_svg(path: str, color: QColor, size: QSize = QSize(24, 24)) -> QIcon:
    renderer = QSvgRenderer(path)
    pixmap = QPixmap(size)
    pixmap.fill(Qt.transparent)

    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing)
    renderer.render(painter)
    painter.setCompositionMode(QPainter.CompositionMode_SourceIn)
    painter.fillRect(pixmap.rect(), color)
    painter.end()

    return QIcon(pixmap)


def setup_single_instance() -> bool:
    socket = QLocalSocket()
    socket.connectToServer("PIDM_INSTANCE_LOCK")
    if socket.waitForConnected(100):
        return False

    server = QLocalServer()
    try:
        QLocalServer.removeServer("PIDM_INSTANCE_LOCK")
        if not server.listen("PIDM_INSTANCE_LOCK"):
            return False
        return True
    except Exception as e:
        print(f"[SingleInstance] Error: {e}")
        return False


def show_download_dialog(download_info: dict, settings, db, parent=None):
    url = download_info.get("url")
    if not url:
        logger.debug("[show_download_dialog] Error: URL is missing in download_info.")
        return

    cookies_str = download_info.get("cookies")
    referrer_str = download_info.get("referrer")
    user_agent_str = download_info.get("userAgent")

    dialog = NewDownloadDialog(settings, db, parent)

    if parent and hasattr(parent, 'refresh_download_table_with_current_filter'):
        dialog.download_added.connect(parent.refresh_download_table_with_current_filter)

    filename = guess_filename_from_url(url)
    initial_metadata = {
        "filename": filename,
        "content_length": 0,
        "content_type": "application/octet-stream",
        "final_url": url
    }

    custom_headers = {}
    if cookies_str:
        custom_headers["Cookie"] = cookies_str
    if referrer_str:
        custom_headers["Referer"] = referrer_str
    if user_agent_str:
        custom_headers["User-Agent"] = user_agent_str

    logger.debug(f"[show_download_dialog] Processing URL: {url}")
    logger.debug(f"[show_download_dialog] Custom Headers for fetch/download: {custom_headers}")

    dialog.set_initial_data(url, initial_metadata, None)
    dialog.fetch_metadata_async(url, custom_headers=custom_headers)

    dialog.setWindowFlags(Qt.Dialog | Qt.WindowStaysOnTopHint)
    dialog.setAttribute(Qt.WA_ShowWithoutActivating, False)

    dialog.show()
    dialog.raise_()
    dialog.activateWindow()

    QApplication.processEvents()
    dialog.exec()


def apply_standalone_style(widget, settings):
    if widget.parent():
        return
    theme_name = settings.get("theme", "dark")
    theme_path = Path(__file__).parent / "assets" / "themes" / f"{theme_name}.qss"
    if theme_path.exists():
        widget.setStyleSheet(theme_path.read_text(encoding="utf-8"))

def bring_to_front():
    win.show()
    win.raise_()
    win.activateWindow()

def get_asset_path(relative_path: str) -> str:
    parent_dir = Path(__file__).parent
    asset_path = parent_dir / relative_path
    return str(asset_path)

def is_native_messaging():
    try:
        return sys.stdin and not sys.stdin.isatty()
    except Exception:
        return False

def guess_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).name
    return name if name else "downloaded_file"


if __name__ == "__main__":
    app = QApplication(sys.argv)
    settings = SettingsManager()
    custom_font = load_custom_font()
    app.setFont(custom_font if custom_font else QFont("Segoe UI", 9))

    current_lang = settings.get("language", QLocale.system().name().split('_')[0])
    translator = QTranslator()
    qt_translator = QTranslator()
    translation_dir = Path(__file__).parent / "translations"
    qt_translations_dir = QLibraryInfo.path(QLibraryInfo.TranslationsPath)

    if current_lang != "en" and translator.load(QLocale(current_lang), f"pidm_{current_lang}.qm", "_", str(translation_dir)):
        app.installTranslator(translator)
        if qt_translator.load(f"qtbase_{current_lang}", qt_translations_dir):
            app.installTranslator(qt_translator)
        app.setLayoutDirection(Qt.RightToLeft if current_lang in ("fa", "ar", "he") else Qt.LeftToRight)
    else:
        if translator.load(QLocale("en"), "pidm_en.qm", "_", str(translation_dir)):
            app.installTranslator(translator)
        app.setLayoutDirection(Qt.LeftToRight)

    DownloadWorker.set_global_speed_limit(settings.get("global_speed_limit_kbps", 0))

    win = PIDM()
    db = win.db

    if not setup_single_instance():
        sys.exit(0)

    # Start the local proxy server and hook to download dialog
    proxy = ProxyServer()
    proxy.link_received.connect(lambda url: show_download_dialog(url, settings, db, parent=win))

    win.show()
    sys.exit(app.exec())
