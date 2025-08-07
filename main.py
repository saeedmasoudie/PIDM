import collections
import logging
import mimetypes
import platform
import re
import signal
import subprocess
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse, unquote
import yt_dlp
import httpx
from PySide6.QtGui import QAction, QIcon, QActionGroup, QDesktopServices, QFontDatabase, QFont, QColor, QPixmap, \
    QPainter, QPen
from PySide6.QtNetwork import QLocalSocket, QLocalServer
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QToolBar, QTreeWidget, QTreeWidgetItem,
    QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget, QHBoxLayout,
    QHeaderView, QMenu, QPushButton, QLabel, QLineEdit, QDialog, QFileDialog, QCheckBox, QComboBox,
    QFormLayout, QDialogButtonBox, QMessageBox, QInputDialog, QProgressBar, QFileIconProvider, QTimeEdit, QGridLayout,
    QTabWidget, QRadioButton, QDateEdit, QAbstractItemView, QSpinBox, QSizePolicy, QSplitter, QTextEdit,
    QSystemTrayIcon, QListWidget, QListWidgetItem, QGraphicsBlurEffect
)
from PySide6.QtCore import Qt, QSize, QTranslator, QLocale, QLibraryInfo, QCoreApplication, QTimer, QDateTime, Slot, \
    QFileInfo, QUrl, QPoint, QTime, QDate, QProcess, QThread, Signal, QObject, QRect, QRectF
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
STATUS_YTDLP = "streaming"

APP_RESTART_CODE = 1000
logger = setup_logger()


class DuplicateDownloadError(Exception):
    """Custom exception raised when trying to add a download that already exists."""
    pass


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
            is_stream INTEGER DEFAULT 0,
            selected_format_id TEXT DEFAULT NULL,
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
            if 'is_stream' not in columns:
                logger.info("Adding 'is_stream' column to 'downloads' table.")
                cursor.execute("ALTER TABLE downloads ADD COLUMN is_stream INTEGER DEFAULT 0")
            if 'selected_format_id' not in columns:
                logger.info("Adding 'selected_format_id' column to 'downloads' table.")
                cursor.execute("ALTER TABLE downloads ADD COLUMN selected_format_id TEXT DEFAULT NULL")
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
            is_stream_value = 1 if data.get("is_stream", False) else 0

            custom_headers_json = json.dumps(custom_headers_dict) if custom_headers_dict else None
            auth_json = json.dumps(auth_tuple) if auth_tuple else None

            cur.execute("""
                        INSERT INTO downloads (
                            url, file_name, save_path, description, status,
                            progress, speed, eta, queue_id, queue_position,
                            created_at, bytes_downloaded, total_size, referrer,
                            custom_headers_json, auth_json, is_stream, selected_format_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                data["url"], data["file_name"], data["save_path"],
                data.get("description", ""), data.get("status", "queued"),
                data.get("progress", 0), data.get("speed", ""), data.get("eta", ""),
                data.get("queue_id"), data.get("queue_position", 0),
                data.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                data.get("bytes_downloaded", 0), data.get("total_size", 0),
                data.get("referrer"),
                custom_headers_json, auth_json, is_stream_value,
                data.get("selected_format_id")
            ))
            self.conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            logger.warning(
                f"IntegrityError (Duplicate): Download with URL {data['url']} and path {data['save_path']} already exists.")
            raise DuplicateDownloadError("Duplicate download detected.")
        except sqlite3.Error as e:
            logger.error(f"SQLite error in add_download: {e}")
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
                    elif isinstance(days_raw, list):  # Already a list, use as is
                        q["days"] = days_raw
                    else:  # Unexpected type
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
            self.conn.execute("UPDATE downloads SET queue_id = NULL, queue_position = 0 WHERE queue_id = ?",
                              (queue_id,))
            self.conn.execute("DELETE FROM queues WHERE id = ?", (queue_id,))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite error in delete_queue for Queue ID {queue_id}: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info(f"Database connection to {self.db_path_str} closed.")


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
    def __init__(self, file_path: Path):
        self.resume_file_path = Path(str(file_path) + ".resume")

    def write(self, total_size: int, url: str, segments: list):
        try:
            data = {
                "total_size": total_size,
                "url": url,
                "timestamp": datetime.now().isoformat(),
                "segments": segments
            }
            with open(self.resume_file_path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Error writing resume file {self.resume_file_path}: {e}")

    def read(self) -> dict:
        try:
            if self.resume_file_path.exists():
                with open(self.resume_file_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error reading resume file {self.resume_file_path}: {e}")
        return {}

    def delete(self):
        try:
            if self.resume_file_path.exists():
                self.resume_file_path.unlink()
        except Exception as e:
            logger.error(f"Error deleting resume file {self.resume_file_path}: {e}")


class DownloadWorker(QThread):
    progress_updated = Signal(int, str, str, float, float)
    status_changed = Signal(str)
    finished_successfully = Signal(str)
    total_size_known = Signal(int)
    request_file_deletion = Signal(str, str)
    global_speed_limit_bps = None

    NUM_CONNECTIONS = 5

    def __init__(self, download_id: int, db_manager, url: str, save_path: str,
                 initial_downloaded_bytes: int = 0, total_file_size: int = 0,
                 auth=None, custom_headers: dict = None, is_stream: bool = False, selected_format_id: str = None,
                 parent=None):
        super().__init__(parent)
        self.download_id = download_id
        self.db = db_manager
        self.url = url
        self.save_path = Path(save_path)
        self.auth = auth
        self.custom_headers = custom_headers if custom_headers is not None else {}
        self.is_stream = is_stream
        self.selected_format_id = selected_format_id
        self._last_db_write_time = time.time()
        self.resume_helper = ResumeFileHandler(self.save_path)

        self._speed_samples = collections.deque(maxlen=40)
        self._last_speed_sample_time = 0.0
        self._speed_sample_interval_ms = 500

        self._total_size = total_file_size
        self._downloaded_bytes = initial_downloaded_bytes
        self._is_paused = False
        self._is_cancelled = False
        self._is_running = False
        self._db_write_interval = 2.0
        self._min_bytes_for_db_write = 1024 * 1024 * 5
        self._last_db_write_bytes = initial_downloaded_bytes

        self._last_timestamp = time.time()
        self._last_downloaded_bytes_for_speed_calc = initial_downloaded_bytes
        self._speed_bps = 0
        self._eta_seconds = float('inf')
        self._last_timestamp_for_limit = time.time()
        self.client = None

        self._segments = []
        self._file_write_lock = threading.Lock()
        self._segment_threads = []

        self._pause_event = threading.Event()
        self._pause_event.set()

    def _kill_process_tree(self, pid):
        logger.info(f"Attempting to kill process tree with PID: {pid}")
        try:
            if sys.platform == "win32":
                subprocess.run(
                    ['taskkill', '/F', '/T', '/PID', str(pid)],
                    check=True, capture_output=True
                )
            else:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            logger.info(f"Successfully sent kill signal to process tree {pid}.")
        except (subprocess.CalledProcessError, ProcessLookupError, PermissionError) as e:
            logger.error(f"Failed to kill process tree {pid}: {e}")

    @classmethod
    def set_global_speed_limit(cls, limit_kbps: int):
        if limit_kbps <= 0:
            cls.global_speed_limit_bps = None
        else:
            cls.global_speed_limit_bps = limit_kbps * 1024

    def pause(self):
        if self.is_stream:
            logger.info(f"Pausing a stream is not supported. Cancelling download {self.download_id}.")
            self.cancel()
            return

        if self._is_running and not self._is_paused:
            self._is_paused = True
            self._pause_event.clear()
            self.status_changed.emit(STATUS_PAUSED)
            progress_percent = int((self._downloaded_bytes / self._total_size) * 100) if self._total_size > 0 else 0
            self.db.update_status_and_progress_on_pause(
                self.download_id, STATUS_PAUSED, progress_percent, self._downloaded_bytes
            )
            logger.info(f"Download {self.download_id} paused.")

    def resume(self):
        if self.is_stream:
            # Stream resuming is handled by PIDM class (re-download).
            logger.info(f"Download {self.download_id} is a stream, traditional resume not supported.")
            return

        if self._is_running and self._is_paused:
            self._is_paused = False
            self._pause_event.set()
            self._last_timestamp = time.time()
            self._last_downloaded_bytes_for_speed_calc = self._downloaded_bytes
            self._last_timestamp_for_limit = time.time()
            self.status_changed.emit(STATUS_RESUMING)
            self.db.update_status(self.download_id, STATUS_DOWNLOADING)
            logger.info(f"Download {self.download_id} resumed.")

    def cancel(self):
        if self._is_cancelled:
            return

        logger.info(f"Download {self.download_id}: Cancellation requested.")
        self._is_cancelled = True
        self._pause_event.set()

        if hasattr(self, '_yt_dlp_process') and self._yt_dlp_process:
            if self._yt_dlp_process.poll() is None:
                self._kill_process_tree(self._yt_dlp_process.pid)
            else:
                logger.info("Cancel called, but yt-dlp process was already finished.")

        for segment_thread in self._segment_threads:
            if segment_thread.is_alive():
                pass

        # Give some time for threads to react to cancellation
        self.msleep(100)

        # Join threads here to ensure they complete their cleanup before DownloadWorker exits
        for thread in self._segment_threads:
            if thread.is_alive():
                logger.debug(f"Download {self.download_id}: Waiting for segment thread to join on cancel.")
                thread.join(timeout=1.0)
                if thread.is_alive():
                    logger.warning(
                        f"Download {self.download_id}: Segment thread did not join on cancel, possibly stuck.")

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
        if download_entry and self.save_path.exists():
            self.request_file_deletion.emit(str(self.save_path), download_entry['file_name'])

        self.resume_helper.delete()

        self.db.update_status(self.download_id, STATUS_CANCELLED)
        self.status_changed.emit(STATUS_CANCELLED)

    def _calculate_speed_and_eta(self):
        if self._is_paused:
            return "0.00 KB/s", "Paused"

        current_speed_bps = 0
        eta_str = "N/A"

        if len(self._speed_samples) >= 2:
            oldest_time, oldest_bytes = self._speed_samples[0]
            newest_time, newest_bytes = self._speed_samples[-1]

            time_diff = newest_time - oldest_time
            bytes_diff = newest_bytes - oldest_bytes

            if time_diff > 0:
                current_speed_bps = bytes_diff / time_diff

        # Format speed string
        speed_kbps = current_speed_bps / 1024
        if speed_kbps >= 1024:
            speed_str = f"{speed_kbps / 1024:.2f} MB/s"
        elif speed_kbps > 0:
            speed_str = f"{speed_kbps:.2f} KB/s"
        else:
            speed_str = "0.00 KB/s"

        # Calculate ETA based on the averaged speed
        remaining_bytes = self._total_size - self._downloaded_bytes

        if current_speed_bps > 0 and remaining_bytes > 0:
            remaining_seconds = remaining_bytes / current_speed_bps
            eta_minutes = int(remaining_seconds // 60)
            eta_seconds = int(remaining_seconds % 60)
            eta_hours = int(eta_minutes // 60)
            eta_minutes %= 60

            if eta_hours > 0:
                eta_str = f"{eta_hours}h {eta_minutes}m {eta_seconds}s"
            elif eta_minutes > 0:
                eta_str = f"{eta_minutes}m {eta_seconds}s"
            else:
                eta_str = f"{eta_seconds}s"
        elif 0 < self._total_size <= self._downloaded_bytes:
            eta_str = "Complete"
        elif self._total_size == 0 and self._downloaded_bytes > 0:
            eta_str = "Streaming"
        elif self._total_size == 0:
            eta_str = "Unknown"

        return speed_str, eta_str

    def _download_segment(self, segment_index: int, start_byte: int, end_byte: int):
        """
        Downloads a specific segment of the file.
        This method will be run in a separate thread.
        """
        current_segment_downloaded_bytes = 0
        segment_start_offset = start_byte

        with self._file_write_lock:
            self._segments[segment_index]['downloaded_bytes'] = current_segment_downloaded_bytes
            self._segments[segment_index]['status'] = STATUS_CONNECTING

        logger.debug(f"Segment {segment_index}: Starting download from {start_byte} to {end_byte}")

        # The httpx client is now passed through self.client from the main worker thread
        if not self.client:
            logger.error(
                f"Segment {segment_index}: httpx.Client not initialized in DownloadWorker.run(). Cannot proceed.")
            with self._file_write_lock:
                self._segments[segment_index]['status'] = "Error (Client Not Init)"
            self._is_cancelled = True
            return

        try:
            headers_for_range = self.client.headers.copy()  # Use client's base headers
            request_start_byte = segment_start_offset + current_segment_downloaded_bytes
            headers_for_range["Range"] = f"bytes={request_start_byte}-{end_byte}"

            # Use the existing self.client to stream the response
            with self.client.stream("GET", self.url, headers=headers_for_range) as response:
                response.raise_for_status()

                # Basic check if server ignored range request and sent full file
                if response.status_code == 200 and "Content-Range" not in response.headers:
                    logger.warning(
                        f"Segment {segment_index}: Server returned 200 OK without Content-Range for {self.url}. This segment might get the whole file.")
                elif response.status_code == 206 and "Content-Range" not in response.headers:
                    logger.warning(
                        f"Segment {segment_index}: Server returned 206 Partial Content but no Content-Range for {self.url}. Proceeding but may indicate server issue.")

                with self._file_write_lock:
                    self._segments[segment_index]['status'] = STATUS_DOWNLOADING

                # Handle case where server sends full file when only partial was requested
                # This can happen if server doesn't support range requests or ignores If-Range/Range header
                if response.status_code == 200 and request_start_byte > start_byte:
                    logger.warning(
                        f"Segment {segment_index}: Server returned 200 OK when range was requested. Adjusting offset to match requested start_byte ({request_start_byte}). This might mean discarding initial initial bytes received from stream.")
                    pass

                for chunk in response.iter_bytes(chunk_size=1024 * 512):  # 512KB chunks
                    if self._is_cancelled:
                        logger.info(f"Segment {segment_index}: Cancelled.")
                        break

                    if not self._pause_event.is_set():
                        logger.debug(f"Segment {segment_index}: Paused, waiting for signal.")
                    self._pause_event.wait()

                    if self._is_cancelled:
                        logger.info(f"Segment {segment_index}: Cancelled after pause.")
                        break

                    with self._file_write_lock:
                        try:
                            # Write chunk to the correct position in the file
                            with open(self.save_path, "rb+") as file:
                                file.seek(segment_start_offset + current_segment_downloaded_bytes)
                                file.write(chunk)
                        except IOError as e:
                            logger.error(f"Segment {segment_index}: File write error: {e}")
                            self._is_cancelled = True
                            break

                    current_segment_downloaded_bytes += len(chunk)
                    with self._file_write_lock:
                        self._segments[segment_index]['downloaded_bytes'] = current_segment_downloaded_bytes

                    # Speed Limit Logic for this segment thread
                    if self.__class__.global_speed_limit_bps and self.__class__.global_speed_limit_bps > 0:
                        time_for_chunk_at_limit = len(chunk) / self.__class__.global_speed_limit_bps
                        time.sleep(time_for_chunk_at_limit)

                if not self._is_cancelled:
                    expected_segment_size = (end_byte - start_byte + 1)
                    if current_segment_downloaded_bytes >= expected_segment_size:
                        with self._file_write_lock:
                            self._segments[segment_index]['status'] = STATUS_COMPLETE
                        logger.debug(f"Segment {segment_index}: Completed.")
                    else:
                        with self._file_write_lock:
                            self._segments[segment_index]['status'] = STATUS_INCOMPLETE
                        logger.warning(
                            f"Segment {segment_index}: Incomplete ({current_segment_downloaded_bytes} of {expected_segment_size}).")

        except httpx.HTTPStatusError as e:
            logger.error(f"Segment {segment_index} HTTP Error: {e.response.status_code} for URL {e.request.url}")
            with self._file_write_lock:
                self._segments[segment_index]['status'] = f"Error HTTP {e.response.status_code}"
            self._is_cancelled = True
        except httpx.RequestError as e:
            logger.error(f"Segment {segment_index} Network Error: {e} for URL {self.url}")
            with self._file_write_lock:
                self._segments[segment_index]['status'] = "Error (Network)"
            self._is_cancelled = True
        except Exception as e:
            logger.error(f"Segment {segment_index} Generic Error: {e}", exc_info=True)
            with self._file_write_lock:
                self._segments[segment_index]['status'] = "Error (General)"
            self._is_cancelled = True

    def _run_yt_dlp_download(self):
        self.status_changed.emit(STATUS_YTDLP)
        self.db.update_status(self.download_id, STATUS_YTDLP)
        logger.info(f"Starting yt_dlp module process for {self.url}")

        self.save_path.parent.mkdir(parents=True, exist_ok=True)

        ffmpeg_path = ""
        try:
            base_path = Path(sys.argv[0]).parent
            potential_path = base_path / "bin" / "ffmpeg.exe"
            if potential_path.exists():
                ffmpeg_path = str(potential_path)
                logger.info(f"Found ffmpeg at: {ffmpeg_path}")
            else:
                logger.warning("ffmpeg.exe not found in 'bin' directory.")
        except Exception as e:
            logger.error(f"Could not determine ffmpeg path: {e}")

        download_format = self.selected_format_id or 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'

        ydl_opts = [
            sys.executable, '-m', 'yt_dlp', '--no-warnings', '--progress-template',
            '%(progress.status)s,%(progress.downloaded_bytes)s,%(progress.total_bytes)s,%(progress.speed)s,%(progress.eta)s,%(progress.fragment_index)s,%(progress.fragment_count)s\n',
            '-f', download_format,
            '-o', str(self.save_path), self.url
        ]
        if ffmpeg_path:
            ydl_opts.extend(['--ffmpeg-location', ffmpeg_path])

        creation_flags = 0
        preexec_fn = None
        if sys.platform == "win32":
            creation_flags = subprocess.CREATE_NO_WINDOW
        else:
            preexec_fn = os.setsid

        self._yt_dlp_process = None
        try:
            self._yt_dlp_process = subprocess.Popen(
                ydl_opts, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                encoding='utf-8', errors='ignore', creationflags=creation_flags, preexec_fn=preexec_fn
            )
            for line in iter(self._yt_dlp_process.stdout.readline, ''):
                if self._is_cancelled:
                    self._kill_process_tree(self._yt_dlp_process.pid)
                    break
                line = line.strip()
                try:
                    parts = line.split(',')
                    if len(parts) == 7 and parts[0] == 'downloading':
                        _status, downloaded_str, total_str, speed_str, eta_str, frag_idx_str, frag_count_str = parts
                        progress_percent = 0
                        self._downloaded_bytes = int(float(downloaded_str)) if downloaded_str != "NA" else 0

                        if total_str != "NA":
                            self._total_size = int(float(total_str))
                            if self._total_size > 0: progress_percent = int(
                                (self._downloaded_bytes / self._total_size) * 100)
                        elif frag_count_str != "NA":
                            self._total_size = 0  # Set total size to 0 for streams
                            current_frag, total_frags = int(frag_idx_str), int(frag_count_str)
                            if total_frags > 0: progress_percent = int((current_frag / total_frags) * 100)

                        speed = float(speed_str) if speed_str != "NA" else 0
                        eta = int(float(eta_str)) if eta_str != "NA" else 0
                        speed_str, eta_str = f"{speed / (1024 * 1024):.2f} MB/s", f"{eta // 60}m {eta % 60}s"
                        self.progress_updated.emit(progress_percent, speed_str, eta_str, self._downloaded_bytes,
                                                   self._total_size)
                except (ValueError, TypeError):
                    pass
            self._yt_dlp_process.wait()
            if self._is_cancelled:
                self.status_changed.emit(STATUS_CANCELLED)
                self.db.update_status(self.download_id, STATUS_CANCELLED)
            elif self._yt_dlp_process.returncode == 0:
                logger.info(f"yt_dlp download finished successfully.")
                self.status_changed.emit(STATUS_COMPLETE)
                self.db.mark_complete(self.download_id, QDateTime.currentDateTime().toString(Qt.ISODate))
                self.finished_successfully.emit(str(self.save_path))
            else:
                stderr_output = self._yt_dlp_process.stderr.read()
                error_message = f"yt-dlp process failed with exit code {self._yt_dlp_process.returncode}. Error: {stderr_output.strip()}"
                logger.error(error_message)
                self.status_changed.emit(f"Error: {stderr_output.strip()}")
                self.db.update_status(self.download_id, STATUS_ERROR)

        except Exception as e:
            logger.error(f"A critical error occurred while running yt-dlp: {e}", exc_info=True)
            self.status_changed.emit("Error: Critical failure")
            self.db.update_status(self.download_id, STATUS_ERROR)
        finally:
            self._is_running = False
            self._yt_dlp_process = None

    def run(self):
        self._is_running = True
        self._pause_event.set()

        if self._is_cancelled:
            logger.info(f"Download {self.download_id} run() called but already cancelled.")
            self.status_changed.emit(STATUS_CANCELLED)
            self._is_running = False
            return

        if self.is_stream:
            self._run_yt_dlp_download()
            return

        # If not a stream, proceed with multi-connection download
        timeout_config = httpx.Timeout(30.0, connect=15.0)
        # Ensure auth is a tuple (username, password) or None
        auth_param = tuple(self.auth) if self.auth and isinstance(self.auth, (list, tuple)) and len(
            self.auth) == 2 else None

        try:
            self.client = httpx.Client(
                auth=auth_param,
                timeout=timeout_config,
                follow_redirects=True,
                headers={"User-Agent": "PIDM/1.1 Mozilla/5.0"}
            )

            if self.custom_headers:
                self.client.headers.update(self.custom_headers)

            self.status_changed.emit(STATUS_CONNECTING)
            self.db.update_status(self.download_id, STATUS_CONNECTING)

            # If total size is unknown, perform a HEAD request to get it
            if self._total_size == 0:
                logger.info(f"Download {self.download_id}: Total size unknown, attempting to fetch from server.")
                try:
                    response = self.client.head(self.url)
                    response.raise_for_status()
                    content_length = response.headers.get("Content-Length")
                    if content_length:
                        self._total_size = int(content_length)
                        self.total_size_known.emit(self._total_size)
                        self.db.update_total_size(self.download_id, self._total_size)
                    else:
                        logger.error(f"Download {self.download_id}: Could not get total size from HEAD request.")
                        self.status_changed.emit("Error: Size unknown")
                        self._is_running = False
                        return
                except Exception as e:
                    logger.error(f"Download {self.download_id}: Failed to get total size: {e}")
                    self.status_changed.emit("Error: Failed to get size")
                    self._is_running = False
                    return

            if self._total_size <= 0:
                logger.error(f"Download {self.download_id}: Invalid total size ({self._total_size}). Cannot proceed.")
                self.status_changed.emit("Error: Invalid size")
                self._is_running = False
                return

            self.save_path.parent.mkdir(parents=True, exist_ok=True)

            # --- Multi-connection resume logic and file preparation ---
            resume_data = self.resume_helper.read()
            file_needs_creation = not self.save_path.exists()

            if resume_data and resume_data.get("url") == self.url and \
                    resume_data.get("total_size") == self._total_size and \
                    isinstance(resume_data.get("segments"), list) and len(
                resume_data["segments"]) == self.NUM_CONNECTIONS:

                self._segments = resume_data["segments"]
                logger.info(
                    f"[Resume] Using .resume file for {self.download_id}. Resuming {len(self._segments)} segments.")

                current_file_size = self.save_path.stat().st_size if self.save_path.exists() else 0
                sum_downloaded_from_segments = sum(s['downloaded_bytes'] for s in self._segments)

                if abs(current_file_size - sum_downloaded_from_segments) > 1024 * 1024:  # 1MB tolerance
                    logger.warning(
                        f"[Resume] Large size mismatch for {self.download_id}. File: {current_file_size}, Segments sum: {sum_downloaded_from_segments}. Re-initializing segments.")
                    file_needs_creation = True
                elif current_file_size < sum_downloaded_from_segments:
                    logger.warning(
                        f"[Resume] File size ({current_file_size}) < segments sum ({sum_downloaded_from_segments}) for {self.download_id}. Re-initializing segments.")
                    file_needs_creation = True
                else:
                    pass

            else:
                logger.info(
                    f"[Resume] No valid .resume file or mismatch for {self.download_id}. Starting fresh download.")
                file_needs_creation = True

            try:
                # Open file in binary write mode (creates if not exists, overwrites if wb, keeps if rb+)
                if file_needs_creation:
                    with open(self.save_path, "wb") as f:
                        f.truncate(self._total_size)
                    logger.info(
                        f"Download {self.download_id}: File {self.save_path} pre-allocated to {self._total_size} bytes.")

                    self._segments = []
                    bytes_per_segment = self._total_size // self.NUM_CONNECTIONS

                    for i in range(self.NUM_CONNECTIONS):
                        start_byte = i * bytes_per_segment
                        end_byte = start_byte + bytes_per_segment - 1
                        if i == self.NUM_CONNECTIONS - 1:
                            end_byte = self._total_size - 1

                        end_byte = min(end_byte, self._total_size - 1)

                        if start_byte <= end_byte:
                            self._segments.append({
                                'index': i,
                                'start_byte': start_byte,
                                'end_byte': end_byte,
                                'downloaded_bytes': 0,
                                'status': STATUS_CONNECTING
                            })
                            logger.debug(f"Segment {i}: {start_byte}-{end_byte}")
                        else:
                            logger.warning(f"Segment {i}: Invalid range ({start_byte}-{end_byte}), skipping.")
                else:
                    with open(self.save_path, "rb+") as f:
                        pass
                    logger.info(f"Download {self.download_id}: Resuming existing file {self.save_path}.")

            except IOError as e:
                logger.error(f"Download {self.download_id}: Failed to pre-allocate/access file: {e}")
                self.status_changed.emit(f"Error: File access ({e.strerror})")
                self._is_running = False
                return

            # --- Segment Threading (start segments from their current downloaded position) ---
            self._segment_threads = []
            for i, segment_info in enumerate(self._segments):
                if self._is_cancelled:
                    break

                actual_request_start_byte = segment_info['start_byte'] + segment_info['downloaded_bytes']
                if segment_info['downloaded_bytes'] < (segment_info['end_byte'] - segment_info['start_byte'] + 1):
                    thread = threading.Thread(target=self._download_segment,
                                              args=(
                                                  segment_info['index'], actual_request_start_byte,
                                                  segment_info['end_byte']))
                    self._segment_threads.append(thread)
                    thread.start()
                    logger.debug(
                        f"Download {self.download_id}: Started thread for segment {i} from byte {actual_request_start_byte}.")
                else:
                    logger.debug(
                        f"Download {self.download_id}: Segment {i} already complete, skipping thread creation.")

            # --- Main loop for progress reporting and waiting for segments to finish ---
            self.status_changed.emit(STATUS_DOWNLOADING)
            self.db.update_status(self.download_id, STATUS_DOWNLOADING)

            while not self._is_cancelled and any(
                    s['downloaded_bytes'] < (s['end_byte'] - s['start_byte'] + 1) for s in self._segments):
                total_downloaded = sum(s['downloaded_bytes'] for s in self._segments)
                self._downloaded_bytes = total_downloaded

                now = time.time()
                if now - self._last_speed_sample_time >= (self._speed_sample_interval_ms / 1000.0):
                    self._speed_samples.append((now, self._downloaded_bytes))
                    self._last_speed_sample_time = now

                progress_percent = int(
                    (self._downloaded_bytes / self._total_size) * 100) if self._total_size > 0 else -1
                speed_str, eta_str = self._calculate_speed_and_eta()

                self.progress_updated.emit(progress_percent, speed_str, eta_str, self._downloaded_bytes,
                                           self._total_size)

                now = time.time()
                bytes_since_last_db_write = self._downloaded_bytes - self._last_db_write_bytes

                if (now - self._last_db_write_time >= self._db_write_interval) or \
                        (bytes_since_last_db_write >= self._min_bytes_for_db_write):
                    self.db.update_download_progress_details(self.download_id, max(0, progress_percent),
                                                             self._downloaded_bytes)
                    self._last_db_write_time = now
                    self._last_db_write_bytes = self._downloaded_bytes
                    if self._total_size > 0:
                        with self._file_write_lock:
                            segments_to_save = [s.copy() for s in self._segments]
                        self.resume_helper.write(self._total_size, self.url, segments_to_save)

                self.msleep(self._speed_sample_interval_ms)

            # Wait for all threads to truly finish after loop condition is met or cancelled
            for thread in self._segment_threads:
                if thread.is_alive():
                    logger.debug(f"Download {self.download_id}: Main thread waiting for segment thread to join at end.")
                    thread.join(timeout=5.0)  # Give threads a chance to finish cleanly
                    if thread.is_alive():
                        logger.warning(f"Download {self.download_id}: Segment thread did not join gracefully at end.")

            # --- Final Status Update ---
            if self._is_cancelled:
                self.status_changed.emit(STATUS_CANCELLED)
                self.db.update_status(self.download_id, STATUS_CANCELLED)
                self.resume_helper.delete()
            else:
                # Check if all segments internally reported complete
                all_segments_reported_complete = all(
                    s['downloaded_bytes'] >= (s['end_byte'] - s['start_byte'] + 1) for s in self._segments)

                # Get the actual file size on disk for a robust check
                actual_file_size_on_disk = 0
                if self.save_path.exists():
                    try:
                        actual_file_size_on_disk = self.save_path.stat().st_size
                    except Exception as e:
                        logger.error(
                            f"Download {self.download_id}: Could not get actual file size from disk for final check: {e}")

                # Determine if the download is considered complete
                is_actually_complete = False
                if self._total_size > 0:
                    if (self._downloaded_bytes == self._total_size) or \
                            (self._downloaded_bytes == self._total_size - 1 and self._total_size > 0) or \
                            (actual_file_size_on_disk == self._total_size):
                        is_actually_complete = True
                elif self._total_size == 0 and self._downloaded_bytes > 0:
                    is_actually_complete = True

                if is_actually_complete:
                    self.status_changed.emit(STATUS_COMPLETE)
                    self.db.mark_complete(self.download_id, QDateTime.currentDateTime().toString(Qt.ISODate))
                    self.finished_successfully.emit(str(self.save_path))
                    self.resume_helper.delete()
                else:
                    logger.warning(f"Download {self.download_id}: Marked as INCOMPLETE. Final check failed.")
                    logger.warning(f"  - Downloaded bytes: {self._downloaded_bytes} / Total size: {self._total_size}")
                    logger.warning(f"  - Actual size on disk: {actual_file_size_on_disk}")
                    logger.warning(f"  - All segments reported complete: {all_segments_reported_complete}")
                    self.status_changed.emit(STATUS_INCOMPLETE)
                    self.db.update_status(self.download_id, STATUS_INCOMPLETE)

        finally:
            if self.client:
                try:
                    self.client.close()
                except Exception as e_close:
                    logger.debug(f"Exception closing httpx.Client for {self.download_id}: {e_close}")
            self._is_running = False


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
        self.is_stream = False  # New flag to indicate if it's a stream

    def run(self):
        try:
            info = self._get_ytdlp_info(self.original_url)
            is_ytdlp_stream = False

            if info:
                formats = info.get("formats", [])
                is_ytdlp_stream = (
                        info.get('protocol') in ['hls', 'm3u8', 'm3u8_native', 'http_dash_segments'] or
                        info.get('is_live') is True or
                        (formats and len(formats) > 1)
                )

            if is_ytdlp_stream:
                logger.info(
                    f"yt-dlp identified a stream. Protocol: {info.get('protocol')}, Extractor: {info.get('extractor_key')}")
                self.is_stream = True

                filename = info.get('title', guess_filename_from_url(self.original_url))
                if 'ext' in info:
                    filename = f"{filename}.{info['ext']}"

                # Build a list of cleaned-up formats for quality selection
                clean_formats = []
                for fmt in info.get("formats", []):
                    if fmt.get("vcodec", "none") != "none":
                        label = self._format_resolution_label(fmt)
                        clean_formats.append({
                            "format_id": fmt.get("format_id"),
                            "ext": fmt.get("ext"),
                            "filesize": fmt.get("filesize") or fmt.get("filesize_approx"),
                            "resolution": label,
                            "format_note": fmt.get("format_note", ""),
                        })

                # Emit full metadata including formats
                self.result.emit({
                    "content_length": info.get('filesize') or info.get('filesize_approx') or 0,
                    "content_type": "video/stream",
                    "filename": filename,
                    "final_url": self.original_url,
                    "is_stream": True,
                    "formats": clean_formats
                })
                return

            else:
                logger.debug("yt-dlp did not identify a specific stream. Proceeding with standard HTTP fetch.")

        except Exception as e:
            logger.debug(f"yt-dlp check failed, proceeding with standard HTTP. Error: {e}")

        # Fallback to HTTP metadata
        try:
            metadata = self._fetch_metadata_attempt(self.original_url)
            if metadata:
                metadata["is_stream"] = False
                self.result.emit(metadata)
                return
        except httpx.TimeoutException:
            logger.error(f"[MetadataFetcher] Timeout for {self.original_url}")
            self.timeout.emit()
            return
        except Exception as e:
            logger.error(f"[MetadataFetcher] Error with {self.original_url}: {e}")

        # Final fallback
        if self.original_url.startswith("https://"):
            fallback_url = self.original_url.replace("https://", "http://", 1)
            logging.debug(f"[MetadataFetcher] Trying HTTP fallback: {fallback_url}")
            try:
                metadata = self._fetch_metadata_attempt(fallback_url)
                if metadata:
                    metadata["is_stream"] = False
                    self.result.emit(metadata)
                    return
            except Exception:
                pass

        logging.debug(f"[MetadataFetcher] All attempts failed for {self.original_url}. Emitting error.")
        self.error.emit(self.tr("Could not fetch metadata for the given URL."))

    def _format_resolution_label(self, fmt):
        height = fmt.get("height")
        if height:
            if height >= 2160:
                return "4K"
            elif height >= 1440:
                return "1440p"
            elif height >= 1080:
                return "1080p"
            elif height >= 720:
                return "720p"
            elif height >= 480:
                return "480p"
            elif height >= 360:
                return "360p"
            elif height >= 240:
                return "240p"
        return fmt.get("format_note") or "Unknown"

    def _get_ytdlp_info(self, url):
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'simulate': True,
                'force_generic_extractor': False,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if 'entries' in info and info['entries']:
                    return info['entries'][0]
                return info
        except yt_dlp.utils.DownloadError:
            return None

    def _is_streamable_by_ytdlp(self, url):
        """
        Checks if a URL is recognized and streamable by yt_dlp without downloading.
        """
        try:
            # Suppress yt_dlp's own logging to avoid clutter
            class NullLogger(object):
                def debug(self, msg): pass

                def warning(self, msg): pass

                def error(self, msg): pass

            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'force_generic_extractor': False,
                'logger': NullLogger(),
                'skip_download': True,
                'simulate': True,
                'extract_flat': True,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False, process=False)  # process=False to avoid full processing
                return 'entries' in info or 'id' in info
        except yt_dlp.utils.DownloadError as e:
            logger.debug(f"yt_dlp failed to extract info for {url}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Unexpected error during yt_dlp check for {url}: {e}")
            return False

    def _fetch_metadata_attempt(self, url_to_fetch):
        request_headers = {"User-Agent": "PIDM/1.1 Mozilla/5.0"}
        if self.custom_headers:
            request_headers.update(self.custom_headers)

        auth_param = tuple(self.auth) if self.auth and isinstance(self.auth, (list, tuple)) and len(
            self.auth) == 2 else None

        try:
            transport = httpx.HTTPTransport(retries=1)
            with httpx.Client(auth=auth_param, follow_redirects=True, timeout=self.max_seconds,
                              transport=transport) as client:

                headers_for_get = request_headers.copy()
                headers_for_get["Range"] = "bytes=0-0"

                logging.debug(
                    f"[MetadataFetcher] Sending GET (range 0-0) request to: {url_to_fetch} with headers: {headers_for_get}")
                response_get = client.get(url_to_fetch, headers=headers_for_get)
                response_get.raise_for_status()
                logging.debug(
                    f"[MetadataFetcher] GET (range 0-0) response from {response_get.url} headers: {dict(response_get.headers)}")

                try:
                    for chunk in response_get.iter_bytes(chunk_size=1):
                        break
                finally:
                    response_get.close()

                metadata = self._parse_headers(response_get.headers, str(response_get.url))
                metadata["is_stream"] = False
                return metadata

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
    update_available = Signal(str, str)

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
    proxy_startup_failed = Signal(str)

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

        self.server = None
        error_message = self.tr("Could not bind to any port in the range {0}-{1}.").format(
            start_port, start_port + max_tries - 1
        )
        logger.error(f"[ProxyServer] FATAL: {error_message}")
        self.proxy_startup_failed.emit(error_message)

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
                    self._set_headers(400);
                    self.wfile.write(b'{"error":"invalid json"}')
                except Exception as e:
                    self._set_headers(500);
                    self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))

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
            "final_url": self.url,
            "is_stream": False  # Default to False, MetadataFetcher will update
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


class QualitySelectionDialog(QDialog):
    def __init__(self, formats: list[dict], parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Select Video Quality"))
        self.setMinimumWidth(400)
        self.selected_format_id = None

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(self.tr("Choose video quality:")))

        self.list_widget = QListWidget()

        qualities = self._process_and_sort_formats(formats)

        for label, fmt_id in qualities:
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, fmt_id)
            self.list_widget.addItem(item)

        if self.list_widget.count() > 0:
            self.list_widget.setCurrentRow(0)
        layout.addWidget(self.list_widget)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _get_sort_key(self, resolution_str: str) -> int:
        resolution_str = resolution_str.lower()
        if "4k" in resolution_str:
            return 2160
        numeric_part = re.search(r'(\d+)', resolution_str)
        if numeric_part:
            return int(numeric_part.group(1))
        return 0

    def _process_and_sort_formats(self, formats: list[dict]):
        seen_resolutions = {}
        for f in formats:
            fmt_id = f.get("format_id")
            label = f.get("resolution")

            if not label or not fmt_id or label.lower() == "unknown":
                continue

            if label not in seen_resolutions:
                seen_resolutions[label] = fmt_id

        sorted_labels = sorted(
            seen_resolutions.keys(),
            key=self._get_sort_key,
            reverse=True
        )
        return [(label, seen_resolutions[label]) for label in sorted_labels]

    def _on_accept(self):
        selected = self.list_widget.currentItem()
        if selected:
            self.selected_format_id = selected.data(Qt.UserRole)
        self.accept()

    def get_selected_format_id(self):
        return self.selected_format_id


class LoadingSpinnerWidget(QWidget):
    """A custom widget that displays a spinning loading animation."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._angle = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._animate)
        self._timer.setInterval(15)
        self.setMinimumSize(50, 50)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        side = min(self.width(), self.height())

        pen = QPen()
        pen.setColor(QColor("white") if self.palette().window().color().lightness() < 128 else QColor("black"))
        pen_width = max(2, int(side / 15.0))
        pen.setWidth(pen_width)
        pen.setCapStyle(Qt.RoundCap)
        painter.setPen(pen)

        margin = pen_width / 2
        rect = QRectF(margin, margin, side - pen_width, side - pen_width)
        rect.moveCenter(self.rect().center())

        painter.drawArc(rect, self._angle * 16, 270 * 16)

    def _animate(self):
        self._angle = (self._angle + 8) % 360
        self.update()

    def start(self):
        self.show()
        self._timer.start()

    def stop(self):
        self._timer.stop()
        self.hide()


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
        self.is_stream_download = False

        apply_standalone_style(self, self.settings)

        # --- Blur and Loading Overlay ---
        self.blur_effect = QGraphicsBlurEffect(self)
        self.blur_effect.setBlurRadius(8)

        self.loading_overlay = QWidget(self)
        self.loading_overlay.setStyleSheet("background-color: transparent;")
        self.loading_overlay.hide()

        self.loading_spinner = LoadingSpinnerWidget(self.loading_overlay)
        self.loading_spinner.setFixedSize(80, 80)

        # --- Main Content Widget ---
        self.content_widget = QWidget()
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(self.content_widget)

        # --- UI Setup ---
        content_layout = QVBoxLayout(self.content_widget)
        content_layout.setContentsMargins(15, 15, 15, 15)
        content_layout.setSpacing(12)

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

        form_layout.addRow(self.tr("URL:"), self.url_display)
        form_layout.addRow(self.tr("Save As:"), save_path_layout)
        form_layout.addRow("", self.remember_path_checkbox)
        form_layout.addRow(self.tr("Description:"), self.description_input)

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

        content_layout.addLayout(top_layout)
        content_layout.addSpacing(10)
        content_layout.addWidget(button_box)

    def _add_to_database_and_close(self, db_payload):
        if not db_payload:
            return
        try:
            self.final_download_id = self.db.add_download(db_payload)
            self.download_added.emit()
            super().accept()
        except DuplicateDownloadError:
            QMessageBox.warning(self, self.tr("Duplicate Download"),
                                self.tr("This download (URL and save path) already exists in the list."))
        except Exception as e:
            logger.error(f"An unexpected error occurred when adding download: {e}")
            QMessageBox.critical(self, self.tr("Database Error"),
                                 self.tr("An unexpected error occurred. Please check the logs."))

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.loading_overlay.resize(self.size())
        self.loading_spinner.move(
            (self.width() - self.loading_spinner.width()) // 2,
            (self.height() - self.loading_spinner.height()) // 2
        )

    def _on_save_path_selected(self, index: int):
        """When a user selects a recent path, combine it with the current filename."""
        selected_path_str = self.save_path_combo.itemText(index)
        path_obj = Path(selected_path_str)

        if path_obj.is_dir():
            current_filename = self.fetched_metadata.get("filename") or self.guessed_filename
            new_full_path = str(path_obj / current_filename)
            self.save_path_combo.setCurrentText(new_full_path)

    def fetch_metadata_async(self, url: str, auth: tuple | None = None, custom_headers=None):
        self.custom_headers_for_download = custom_headers
        self._metadata_fetcher = MetadataFetcher(url, auth, custom_headers=custom_headers)
        self._metadata_fetcher.result.connect(self.update_metadata_fields)
        self._metadata_fetcher.error.connect(self._handle_metadata_error)
        self._metadata_fetcher.timeout.connect(self._handle_metadata_timeout)

        self.content_widget.setGraphicsEffect(self.blur_effect)
        self.loading_overlay.show()
        self.loading_overlay.raise_()
        self.loading_spinner.start()

        QTimer.singleShot(0, self._metadata_fetcher.start)

    def _hide_loading_overlay(self):
        self.loading_spinner.stop()
        self.loading_overlay.hide()
        self.content_widget.setGraphicsEffect(None)

    def _handle_metadata_error(self, error_message):
        self._hide_loading_overlay()
        QMessageBox.warning(self, self.tr("Metadata Error"),
                            self.tr("Could not fetch download information:\n{0}").format(error_message))
        logger.error(f"Metadata Error Slot Called: {error_message}")
        self.reject()

    def _handle_metadata_timeout(self):
        self._hide_loading_overlay()
        QMessageBox.warning(self, self.tr("Metadata Timeout"),
                            self.tr("Fetching download information timed out."))
        logger.warning("Metadata Timeout Slot Called")
        self.reject()

    def update_metadata_fields(self, metadata: dict):
        self.fetched_metadata = metadata
        self.is_stream_download = metadata.get("is_stream", False)

        if self.is_stream_download and metadata.get("formats"):
            dialog = QualitySelectionDialog(metadata["formats"], self)
            if dialog.exec() == QDialog.Accepted:
                selected_format = dialog.get_selected_format_id()
                if selected_format:
                    self.fetched_metadata['final_url'] = self.url_display.text()
                    self.fetched_metadata["selected_format_id"] = selected_format
            else:
                video_formats = [f for f in metadata.get("formats", []) if
                                 f.get("vcodec", "none") != "none" and f.get("height")]
                if video_formats:
                    best_format = max(video_formats, key=lambda f: f.get("height", 0))
                    self.fetched_metadata["selected_format_id"] = best_format.get("format_id")
                    logger.info(
                        f"Quality selection cancelled. Defaulting to best format: {best_format.get('resolution')}")
                else:
                    self.fetched_metadata["selected_format_id"] = 'best'

        self._hide_loading_overlay()

        filename = metadata.get("filename", self.guessed_filename)
        size_bytes = metadata.get("content_length", 0)

        self.file_name_preview_label.setText(self.tr("Filename: ") + filename)
        if self.is_stream_download:
            self.file_size_preview_label.setText(self.tr("Size: Streaming"))
            base_filename, _ = os.path.splitext(filename)
            default_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
            self.save_path_combo.setCurrentText(str(Path(default_dir) / base_filename))
        else:
            self.file_size_preview_label.setText(self.tr("Size: ") + format_size(size_bytes))
            current_path_in_combo = self.save_path_combo.currentText().strip()
            if not current_path_in_combo or Path(current_path_in_combo).is_dir():
                default_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
                self.save_path_combo.setCurrentText(str(Path(default_dir) / filename))

        icon = get_system_icon_for_file(filename)
        self.file_icon_label.setPixmap(icon.pixmap(48, 48))

    def set_initial_data(self, url: str, metadata: dict | None, auth: tuple | None):
        self.url_display.setText(url)
        self.auth_tuple = auth
        self.fetched_metadata = metadata or {}
        self.is_stream_download = self.fetched_metadata.get("is_stream", False)  # Initialize stream flag

        filename = self.fetched_metadata.get("filename") or guess_filename_from_url(url)
        self.guessed_filename = filename
        size_bytes = self.fetched_metadata.get("content_length", 0)

        icon = get_system_icon_for_file(filename)
        pixmap = icon.pixmap(48, 48)
        self.file_icon_label.setPixmap(
            pixmap if not pixmap.isNull() else QIcon(get_asset_path("assets/icons/file-x.svg")).pixmap(48, 48))
        self.file_name_preview_label.setText(self.tr("Filename: ") + filename)

        # Adjust size label for streams
        if self.is_stream_download:
            self.file_size_preview_label.setText(self.tr("Size: Streaming (Unknown)"))
            default_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
            base_filename = Path(filename).stem
            self.save_path_combo.setCurrentText(str(Path(default_dir) / base_filename))
        else:
            self.file_size_preview_label.setText(self.tr("Size: ") + format_size(size_bytes))
            current_path_in_combo = self.save_path_combo.currentText().strip()
            if not current_path_in_combo or Path(current_path_in_combo).is_dir():
                default_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
                self.save_path_combo.setCurrentText(str(Path(default_dir) / filename))

    def select_save_path(self):
        authoritative_filename = self.fetched_metadata.get("filename") or self.guessed_filename
        current_path_str = self.save_path_combo.currentText().strip()
        start_dir = self.settings.get("default_download_directory", str(Path.home() / "Downloads"))
        if current_path_str:
            p = Path(current_path_str)
            if p.name:
                start_dir = str(p.parent)
            else:
                start_dir = str(p)

        if not Path(start_dir).is_dir():
            start_dir = str(Path.home() / "Downloads")
            Path(start_dir).mkdir(parents=True, exist_ok=True)

        # For streams, suggest filename without extension, as yt-dlp will add it
        if self.is_stream_download:
            target_path_suggestion = str(Path(start_dir) / Path(authoritative_filename).stem)
            file_filter = self.tr("All Files (*)")  # No specific filter for streams
        else:
            target_path_suggestion = str(Path(start_dir) / authoritative_filename)
            ext = Path(authoritative_filename).suffix
            mime_type = self.fetched_metadata.get("content_type", "application/octet-stream")
            if not ext and mime_type != "application/octet-stream":
                guessed_ext = mimetypes.guess_extension(mime_type)
                if guessed_ext: ext = guessed_ext
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
            "file_name": self.fetched_metadata.get("filename") or Path(download_path_str).name,
            "save_path": download_path_str,
            "description": self.description_input.text().strip(),
            "status": status_on_add,
            "queue_id": queue_id,
            "queue_position": queue_pos,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_size": self.fetched_metadata.get("content_length", 0),
            "referrer": self.custom_headers_for_download.get("Referer") if self.custom_headers_for_download else None,
            "custom_headers": self.custom_headers_for_download,
            "auth": self.auth_tuple,
            "is_stream": self.is_stream_download,
            "selected_format_id": self.fetched_metadata.get("selected_format_id")
        }
        return data_for_db

    def _handle_download_later(self):
        self.accepted_action = "later"
        queue_dialog = SelectQueueDialog(self.db, self.settings, self)
        if queue_dialog.exec() != QDialog.Accepted:
            return

        selected_queue_id, remember_choice = queue_dialog.get_selected_queue_info()
        if selected_queue_id is None:
            QMessageBox.warning(self, self.tr("No Queue Selected"),
                                self.tr("You must select a queue to use 'Download Later'."))
            return

        if remember_choice:
            self.settings.set("remembered_queue_id_for_later", selected_queue_id)
        else:
            self.settings.set("remembered_queue_id_for_later", None)

        db_payload = self._prepare_download_data(STATUS_QUEUED, selected_queue_id)
        self._add_to_database_and_close(db_payload)

    def _handle_download_now(self):
        self.accepted_action = "now"

        db_payload = self._prepare_download_data(STATUS_CONNECTING, queue_id=None)
        self._add_to_database_and_close(db_payload)

    def get_final_download_data(self) -> tuple[int | None, str | None]:
        if self.result() == QDialog.Accepted and self.final_download_id is not None:
            return self.final_download_id, self.accepted_action
        return None, None


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

        # Initial display update
        self._update_display(
            int((worker._downloaded_bytes / worker._total_size) * 100) if worker._total_size > 0 else 0,
            "0 KB/s", "--", worker._downloaded_bytes, worker._total_size
        )
        # Handle initial status for streams
        if worker.is_stream:
            self.status_label.setText(self.tr("Status: ") + self.tr(STATUS_YTDLP))
            self.progress_bar.setRange(0, 0)
            self.progress_bar.setFormat(self.tr("Downloading..."))

    @Slot(int, str, str, float, float)
    def _update_display(self, percent, speed_str, eta_str, downloaded, total):
        if self.worker.is_stream:
            self.progress_bar.setRange(0, 0)
            self.progress_bar.setFormat(self.tr("Downloading..."))
        else:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(percent if percent >= 0 else 0)
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
        status_text = tr_status(download_data.get('status', self.tr("N/A")))
        form_layout.addRow(self.tr("<b>Status:</b>"), QLabel(status_text))

        # --- Size (Read-only) ---
        size_bytes = download_data.get("total_size", 0)
        size_display = format_size(size_bytes)
        if download_data.get("is_stream", False):  # Check if it's a stream
            size_display = self.tr("Streaming (Unknown)") if size_bytes == 0 else size_display
        form_layout.addRow(self.tr("<b>Size:</b>"), QLabel(size_display))

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
            Path(dir_path).mkdir(parents=True, exist_ok=True)

        # For streams, suggest filename without extension, as yt-dlp will add it
        if self.download_data.get("is_stream", False):
            target_path = str(Path(dir_path) / Path(file_name_suggestion).stem)
            file_filter = self.tr("All Files (*)")
        else:
            target_path = str(Path(dir_path) / file_name_suggestion)
            ext = Path(file_name_suggestion).suffix
            mime_type = self.download_data.get("content_type", "application/octet-stream")
            if not ext and mime_type != "application/octet-stream":
                guessed_ext = mimetypes.guess_extension(mime_type)
                if guessed_ext: ext = guessed_ext
            file_filter = f"{mime_type} (*{ext})" if ext else self.tr("All Files (*)")

        new_path_str, _ = QFileDialog.getSaveFileName(self, self.tr("Change Save Location"), target_path, file_filter)
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
        self.app_version = "1.1.2"
        self.settings = SettingsManager()
        self.db = DatabaseManager()
        self.is_quitting = False

        self.setWindowTitle(self.tr(f"Python Internet Download Manager {self.app_version}"))
        self.setWindowIcon(QIcon(get_asset_path("assets/icons/pidm_icon.ico")))
        self.resize(900, 550)

        if not self.db.get_queue_by_name("Main Queue"):
            self.db.add_queue("Main Queue", enabled=1, max_concurrent=self.settings.get("default_max_concurrent", 3))

        self.active_workers = {}
        self.download_id_to_row_map = {}

        self.category_filters = {
            self.tr("Compressed"): [".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso", ".cab", ".lz", ".lzma",
                                    ".ace", ".arj"],
            self.tr("Documents"): [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".rtf", ".odt",
                                   ".ods", ".csv", ".epub"],
            self.tr("Music"): [".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a", ".alac", ".aiff"],
            self.tr("Videos"): [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mpeg", ".mpg", ".3gp", ".ts",
                                ".m4v"],
            self.tr("Programs"): [".exe", ".msi", ".dmg", ".pkg", ".sh", ".bat", ".apk", ".jar", ".bin", ".app", ".deb",
                                  ".rpm"]
        }

        self.init_ui()
        self.apply_theme(self.settings.get("theme", "dark"))
        self.init_category_tree()
        self.load_downloads_from_db()
        self.update_action_buttons_state()

        self.proxy = ProxyServer()
        self.proxy.proxy_startup_failed.connect(self.handle_proxy_startup_failure)
        self.proxy.link_received.connect(self.handle_link_received)

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

        self.donate_action = QAction(self.tr("Donate"), self)
        self.donate_action.triggered.connect(
            lambda: QDesktopServices.openUrl(QUrl("https://www.saeedmasoudie.ir/donate.html"))
        )
        self.donate_action.setToolTip(self.tr("Support the project by a simple donate"))
        toolbar.addAction(self.report_bug_action)
        toolbar.addAction(self.donate_action)

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
            (STATUS_ERROR, lambda: self.tr("Error/Incomplete")), (STATUS_YTDLP, lambda: self.tr("Streaming"))
            # New status
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
        # If it's a stream and size is 0, display "Streaming"
        if download_data.get('is_stream', False) and download_data.get('total_size', 0) == 0:
            self.download_table.setItem(row_idx, 2, QTableWidgetItem(self.tr("Streaming")))
        else:
            self.download_table.setItem(row_idx, 2, QTableWidgetItem(total_size_str))

        status_key = download_data['status']
        status_text = tr_status(status_key)
        self.download_table.setItem(row_idx, 3, QTableWidgetItem(status_text))

        progress_bar = QProgressBar(self.download_table)
        progress_bar.setProperty("status", download_data['status'])
        progress_value = download_data.get('progress', 0)
        current_status = download_data['status']

        if current_status == STATUS_QUEUED: progress_value = 0

        # Handle progress bar for streams
        if download_data.get('is_stream', False) and current_status not in [STATUS_COMPLETE, STATUS_ERROR,
                                                                            STATUS_CANCELLED]:
            progress_bar.setRange(0, 0)  # Indeterminate
            progress_bar.setFormat(self.tr("Downloading..."))
        elif current_status == STATUS_YTDLP:
            progress_bar.setRange(0, 0)  # Indeterminate
            progress_bar.setFormat(self.tr("Streaming..."))
        elif download_data.get('total_size', 0) == 0 and current_status in [STATUS_DOWNLOADING, STATUS_CONNECTING,
                                                                            STATUS_RESUMING]:
            progress_bar.setRange(0, 0)
            progress_bar.setFormat(self.tr("Downloading..."))
        else:
            progress_bar.setRange(0, 100)
            progress_bar.setValue(progress_value if progress_value >= 0 else 0)
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
            if status in [STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING, STATUS_YTDLP]:
                if item_data.get('is_stream', False):
                    item_data['status'] = STATUS_INCOMPLETE
                    self.db.update_status(item_data['id'], STATUS_INCOMPLETE)
                else:
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
                            STATUS_ERROR, STATUS_YTDLP]:
            if filter_key == STATUS_ERROR:
                filtered_downloads_list = [d for d in all_db_downloads if
                                           d["status"] == STATUS_ERROR or d["status"] == STATUS_INCOMPLETE or "Error" in
                                           d["status"]]
            else:
                filtered_downloads_list = [d for d in all_db_downloads if d["status"] == filter_key]
        elif isinstance(filter_key, int):
            filtered_downloads_list = self.db.get_downloads_in_queue(filter_key)

        for download_item in filtered_downloads_list:
            if download_item['status'] in [STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING, STATUS_YTDLP] and \
                    download_item[
                        'id'] not in self.active_workers:
                # If it was downloading a stream and app closed, mark as incomplete
                if download_item.get('is_stream', False):
                    download_item['status'] = STATUS_INCOMPLETE
                else:
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
                is_stream = dl_data.get('is_stream', False)

                # Resume logic
                resumable_statuses = [STATUS_QUEUED, STATUS_PAUSED, STATUS_ERROR, STATUS_INCOMPLETE]
                if not is_stream and (status in resumable_statuses or "Error" in status):
                    can_resume_dl = True
                elif is_stream and status in [STATUS_QUEUED, STATUS_ERROR, STATUS_INCOMPLETE, STATUS_PAUSED]:
                    can_resume_dl = True

                # Pause logic
                pausable_statuses = [STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING, STATUS_YTDLP]
                if is_active and status in pausable_statuses:
                    can_pause_dl = True

                # Cancel logic
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
                    d["status"] not in [STATUS_COMPLETE, STATUS_DOWNLOADING, STATUS_CONNECTING, STATUS_RESUMING,
                                        STATUS_YTDLP] for d
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

        is_stream = dl_entry.get('is_stream', False)
        selected_format = dl_entry.get('selected_format_id')

        logger.info(f"Starting worker for ID {download_id} (Is Stream: {is_stream}, Format: {selected_format})")
        worker = DownloadWorker(
            download_id, self.db, dl_entry["url"], dl_entry["save_path"],
            initial_downloaded_bytes=dl_entry["bytes_downloaded"],
            total_file_size=dl_entry["total_size"],
            auth=auth_tuple,
            custom_headers=custom_headers,
            is_stream=is_stream,
            selected_format_id=selected_format,
            parent=self
        )

        worker.status_changed.connect(self._on_worker_status_changed)
        worker.progress_updated.connect(self._on_worker_progress_updated)
        worker.finished_successfully.connect(self._on_worker_finished_successfully)
        worker.total_size_known.connect(self._on_worker_total_size_known)
        worker.original_queue_id = dl_entry.get("queue_id")
        worker.request_file_deletion.connect(self._delete_associated_files)

        self.active_workers[download_id] = {"worker": worker}
        worker.start()
        self.update_action_buttons_state()

    @Slot(str)
    def _on_worker_status_changed(self, status_message: str):
        worker = self.sender()
        if not isinstance(worker, DownloadWorker):
            return
        download_id = worker.download_id

        dl_entry = self.db.get_download_by_id(download_id)
        if not dl_entry:
            logger.warning(
                f"Received status update for non-existent download ID {download_id}, likely already deleted.")
            if download_id in self.active_workers:
                del self.active_workers[download_id]
            return

        db_status = dl_entry.get('status')
        if status_message == STATUS_CANCELLED and db_status == STATUS_PAUSED:
            logger.debug(f"Ignoring CANCELLED signal for {download_id} because it was intentionally paused.")
            if download_id in self.active_workers:
                del self.active_workers[download_id]
            self.update_action_buttons_state()
            return

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
                    progress_bar.setRange(0, 100)
                    progress_bar.setFormat(self.tr("Paused"))
                elif "Error" in status_message or status_message == STATUS_INCOMPLETE:
                    progress_bar.setRange(0, 100)
                    progress_bar.setValue(0)
                    progress_bar.setFormat(self.tr("Error"))
                elif status_message == STATUS_QUEUED:
                    progress_bar.setRange(0, 100)
                    progress_bar.setValue(0)
                    progress_bar.setFormat(self.tr("Queued"))
                elif status_message == STATUS_YTDLP:
                    progress_bar.setRange(0, 0)
                    progress_bar.setFormat(self.tr("Streaming..."))

        if status_message in [STATUS_COMPLETE, STATUS_ERROR, STATUS_CANCELLED]:
            if download_id in self.active_workers:
                del self.active_workers[download_id]

        self.update_action_buttons_state()

    def check_for_updates_background(self):
        self.update_thread = UpdateCheckThread(self.app_version, self)
        self.update_thread.update_available.connect(self._show_update_dialog)
        self.update_thread.start()

    @Slot(int, str, str, float, float)
    def _on_worker_progress_updated(self, percent: int, speed: str, eta: str, downloaded_bytes: float,
                                    total_bytes: float):
        worker = self.sender()
        if not isinstance(worker, DownloadWorker):
            return
        download_id = worker.download_id

        row = self.download_id_to_row_map.get(download_id)
        if row is not None and row < self.download_table.rowCount():
            progress_bar = self.download_table.cellWidget(row, 4)
            size_item = self.download_table.item(row, 2)

            self.download_table.setItem(row, 5, QTableWidgetItem(speed))
            self.download_table.setItem(row, 6, QTableWidgetItem(eta))

            is_stream_with_known_progress = worker.is_stream and percent >= 0

            if not is_stream_with_known_progress and total_bytes <= 0:
                progress_bar.setRange(0, 0)
                progress_bar.setFormat(self.tr("Connecting..."))
            else:
                progress_bar.setRange(0, 100)
                progress_bar.setValue(max(0, percent))
                progress_bar.setFormat(f"{max(0, percent)}%")

            if total_bytes > 0:
                size_str = f"{format_size(downloaded_bytes)} / {format_size(total_bytes)}"
                size_item.setText(size_str)
            elif worker.is_stream:
                size_str = f"{format_size(downloaded_bytes)} / ∞"
                size_item.setText(size_str)
            else:
                size_item.setText("...")

    @Slot(str)
    def handle_proxy_startup_failure(self, error_message):
        """Shows a warning popup when the browser listener fails to start."""
        QMessageBox.warning(
            self,
            self.tr("Browser Integration Failed"),
            self.tr(
                "PIDM could not start the listener required for browser integration. "
                "This usually means another application (or another instance of PIDM) "
                "is using the required ports.\n\n"
                "You can still use the application to add downloads manually."
            )
        )

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
            if not worker.is_stream or total_size_val > 0:
                self.download_table.setItem(row, 2, QTableWidgetItem(format_size(total_size_val)))
            elif worker.is_stream and total_size_val == 0:
                self.download_table.setItem(row, 2, QTableWidgetItem(self.tr("Streaming")))

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

            if (dl_item.get('is_stream', False) and dl_item["status"] in [STATUS_QUEUED, STATUS_ERROR,
                                                                          STATUS_INCOMPLETE, STATUS_PAUSED]) or \
                    (not dl_item.get('is_stream', False) and dl_item["status"] in resumable_statuses):
                logger.debug(f"[AutoStart] Starting new worker for ID {dl_id} from queue.")

                self._start_download_worker(dl_id)

                active_in_this_queue += 1

    @Slot()
    def handle_resume_selected(self):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return
        self.handle_resume_download_id_logic(dl_id)

    def handle_resume_download_id_logic(self, download_id: int):
        dl_data = self.db.get_download_by_id(download_id)
        if not dl_data: return

        worker_info = self.active_workers.get(download_id)
        is_stream = dl_data.get('is_stream', False)

        if worker_info and worker_info["worker"].isRunning():
            if not is_stream and worker_info["worker"]._is_paused:
                worker_info["worker"].resume()
            else:
                QMessageBox.information(self, self.tr("Cannot Resume"),
                                        self.tr(f"Download is currently active or cannot be resumed."))
        elif not worker_info:
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

        dl_data = self.db.get_download_by_id(dl_id)
        if not dl_data: return

        worker_info = self.active_workers.get(dl_id)

        if dl_data.get('is_stream', False):
            if worker_info and worker_info["worker"].isRunning():
                reply = QMessageBox.question(
                    self,
                    self.tr("Pause Stream Download"),
                    self.tr("Pausing will interrupt the download, and progress may be lost. "
                            "Resuming will restart it from the beginning.\n\n"
                            "Are you sure you want to pause?"),
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                if reply == QMessageBox.No:
                    return

                # For streams, "pausing" means stopping the worker cleanly.
                worker = worker_info["worker"]
                worker.cancel()

                # Wait for the worker thread to finish. This is crucial.
                if not worker.wait(3000):  # Wait 3 seconds
                    logger.warning(f"Stream worker {dl_id} did not terminate gracefully on pause. Forcing termination.")
                    worker.terminate()

                # The worker's status_changed signal should have removed it from active_workers upon cancellation.
                # We can ensure it's gone here.
                if dl_id in self.active_workers:
                    del self.active_workers[dl_id]

                self.db.update_status(dl_id, STATUS_PAUSED)
                row = self.download_id_to_row_map.get(dl_id)
                if row is not None and row < self.download_table.rowCount():
                    self.download_table.setItem(row, 3, QTableWidgetItem(self.tr(STATUS_PAUSED)))
                    pb = self.download_table.cellWidget(row, 4)
                    if isinstance(pb, QProgressBar):
                        pb.setProperty("status", STATUS_PAUSED)
                        self.style().unpolish(pb)
                        self.style().polish(pb)
                        pb.setRange(0, 100)  # Set to determinate
                        dl_progress = self.db.get_download_by_id(dl_id).get('progress', 0)
                        pb.setValue(dl_progress if dl_progress > 0 else 0)
                        pb.setFormat(self.tr("Paused"))
                self.update_action_buttons_state()
            return

        # Original logic for non-stream downloads
        if worker_info and worker_info["worker"].isRunning() and not worker_info["worker"]._is_paused:
            worker_info["worker"].pause()
        elif not worker_info:
            if dl_data and dl_data['status'] == STATUS_QUEUED:
                self.db.update_status(dl_id, STATUS_PAUSED)
                row = self.download_id_to_row_map.get(dl_id)
                if row is not None and row < self.download_table.rowCount():
                    self.download_table.setItem(row, 3, QTableWidgetItem(self.tr(STATUS_PAUSED)))
                    pb = self.download_table.cellWidget(row, 4)
                    if isinstance(pb, QProgressBar):
                        pb.setProperty("status", STATUS_PAUSED)
                        self.style().unpolish(pb)
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

        msg_box = QMessageBox(self)
        msg_box.setWindowTitle(self.tr("Confirm Removal"))
        msg_box.setText(self.tr(f"What would you like to do with '{dl_data['file_name']}'?"))
        msg_box.setIcon(QMessageBox.Question)
        btn_delete_all = msg_box.addButton(self.tr("Delete from Disk"), QMessageBox.DestructiveRole)
        btn_remove_only = msg_box.addButton(self.tr("Remove from List Only"), QMessageBox.ActionRole)
        btn_cancel = msg_box.addButton(self.tr("Cancel"), QMessageBox.RejectRole)
        msg_box.exec()
        clicked_button = msg_box.clickedButton()

        if clicked_button == btn_cancel:
            return

        should_delete_files = (clicked_button == btn_delete_all)

        worker_info = self.active_workers.pop(dl_id, None)
        if worker_info:
            worker = worker_info["worker"]
            logger.info(f"Stopping worker {dl_id} before deletion.")
            worker.cancel()
            if not worker.wait(3000):
                logger.warning(f"Worker {dl_id} did not terminate gracefully. Forcing termination.")
                worker.terminate()

        if should_delete_files:
            self._delete_associated_files(dl_data["save_path"], dl_data["file_name"])

        self.db.delete_download(dl_id)
        self._remove_row_from_table_by_id(dl_id)
        self.update_action_buttons_state()

    def _delete_associated_files(self, save_path_str: str, file_name_str: str):
        """A robust helper to delete all files associated with a download."""
        logger.info(f"Deleting files for save path: {save_path_str}")
        main_file_path = Path(save_path_str)
        final_filename = Path(file_name_str)
        save_dir = main_file_path.parent

        if main_file_path.is_dir():
            main_file_path = save_dir / final_filename

        files_to_delete = {
            main_file_path,
            Path(str(main_file_path) + ".resume"),
            Path(str(main_file_path) + ".ytdl"),
            Path(str(main_file_path) + ".part"),
        }

        try:
            for file_path in files_to_delete:
                if file_path.exists():
                    logger.debug(f"Deleting: {file_path}")
                    file_path.unlink()

            stem_base = main_file_path.stem
            for extra_file in main_file_path.parent.glob(stem_base + "*"):
                if extra_file.suffix in [".part", ".temp", ".ytdl", ".resume"] and extra_file.exists():
                    logger.debug(f"Cleaning up residual file: {extra_file}")
                    try:
                        extra_file.unlink()
                    except Exception as e:
                        logger.warning(f"Could not delete {extra_file}: {e}")

            logger.info(f"Successfully deleted associated files for {file_name_str}.")
        except OSError as e:
            logger.error(f"Error during file deletion for {file_name_str}: {e}")
            QMessageBox.warning(self, self.tr("File Deletion Error"),
                                self.tr(f"Could not delete file: {e.filename}\nError: {e.strerror}"))

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
                (d["status"] in [STATUS_QUEUED, STATUS_PAUSED, STATUS_ERROR, STATUS_INCOMPLETE] and not d.get(
                    'is_stream', False)) or
                (d.get('is_stream', False) and d["status"] in [STATUS_QUEUED, STATUS_ERROR, STATUS_INCOMPLETE, STATUS_PAUSED])
                for d in queue_downloads
            )
            has_active = any(
                d["status"] in [STATUS_DOWNLOADING, STATUS_YTDLP]
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
                # Only pause if it's a non-stream download, otherwise cancel (streams can't be paused)
                if not dl_entry.get('is_stream', False) and worker_info_iter["worker"].isRunning() and not \
                worker_info_iter["worker"]._is_paused:
                    worker_info_iter["worker"].pause()
                elif dl_entry.get('is_stream', False) and worker_info_iter["worker"].isRunning():
                    worker_info_iter["worker"].cancel()  # For streams, stopping means cancelling
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

    @Slot(dict)
    def handle_link_received(self, download_info: dict):
        """
        This is the central handler for all incoming downloads.
        It now ensures the dialog window itself is brought to the front.
        """

        if self.isMinimized():
            self.showNormal()
        self.raise_()
        self.activateWindow()

        # Extract data from the incoming link info
        url = download_info.get("url")
        cookies = download_info.get("cookies")
        referrer = download_info.get("referrer")
        user_agent = download_info.get("userAgent")
        metadata = None
        auth_tuple = None

        # If no URL was passed (e.g., from the "Add URL" button), show the dialog to get one.
        if not url:
            url_dialog = UrlInputDialog(self)
            if url_dialog.exec() != QDialog.Accepted:
                return
            url, metadata, auth_tuple = url_dialog.get_url_and_metadata()
            custom_headers = None  # No custom headers for manual adds
        else:
            # For extension links, build the headers from the received info.
            custom_headers = {}
            if cookies: custom_headers["Cookie"] = cookies
            if referrer: custom_headers["Referer"] = referrer
            if user_agent: custom_headers["User-Agent"] = user_agent

        if not url:
            QMessageBox.warning(self, self.tr("Error"), self.tr("A valid URL is required."))
            return

        # Create and prepare the dialog
        new_dl_dialog = NewDownloadDialog(self.settings, self.db, self)
        new_dl_dialog.set_initial_data(url, metadata, auth_tuple)
        new_dl_dialog.fetch_metadata_async(url, auth_tuple, custom_headers)
        new_dl_dialog.setWindowFlags(new_dl_dialog.windowFlags() | Qt.WindowStaysOnTopHint)
        new_dl_dialog.show()
        new_dl_dialog.raise_()
        new_dl_dialog.activateWindow()

        if new_dl_dialog.result() == QDialog.Accepted or new_dl_dialog.exec() == QDialog.Accepted:
            added_dl_id, accepted_how = new_dl_dialog.get_final_download_data()
            if added_dl_id:
                db_payload = self.db.get_download_by_id(added_dl_id)
                if not db_payload:
                    logger.error(f"Could not retrieve newly added download {added_dl_id}")
                    return
                self._add_download_to_table(db_payload)
                self.download_table.scrollToBottom()
                if db_payload.get("status") == STATUS_CONNECTING:
                    self._start_download_worker(added_dl_id)
                elif accepted_how == "now" and db_payload.get("queue_id") is not None:
                    self.try_auto_start_from_queue(db_payload["queue_id"])

        self.update_action_buttons_state()

    @Slot()
    def handle_new_download_dialog(self):
        """Handles the 'Add URL' button by calling the main link handler with empty data."""
        self.handle_link_received({})

    @Slot(QPoint)
    def show_download_context_menu(self, pos: QPoint):
        dl_id = self._get_selected_download_id()
        if dl_id is None: return

        dl_data = self.db.get_download_by_id(dl_id)
        if not dl_data: return

        menu = QMenu(self)
        is_stream = dl_data.get('is_stream', False)

        # Resume/Pause actions
        if is_stream:
            resume_stream_action = QAction(self.tr("Retry/Start Stream"), self)
            resume_stream_action.triggered.connect(self.handle_resume_selected)
            resume_stream_action.setEnabled(self.resume_action.isEnabled())
            menu.addAction(resume_stream_action)
        else:
            menu.addAction(self.resume_action)

        menu.addAction(self.pause_action)
        menu.addSeparator()

        redownload_action = QAction(self.tr("Redownload"), self)
        redownload_action.triggered.connect(self.handle_resume_selected)

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
                # For non-stream, set to connecting and try to resume
                if not dl_data.get('is_stream', False):
                    self.db.update_status(download_id, STATUS_CONNECTING)
                    self.handle_resume_download_id_logic(download_id)
                else:  # For streams, just update status and let user manually start if needed
                    self.db.update_status(download_id, STATUS_QUEUED)

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
            dl_data = self.db.get_download_by_id(dl_id_iter)
            if dl_data:
                if not dl_data.get('is_stream', False) and worker_info_iter["worker"].isRunning() and not \
                worker_info_iter["worker"]._is_paused:
                    worker_info_iter["worker"].pause()
                elif dl_data.get('is_stream', False) and worker_info_iter["worker"].isRunning():
                    worker_info_iter["worker"].cancel()  # For streams, stopping means cancelling
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
        self.start_queue_action.setIcon(
            icon(get_asset_path("assets/icons/player-track-next.svg"), self.start_queue_action.isEnabled()))
        self.stop_queue_action.setIcon(
            icon(get_asset_path("assets/icons/copy-x.svg"), self.stop_queue_action.isEnabled()))
        self.remove_entry_action.setIcon(
            icon(get_asset_path("assets/icons/trash-x.svg"), self.remove_entry_action.isEnabled()))
        self.cancel_dl_action.setIcon(
            icon(get_asset_path("assets/icons/cancel.svg"), self.cancel_dl_action.isEnabled()))
        self.stop_all_action.setIcon(
            icon(get_asset_path("assets/icons/hand-stop.svg"), self.stop_all_action.isEnabled()))
        self.report_bug_action.setIcon(icon(get_asset_path("assets/icons/bug.svg"), self.report_bug_action.isEnabled()))
        self.donate_action.setIcon(icon(get_asset_path("assets/icons/donate.svg"), self.donate_action.isEnabled()))

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
        about_dialog.setMinimumWidth(460)

        layout = QVBoxLayout(about_dialog)

        # Description
        description = QLabel(self.tr(
            f"<h2 align='center'>Python Internet Download Manager (PIDM)</h2>"
            f"<p align='center'><b>Version:</b> {self.app_version}</p>"
            "<p>PIDM is a modern, open-source internet download manager built with Python and PySide6.</p>"
            "<p>It features a smart and theme-aware interface, supports stream downloads, "
            "browser integration, scheduling, multilingual UI, speed limits, and more.</p>"
            "<p align='center'>"
            "🔗 <a href='https://github.com/saeedmasoudie/PIDM'>GitHub Repository</a><br>"
            "🔗 <a href='https://github.com/saeedmasoudie/PIDM-ext'>Browser Extension</a><br>"
            "🔗 <a href='https://github.com/saeedmasoudie/PIDM/releases'>Latest Releases</a>"
            "</p>"
        ))
        description.setWordWrap(True)
        description.setOpenExternalLinks(True)
        layout.addWidget(description)

        # Footer
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
                if worker_info and worker_info["worker"].isRunning():
                    # For streams, cancel instead of pausing
                    if dl_id_iter in self.db.get_download_by_id(dl_id_iter) and self.db.get_download_by_id(
                            dl_id_iter).get('is_stream', False):
                        worker_info["worker"].cancel()
                    elif not worker_info["worker"]._is_paused:
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
        STATUS_INCOMPLETE: QCoreApplication.translate("Status", "Incomplete"),
        STATUS_YTDLP: QCoreApplication.translate("Status", "Streaming")  # New translation for yt_dlp status
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
        "final_url": url,
        "is_stream": False  # Default to False, MetadataFetcher will update
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

    if current_lang != "en" and translator.load(QLocale(current_lang), f"pidm_{current_lang}.qm", "_",
                                                str(translation_dir)):
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

    win.show()
    sys.exit(app.exec())
