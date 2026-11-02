from __future__ import annotations

import re
from datetime import datetime

from PySide6.QtCore import (
    QAbstractListModel,
    QModelIndex,
    Property,
    QTimer,
    Qt,
    Signal,
    Slot,
)

from core.database import DownloadDatabase
from models.download_model import DownloadModel

DAY_KEYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


class QueueModel(QAbstractListModel):
    IdRole = Qt.UserRole + 1
    NameRole = Qt.UserRole + 2
    DownloadCountRole = Qt.UserRole + 3
    ActiveCountRole = Qt.UserRole + 4
    WaitingCountRole = Qt.UserRole + 5
    EnabledRole = Qt.UserRole + 6
    StartTimeRole = Qt.UserRole + 7
    StopTimeRole = Qt.UserRole + 8
    DaysRole = Qt.UserRole + 9
    MaxConcurrentRole = Qt.UserRole + 10
    RunningRole = Qt.UserRole + 11
    PauseBetweenRole = Qt.UserRole + 12
    PostActionRole = Qt.UserRole + 13
    ErrorCountRole = Qt.UserRole + 14

    changed = Signal()
    errorOccurred = Signal(str)
    postActionRequested = Signal(str, str)

    def __init__(
        self,
        database: DownloadDatabase,
        downloads: DownloadModel,
        parent=None,
    ):
        super().__init__(parent)
        self._database = database
        self._downloads = downloads
        self._items = self._database.queues()
        self._last_triggers: dict[tuple[int, str], str] = {}
        self._revision = 0
        self._downloads.statsChanged.connect(self.refresh)
        self._downloads.queueActivityChanged.connect(self._on_queue_activity)
        self._downloads.queueCompleted.connect(self._on_queue_completed)
        self._timer = QTimer(self)
        self._timer.setInterval(15_000)
        self._timer.timeout.connect(self.checkSchedules)
        self._timer.start()
        QTimer.singleShot(1000, self.checkSchedules)

    def roleNames(self):
        return {
            self.IdRole: b"queueId",
            self.NameRole: b"name",
            self.DownloadCountRole: b"downloadCount",
            self.ActiveCountRole: b"activeCount",
            self.WaitingCountRole: b"waitingCount",
            self.EnabledRole: b"scheduleEnabled",
            self.StartTimeRole: b"startTime",
            self.StopTimeRole: b"stopTime",
            self.DaysRole: b"daysText",
            self.MaxConcurrentRole: b"maxConcurrent",
            self.RunningRole: b"running",
            self.PauseBetweenRole: b"pauseBetween",
            self.PostActionRole: b"postAction",
            self.ErrorCountRole: b"errorCount",
        }

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._items)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not 0 <= index.row() < len(self._items):
            return None
        item = self._items[index.row()]
        values = {
            self.IdRole: item["id"],
            self.NameRole: item["name"],
            self.DownloadCountRole: item.get("download_count", 0) or 0,
            self.ActiveCountRole: item.get("active_count", 0) or 0,
            self.WaitingCountRole: item.get("waiting_count", 0) or 0,
            self.EnabledRole: bool(item.get("enabled")),
            self.StartTimeRole: item.get("start_time", ""),
            self.StopTimeRole: item.get("stop_time", ""),
            self.DaysRole: ", ".join(item.get("days", [])) or "Every day",
            self.MaxConcurrentRole: item.get("max_concurrent", 1),
            self.RunningRole: bool(item.get("running")),
            self.PauseBetweenRole: item.get("pause_between", 0),
            self.PostActionRole: item.get("post_action", "none"),
            self.ErrorCountRole: item.get("error_count", 0) or 0,
        }
        return values.get(role)

    @Property(int, notify=changed)
    def count(self):
        return len(self._items)

    @Property(int, notify=changed)
    def firstQueueId(self):
        return int(self._items[0]["id"]) if self._items else -1

    @Property(int, notify=changed)
    def revision(self):
        return self._revision

    @Slot()
    def refresh(self) -> None:
        self.beginResetModel()
        self._items = self._database.queues()
        self.endResetModel()
        self._revision += 1
        self.changed.emit()

    @Slot(int, result="QVariantMap")
    def details(self, row: int) -> dict:
        if not 0 <= row < len(self._items):
            return {}
        item = self._items[row]
        return {
            "id": item["id"],
            "name": item["name"],
            "enabled": bool(item.get("enabled")),
            "startTime": item.get("start_time", ""),
            "stopTime": item.get("stop_time", ""),
            "days": ",".join(item.get("days", [])),
            "maxConcurrent": item.get("max_concurrent", 1),
            "pauseBetween": item.get("pause_between", 0),
            "postAction": item.get("post_action", "none"),
        }

    @Slot(int, str, bool, str, str, str, int, int, str, result=bool)
    def saveQueue(
        self,
        row: int,
        name: str,
        enabled: bool,
        start_time: str,
        stop_time: str,
        days_text: str,
        max_concurrent: int,
        pause_between: int,
        post_action: str,
    ) -> bool:
        name = name.strip()
        start_time = start_time.strip()
        stop_time = stop_time.strip()
        if not name:
            self.errorOccurred.emit("Queue name cannot be empty.")
            return False
        time_pattern = r"^(?:[01]\d|2[0-3]):[0-5]\d$"
        if enabled and not re.fullmatch(time_pattern, start_time):
            self.errorOccurred.emit("Start time must use the 24-hour HH:mm format.")
            return False
        if stop_time and not re.fullmatch(time_pattern, stop_time):
            self.errorOccurred.emit("Stop time must use the 24-hour HH:mm format.")
            return False
        raw_days = [value.strip() for value in days_text.split(",") if value.strip()]
        if len(raw_days) == 1 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_days[0]):
            try:
                datetime.strptime(raw_days[0], "%Y-%m-%d")
            except ValueError:
                self.errorOccurred.emit("The one-time schedule date is invalid.")
                return False
            days = raw_days
        else:
            days = [value.title()[:3] for value in raw_days]
            invalid_days = [day for day in days if day not in DAY_KEYS]
            if invalid_days:
                self.errorOccurred.emit(
                    "Days must use Mon, Tue, Wed, Thu, Fri, Sat, or Sun."
                )
                return False
        queue_id = self._items[row]["id"] if 0 <= row < len(self._items) else -1
        try:
            self._database.save_queue(
                queue_id,
                name=name,
                enabled=enabled,
                start_time=start_time if enabled else "",
                stop_time=stop_time if enabled else "",
                days=days,
                max_concurrent=max_concurrent,
                pause_between=pause_between,
                post_action=post_action,
            )
        except Exception as exc:
            self.errorOccurred.emit(f"Could not save queue: {exc}")
            return False
        self.refresh()
        return True

    @Slot(int)
    def startQueue(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        item = self._items[row]
        self._database.set_queue_running(item["id"], True)
        self._downloads.startQueue(
            item["id"],
            item.get("max_concurrent", 1),
            item.get("pause_between", 0),
        )
        self.refresh()

    @Slot(int)
    def stopQueue(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        queue_id = self._items[row]["id"]
        self._database.set_queue_running(queue_id, False)
        self._downloads.stopQueue(queue_id)
        self.refresh()

    @Slot(int)
    def retryQueue(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        item = self._items[row]
        self._downloads.retryQueueErrors(item["id"])
        self.startQueue(row)

    @Slot(int)
    def deleteQueue(self, row: int) -> None:
        if not 0 <= row < len(self._items):
            return
        try:
            self._database.delete_queue(self._items[row]["id"])
        except ValueError as exc:
            self.errorOccurred.emit(str(exc))
            return
        self.refresh()

    @Slot(int, result="QVariantList")
    def downloadsForQueue(self, queue_id: int) -> list[dict]:
        return self._database.downloads_for_queue(queue_id)

    @Slot(int, int, int)
    def moveDownload(self, queue_id: int, download_id: int, direction: int) -> None:
        self._database.move_queue_download(queue_id, download_id, direction)
        self.refresh()

    @Slot(int)
    def removeDownload(self, download_id: int) -> None:
        self._database.remove_from_queue(download_id)
        self.refresh()

    @Slot()
    def checkSchedules(self) -> None:
        now = datetime.now()
        today = now.strftime("%a")
        current_time = now.strftime("%H:%M")
        current_date = now.strftime("%Y-%m-%d")
        for item in self._database.queues():
            if not item.get("enabled"):
                continue
            days = item.get("days", [])
            if days:
                if (
                    len(days) == 1
                    and re.fullmatch(r"\d{4}-\d{2}-\d{2}", days[0])
                ):
                    if current_date != days[0]:
                        continue
                elif today not in days:
                    continue
            queue_id = item["id"]
            if item.get("start_time") == current_time:
                key = (queue_id, "start")
                if self._last_triggers.get(key) != current_date:
                    row = self._row_for_id(queue_id)
                    self.startQueue(row)
                    self._last_triggers[key] = current_date
            if item.get("stop_time") == current_time:
                key = (queue_id, "stop")
                if self._last_triggers.get(key) != current_date:
                    row = self._row_for_id(queue_id)
                    self.stopQueue(row)
                    self._last_triggers[key] = current_date

    def _row_for_id(self, queue_id: int) -> int:
        return next(
            (row for row, item in enumerate(self._items) if item["id"] == queue_id),
            -1,
        )

    @Slot(int, bool)
    def _on_queue_activity(self, queue_id: int, running: bool) -> None:
        self._database.set_queue_running(queue_id, running)
        self.refresh()

    @Slot(int)
    def _on_queue_completed(self, queue_id: int) -> None:
        item = self._database.queue(queue_id)
        if not item:
            return
        self._database.set_queue_running(queue_id, False)
        self.refresh()
        action = item.get("post_action", "none")
        if action != "none":
            self.postActionRequested.emit(action, item["name"])
