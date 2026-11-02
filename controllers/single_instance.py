from __future__ import annotations

from PySide6.QtCore import QObject, Signal
from PySide6.QtNetwork import QLocalServer, QLocalSocket


class SingleInstance(QObject):
    activationRequested = Signal()

    def __init__(self, server_name: str, parent=None):
        super().__init__(parent)
        self._server_name = server_name
        self._server = QLocalServer(self)
        self._server.newConnection.connect(self._receive_activation)

    def claim(self) -> bool:
        socket = QLocalSocket()
        socket.connectToServer(self._server_name)
        if socket.waitForConnected(250):
            socket.write(b"activate")
            socket.waitForBytesWritten(250)
            socket.disconnectFromServer()
            return False
        QLocalServer.removeServer(self._server_name)
        return self._server.listen(self._server_name)

    def _receive_activation(self) -> None:
        while self._server.hasPendingConnections():
            socket = self._server.nextPendingConnection()
            socket.waitForReadyRead(50)
            socket.deleteLater()
        self.activationRequested.emit()
