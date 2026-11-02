import QtQuick
import QtQuick.Controls
import QtQuick.Dialogs
import QtQuick.Layouts
import QtQuick.Window
import PIDM 1.0
import "components"

Window {
    id: detailsWindow

    property int downloadId: -1
    property var details: ({})
    property bool optionsInitialized: false
    property string selectedDirectory: ""
    property int selectedConnections: 0
    property string message: ""

    width: 840
    height: 760
    minimumWidth: 720
    minimumHeight: 620
    visible: true
    color: "transparent"
    flags: Qt.Window | Qt.FramelessWindowHint
    title: details.name || tr("downloadDetails")
    LayoutMirroring.enabled: I18n.rtl
    LayoutMirroring.childrenInherit: true
    property bool allowImmediateClose: false
    property string pendingWindowAction: ""
    onClosing: close => {
        if (!allowImmediateClose) {
            close.accepted = false
            animateWindowAction("close")
        }
    }

    function animateWindowAction(action) {
        if (detailsExitAnimation.running)
            return
        pendingWindowAction = action
        detailsExitAnimation.restart()
    }

    function toggleMaximized() {
        if (visibility === Window.Maximized)
            showNormal()
        else
            showMaximized()
    }

    function tr(key) {
        const language = I18n.language
        return language.length ? I18n.t(key) : key
    }

    function displayQueueName(name) {
        return name === "Main Queue" ? tr("mainQueue") : name
    }

    function statusLabel(status) {
        const keys = {
            "queued": "statusQueued",
            "downloading": "statusDownloading",
            "pausing": "pausing",
            "paused": "paused",
            "complete": "statusComplete",
            "error": "statusError"
        }
        return tr(keys[status] || status)
    }

    function refresh() {
        const next = downloadsModel.detailsById(downloadId)
        if (!next || next.id === undefined) {
            close()
            return
        }
        details = next
        if (!optionsInitialized) {
            selectedDirectory = next.directory || ""
            speedLimitField.text = (next.speedLimitValue || 0).toString()
            selectedConnections = next.connectionLimitValue || 0
            optionsInitialized = true
        }
    }

    Component.onCompleted: refresh()

    Connections {
        target: downloadsModel
        function onErrorOccurred(text) {
            detailsWindow.message = text
            messageToast.visible = true
            messageTimer.restart()
        }
    }

    Connections {
        target: browserBridge
        function onRefreshLinkReceived(downloadId, url) {
            if (downloadId !== detailsWindow.downloadId)
                return
            detailsWindow.optionsInitialized = false
            detailsWindow.refresh()
            detailsWindow.message = detailsWindow.tr("refreshLinkUpdated")
            messageToast.visible = true
            messageTimer.restart()
        }
    }

    Timer {
        interval: 350
        running: detailsWindow.visible
        repeat: true
        onTriggered: detailsWindow.refresh()
    }

    Timer {
        id: messageTimer
        interval: 3500
        onTriggered: messageToast.visible = false
    }

    FolderDialog {
        id: detailFolderDialog
        title: detailsWindow.tr("chooseFolder")
        onAccepted: detailsWindow.selectedDirectory = selectedFolder.toString()
    }

    ParallelAnimation {
        id: detailsExitAnimation
        NumberAnimation {
            target: detailsShell
            property: "opacity"
            to: 0
            duration: 140
            easing.type: Easing.InCubic
        }
        NumberAnimation {
            target: detailsShell
            property: "scale"
            to: 0.975
            duration: 165
            easing.type: Easing.InCubic
        }
        onFinished: {
            const action = detailsWindow.pendingWindowAction
            if (action === "minimize") {
                detailsWindow.showMinimized()
                detailsShell.opacity = 1
                detailsShell.scale = 1
            } else {
                detailsWindow.allowImmediateClose = true
                detailsWindow.close()
            }
            detailsWindow.pendingWindowAction = ""
        }
    }

    Rectangle {
        id: detailsShell
        anchors.fill: parent
        radius: detailsWindow.visibility === Window.Maximized ? 0 : 15
        color: theme.background
        border.width: detailsWindow.visibility === Window.Maximized ? 0 : 1
        border.color: theme.line
        clip: true

        Rectangle {
            id: titleBar
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.top: parent.top
            height: 56
            color: theme.sidebar
            LayoutMirroring.enabled: false
            LayoutMirroring.childrenInherit: true

            DragHandler {
                target: null
                onActiveChanged: if (active) detailsWindow.startSystemMove()
            }

            TapHandler {
                acceptedButtons: Qt.LeftButton
                onDoubleTapped: detailsWindow.toggleMaximized()
            }

            RowLayout {
                anchors.fill: parent
                anchors.leftMargin: 18
                anchors.rightMargin: 8
                spacing: 10
                Rectangle {
                    Layout.preferredWidth: 30
                    Layout.preferredHeight: 30
                    radius: 9
                    color: appLogoUrl.length > 0 ? "transparent" : theme.accentSoft
                    Image {
                        visible: appLogoUrl.length > 0
                        anchors.fill: parent
                        anchors.margins: 2
                        source: appLogoUrl
                        fillMode: Image.PreserveAspectFit
                        smooth: true
                        mipmap: true
                    }
                    UiIcon {
                        visible: appLogoUrl.length === 0
                        anchors.centerIn: parent
                        name: "download"
                        color: theme.accent
                        width: 17
                        height: 17
                    }
                }
                AppText {
                    Layout.fillWidth: true
                    text: detailsWindow.tr("downloadDetails")
                    color: theme.text
                    font.pixelSize: 12
                    font.weight: Font.DemiBold
                }
                Rectangle {
                    Layout.preferredWidth: detailsWindowControls.implicitWidth + 8
                    Layout.preferredHeight: 38
                    radius: 10
                    color: theme.surface
                    border.width: 1
                    border.color: theme.line

                    Row {
                        id: detailsWindowControls
                        anchors.centerIn: parent
                        spacing: 1

                        WindowButton {
                            iconName: "minus"
                            onClicked: detailsWindow.animateWindowAction("minimize")
                        }
                        WindowButton {
                            iconName: detailsWindow.visibility === Window.Maximized
                                ? "restore" : "maximize"
                            onClicked: detailsWindow.toggleMaximized()
                        }
                        WindowButton {
                            iconName: "close"
                            closeButton: true
                            onClicked: detailsWindow.animateWindowAction("close")
                        }
                    }
                }
            }
        }

        Flickable {
            id: scroller
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.top: titleBar.bottom
            anchors.bottom: parent.bottom
            contentWidth: width
            contentHeight: contentColumn.implicitHeight + 48
            clip: true
            boundsBehavior: Flickable.StopAtBounds

            ScrollBar.vertical: ScrollBar {
                width: 5
                contentItem: Rectangle { radius: 3; color: theme.subtle }
                background: Item {}
            }

            ColumnLayout {
                id: contentColumn
                width: scroller.width - 48
                x: 24
                y: 22
                spacing: 14

                Rectangle {
                    Layout.fillWidth: true
                    Layout.preferredHeight: 92
                    radius: 14
                    color: theme.surface
                    border.width: 1
                    border.color: theme.line

                    RowLayout {
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 13
                        Rectangle {
                            Layout.preferredWidth: 48
                            Layout.preferredHeight: 52
                            radius: 12
                            color: theme.accentSoft
                            AppText {
                                anchors.centerIn: parent
                                text: details.fileType || detailsWindow.tr("file")
                                color: theme.accent
                                font.pixelSize: 9
                                font.weight: Font.Bold
                            }
                        }
                        ColumnLayout {
                            Layout.fillWidth: true
                            spacing: 4
                            AppText {
                                Layout.fillWidth: true
                                text: details.name || ""
                                color: theme.text
                                font.pixelSize: 16
                                font.weight: Font.Bold
                                elide: Text.ElideMiddle
                            }
                            AppText {
                                Layout.fillWidth: true
                                text: details.source || ""
                                color: theme.muted
                                font.pixelSize: 9
                                elide: Text.ElideRight
                            }
                            RowLayout {
                                spacing: 6
                                Repeater {
                                    model: [
                                        details.protocol || "HTTP",
                                        detailsWindow.displayQueueName(details.queue || "—")
                                    ]
                                    delegate: Rectangle {
                                        required property string modelData
                                        width: chipText.implicitWidth + 14
                                        height: 21
                                        radius: 7
                                        color: theme.surfaceRaised
                                        AppText {
                                            id: chipText
                                            anchors.centerIn: parent
                                            text: modelData
                                            color: theme.muted
                                            font.pixelSize: 8
                                            font.weight: Font.DemiBold
                                        }
                                    }
                                }
                            }
                        }
                        Rectangle {
                            Layout.preferredWidth: statusText.implicitWidth + 22
                            Layout.preferredHeight: 30
                            radius: 15
                            color: details.status === "complete"
                                ? theme.accentSoft : theme.surfaceRaised
                            border.width: 1
                            border.color: details.status === "error"
                                ? theme.danger
                                : details.status === "complete" ? theme.success : theme.line
                            AppText {
                                id: statusText
                                anchors.centerIn: parent
                                text: detailsWindow.statusLabel(details.status || "")
                                color: details.status === "error" ? theme.danger
                                    : details.status === "complete" ? theme.success : theme.accent
                                font.pixelSize: 8
                                font.weight: Font.Bold
                                font.letterSpacing: 0.6
                            }
                        }
                    }
                }

                Rectangle {
                    Layout.fillWidth: true
                    Layout.preferredHeight: 88
                    radius: 14
                    color: theme.surface
                    border.width: 1
                    border.color: theme.line
                    ColumnLayout {
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 9
                        RowLayout {
                            Layout.fillWidth: true
                            AppText {
                                text: detailsWindow.tr("progress")
                                color: theme.muted
                                font.pixelSize: 10
                                font.weight: Font.DemiBold
                            }
                            Item { Layout.fillWidth: true }
                            AppText {
                                text: (details.progress || 0) + "%"
                                color: theme.text
                                font.pixelSize: 14
                                font.weight: Font.Bold
                            }
                        }
                        Rectangle {
                            Layout.fillWidth: true
                            Layout.preferredHeight: 7
                            radius: 4
                            color: theme.line
                            Rectangle {
                                width: parent.width * (details.progress || 0) / 100
                                x: I18n.rtl ? parent.width - width : 0
                                height: parent.height
                                radius: 4
                                color: details.status === "complete" ? theme.success : theme.accent
                                Behavior on width { NumberAnimation { duration: 180 } }
                            }
                        }
                    }
                }

                GridLayout {
                    Layout.fillWidth: true
                    columns: 3
                    columnSpacing: 10
                    rowSpacing: 10
                    Repeater {
                        model: [
                            [detailsWindow.tr("downloaded"), details.downloaded || "0 B"],
                            [detailsWindow.tr("remaining"), details.remaining || "—"],
                            [detailsWindow.tr("totalSize"), details.total || "—"],
                            [detailsWindow.tr("speed"), details.speed || "—"],
                            [detailsWindow.tr("eta"), details.eta || "—"],
                            [detailsWindow.tr("connections"), (details.connections || 1).toString()]
                        ]
                        delegate: Rectangle {
                            required property var modelData
                            Layout.fillWidth: true
                            Layout.preferredHeight: 66
                            radius: 11
                            color: theme.surface
                            border.width: 1
                            border.color: theme.line
                            Column {
                                anchors.centerIn: parent
                                width: parent.width - 16
                                spacing: 5
                                AppText {
                                    width: parent.width
                                    horizontalAlignment: Text.AlignHCenter
                                    text: modelData[0].toUpperCase()
                                    color: theme.subtle
                                    font.pixelSize: 8
                                    font.weight: Font.DemiBold
                                    elide: Text.ElideRight
                                }
                                AppText {
                                    width: parent.width
                                    horizontalAlignment: Text.AlignHCenter
                                    text: modelData[1]
                                    color: theme.text
                                    font.pixelSize: 11
                                    font.weight: Font.DemiBold
                                    elide: Text.ElideRight
                                }
                            }
                        }
                    }
                }

                Rectangle {
                    Layout.fillWidth: true
                    Layout.preferredHeight: informationContent.implicitHeight + 32
                    radius: 14
                    color: theme.surface
                    border.width: 1
                    border.color: theme.line
                    ColumnLayout {
                        id: informationContent
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 12
                        AppText {
                            text: detailsWindow.tr("fileInformation")
                            color: theme.text
                            font.pixelSize: 13
                            font.weight: Font.Bold
                        }
                        GridLayout {
                            Layout.fillWidth: true
                            columns: 2
                            columnSpacing: 12
                            rowSpacing: 10
                            Repeater {
                                model: [
                                    [detailsWindow.tr("sourceUrl"), details.url || ""],
                                    [detailsWindow.tr("savePath"), details.destination || ""],
                                    [detailsWindow.tr("downloadEngine"), details.connectionMode || "—"],
                                    [detailsWindow.tr("speedLimit"), details.speedLimit || "—"],
                                    [detailsWindow.tr("retryStatus"), details.retryState || "—"],
                                    [detailsWindow.tr("created"), details.createdAt || "—"],
                                    [detailsWindow.tr("finished"), details.finishedAt || "—"],
                                    [detailsWindow.tr("fileType"), details.fileType || "—"],
                                    [detailsWindow.tr("checksumStatus"), details.checksumStatus || "—"],
                                    [detailsWindow.tr("sha256Checksum"), details.checksumActual || details.checksumExpected || "—"],
                                    [detailsWindow.tr("proxyUrl"), details.proxyUrl || "—"],
                                    [detailsWindow.tr("interfaceAddress"), details.interfaceAddress || "—"]
                                ]
                                delegate: Rectangle {
                                    required property var modelData
                                    Layout.fillWidth: true
                                    Layout.preferredHeight: 56
                                    radius: 10
                                    color: theme.background
                                    border.width: 1
                                    border.color: theme.line
                                    Column {
                                        anchors.fill: parent
                                        anchors.leftMargin: 11
                                        anchors.rightMargin: 11
                                        anchors.topMargin: 8
                                        anchors.bottomMargin: 8
                                        spacing: 4
                                        AppText {
                                            width: parent.width
                                            text: modelData[0]
                                            color: theme.subtle
                                            font.pixelSize: 9
                                            font.weight: Font.DemiBold
                                            elide: Text.ElideRight
                                        }
                                        AppText {
                                            width: parent.width
                                            text: modelData[1]
                                            color: theme.text
                                            font.pixelSize: 10
                                            font.weight: Font.Medium
                                            elide: Text.ElideMiddle
                                        }
                                    }
                                }
                            }
                        }
                        AppText {
                            visible: (details.error || "").length > 0
                            Layout.fillWidth: true
                            text: details.error || ""
                            color: theme.danger
                            font.pixelSize: 10
                            wrapMode: Text.WordWrap
                        }
                    }
                }

                Rectangle {
                    Layout.fillWidth: true
                    Layout.preferredHeight: 254
                    radius: 14
                    color: theme.surface
                    border.width: 1
                    border.color: theme.line
                    ColumnLayout {
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 10
                        RowLayout {
                            Layout.fillWidth: true
                            ColumnLayout {
                                Layout.fillWidth: true
                                spacing: 2
                                AppText {
                                    text: detailsWindow.tr("perDownloadOptions")
                                    color: theme.text
                                    font.pixelSize: 12
                                    font.weight: Font.DemiBold
                                }
                                AppText {
                                    text: detailsWindow.tr("perDownloadOptionsHelper")
                                    color: theme.muted
                                    font.pixelSize: 8
                                }
                            }
                            AppText {
                                visible: !details.canEditOptions
                                text: detailsWindow.tr("pauseToEdit")
                                color: theme.warning
                                font.pixelSize: 8
                                font.weight: Font.DemiBold
                            }
                        }
                        AppText {
                            text: detailsWindow.tr("downloadLocation")
                            color: theme.muted
                            font.pixelSize: 8
                        }
                        RowLayout {
                            Layout.fillWidth: true
                            AppTextField {
                                Layout.fillWidth: true
                                text: detailsWindow.selectedDirectory.toString()
                                    .replace("file:///", "")
                                readOnly: true
                                iconName: "folder"
                            }
                            AppButton {
                                text: detailsWindow.tr("browse")
                                iconName: "folder"
                                enabled: details.canEditOptions || false
                                onClicked: detailFolderDialog.open()
                            }
                        }
                        RowLayout {
                            Layout.fillWidth: true
                            spacing: 14
                            ColumnLayout {
                                Layout.fillWidth: true
                                AppText {
                                    text: detailsWindow.tr("specificSpeedLimit")
                                    color: theme.muted
                                    font.pixelSize: 8
                                }
                                AppTextField {
                                    id: speedLimitField
                                    Layout.fillWidth: true
                                    enabled: details.canEditOptions || false
                                    validator: IntValidator { bottom: 0; top: 10000000 }
                                    placeholderText: "0"
                                }
                            }
                            ColumnLayout {
                                Layout.fillWidth: true
                                AppText {
                                    text: detailsWindow.tr("specificConnections")
                                    color: theme.muted
                                    font.pixelSize: 8
                                }
                                RowLayout {
                                    Layout.fillWidth: true
                                    spacing: 5
                                    Repeater {
                                        model: [0, 1, 2, 4, 8]
                                        delegate: AppButton {
                                            required property int modelData
                                            Layout.fillWidth: true
                                            compact: false
                                            text: modelData === 0
                                                ? detailsWindow.tr("auto")
                                                : modelData.toString()
                                            tone: detailsWindow.selectedConnections === modelData
                                                ? "primary" : "neutral"
                                            enabled: details.canEditOptions || false
                                            onClicked: detailsWindow.selectedConnections = modelData
                                        }
                                    }
                                }
                            }
                        }
                        RowLayout {
                            Layout.fillWidth: true
                            AppText {
                                Layout.fillWidth: true
                                text: detailsWindow.tr("optionChangesHint")
                                color: theme.subtle
                                font.pixelSize: 8
                            }
                            AppButton {
                                text: detailsWindow.tr("applyOptions")
                                iconName: "check"
                                tone: "primary"
                                enabled: details.canEditOptions || false
                                onClicked: {
                                    if (downloadsModel.updateDownloadOptions(
                                        detailsWindow.downloadId,
                                        detailsWindow.selectedDirectory,
                                        parseInt(speedLimitField.text || "0"),
                                        detailsWindow.selectedConnections)) {
                                        detailsWindow.message = detailsWindow.tr("optionsSaved")
                                        messageToast.visible = true
                                        messageTimer.restart()
                                        detailsWindow.optionsInitialized = false
                                        detailsWindow.refresh()
                                    }
                                }
                            }
                        }
                    }
                }

                RowLayout {
                    Layout.fillWidth: true
                    Layout.bottomMargin: 6
                    AppButton {
                        visible: details.protocol !== "TORRENT"
                            && details.status !== "complete"
                        text: browserBridge.refreshWaiting
                            && browserBridge.refreshDownloadId === detailsWindow.downloadId
                            ? detailsWindow.tr("cancelRefreshLink")
                            : detailsWindow.tr("refreshDownloadLink")
                        iconName: "network"
                        tone: browserBridge.refreshWaiting
                            && browserBridge.refreshDownloadId === detailsWindow.downloadId
                            ? "primary" : "neutral"
                        enabled: details.status !== "downloading"
                            && details.status !== "pausing"
                        onClicked: {
                            if (browserBridge.refreshWaiting
                                && browserBridge.refreshDownloadId
                                    === detailsWindow.downloadId) {
                                browserBridge.cancelRefresh(detailsWindow.downloadId)
                                messageToast.visible = false
                            } else {
                                browserBridge.waitForRefresh(detailsWindow.downloadId)
                                detailsWindow.message =
                                    detailsWindow.tr("refreshLinkWaiting")
                                messageToast.visible = true
                                messageTimer.stop()
                            }
                        }
                    }
                    AppButton {
                        text: detailsWindow.tr("copyUrl")
                        iconName: "external"
                        onClicked: downloadsModel.copyUrlById(detailsWindow.downloadId)
                    }
                    AppButton {
                        text: detailsWindow.tr("openFolder")
                        iconName: "folder"
                        onClicked: downloadsModel.openFolderById(detailsWindow.downloadId)
                    }
                    Item { Layout.fillWidth: true }
                    AppButton {
                        text: details.status === "complete"
                            ? detailsWindow.tr("open")
                            : details.status === "downloading"
                                ? detailsWindow.tr("pause") : detailsWindow.tr("resume")
                        iconName: details.status === "complete" ? "external"
                            : details.status === "downloading" ? "pause" : "play"
                        tone: "primary"
                        onClicked: details.status === "complete"
                            ? downloadsModel.openDownloadById(detailsWindow.downloadId)
                            : downloadsModel.togglePausedById(detailsWindow.downloadId)
                    }
                }
            }
        }

        Rectangle {
            id: messageToast
            visible: false
            anchors.horizontalCenter: parent.horizontalCenter
            anchors.bottom: parent.bottom
            anchors.bottomMargin: 18
            width: Math.min(560, parent.width - 40)
            height: 48
            radius: 11
            color: theme.surfaceRaised
            border.width: 1
            border.color: theme.accent
            z: 20
            AppText {
                anchors.centerIn: parent
                width: parent.width - 28
                text: detailsWindow.message
                color: theme.text
                font.pixelSize: 9
                horizontalAlignment: Text.AlignHCenter
                elide: Text.ElideRight
            }
        }
    }
}
