import QtQuick
import QtQuick.Controls
import QtQuick.Dialogs
import QtQuick.Layouts
import QtQuick.Window
import PIDM 1.0
import "components"

ApplicationWindow {
    id: window

    visible: true
    width: 1320
    height: 820
    minimumWidth: 860
    minimumHeight: 620
    title: "PIDM"
    font.family: I18n.fontFamily
    color: "transparent"
    flags: Qt.Window | Qt.FramelessWindowHint
    LayoutMirroring.enabled: I18n.rtl
    LayoutMirroring.childrenInherit: true
    onClosing: close => {
        if (!window.allowImmediateClose) {
            close.accepted = false
            window.requestClose()
        }
    }

    property bool compactSidebar: width < 1040
    property bool showOverview: true
    property int pageIndex: 0
    property string currentFilter: "all"
    property string selectedDirectory: downloadsModel.defaultDownloadPath
    property string errorMessage: ""
    property int selectedQueueId: queuesModel.firstQueueId
    property string selectedQueueName: "Main Queue"
    property bool startImmediately: true
    property bool showAdvancedRequest: false
    property bool resolvingMedia: false
    property bool resolvedMediaReady: false
    property string addDownloadMessage: ""
    property bool addDownloadMessageIsError: false
    property string mediaFormatTitle: ""
    property var mediaFormats: []
    property var selectedMediaFormat: ({})
    property string mediaInternalHeaders: ""
    property bool resolvingTorrent: false
    property bool resolvedTorrentReady: false
    property string torrentTitle: ""
    property var torrentFiles: []
    property var selectedTorrentFiles: []
    property var batchItems: []
    property string batchSourceTitle: ""
    property bool batchStartImmediately: false
    property string latestUpdateVersion: ""
    property string latestUpdateUrl: ""
    property string latestUpdateNotes: ""
    property double selectedExpectedSize: 0
    property var detailsWindowInstance: null
    property bool allowImmediateClose: false
    property bool windowTransitionBusy: false
    property string pendingWindowAction: ""

    function translate(key) {
        const currentLanguage = I18n.language
        return currentLanguage.length ? I18n.t(key) : key
    }

    function displayQueueName(name) {
        return name === "Main Queue" ? translate("mainQueue") : name
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
        return translate(keys[status] || status)
    }

    function linksFromText(text) {
        const matches = String(text || "").match(/(?:https?:\/\/[^\s]+|magnet:\?[^\s]+)/gi) || []
        let links = []
        for (let index = 0; index < matches.length; ++index) {
            const link = matches[index].replace(/[),.;]+$/, "")
            if (links.indexOf(link) < 0)
                links.push(link)
        }
        return links
    }

    function batchItemsFromLinks(links) {
        let items = []
        for (let index = 0; index < links.length; ++index) {
            const link = links[index]
            let title = link
            if (link.toLowerCase().startsWith("magnet:")) {
                title = translate("magnetLink")
            } else {
                const path = link.replace(/^https?:\/\//i, "")
                    .split(/[?#]/)[0].split("/").filter(Boolean)
                title = path.length > 1 ? path[path.length - 1] : path[0] || link
            }
            items.push({
                "url": link,
                "title": title,
                "kind": "link",
                "pageUrl": "",
                "headers": "",
                "expectedSize": 0,
                "selected": true
            })
        }
        return items
    }

    function openPastedLinks(text) {
        const links = linksFromText(text)
        if (links.length === 0) {
            presentMessage(translate("clipboardNoLinks"), true)
            return
        }
        if (links.length === 1) {
            urlField.text = links[0]
            selectedExpectedSize = 0
            selectedTorrentFiles = []
            resolvedMediaReady = false
            resolvedTorrentReady = false
            addPopup.open()
            return
        }
        batchItems = batchItemsFromLinks(links)
        batchSourceTitle = translate("pastedLinks")
        batchStartImmediately = false
        batchReviewPopup.open()
    }

    function openMultipleLinks() {
        multipleLinksField.text = clipboardMonitor.text()
        multipleLinksMessage.text = ""
        multipleLinksPopup.open()
    }

    function reviewMultipleLinks() {
        const links = linksFromText(multipleLinksField.text)
        if (links.length === 0) {
            multipleLinksMessage.text = translate("multipleLinksInvalid")
            return
        }
        batchItems = batchItemsFromLinks(links)
        batchSourceTitle = translate("pastedLinks")
        batchStartImmediately = false
        multipleLinksPopup.close()
        batchReviewPopup.open()
    }

    function editableFieldFocused() {
        const item = window.activeFocusItem
        return item && (item instanceof TextInput || item instanceof TextEdit)
    }

    function visibleForFilter(status, fileName) {
        const query = searchField.text.trim().toLowerCase()
        const matchesSearch = query.length === 0 || fileName.toLowerCase().includes(query)
        if (!matchesSearch)
            return false
        if (currentFilter === "active")
            return status === "downloading" || status === "pausing" || status === "queued"
        if (currentFilter === "completed")
            return status === "complete"
        if (currentFilter === "problems")
            return status === "paused" || status === "error"
        return true
    }

    function toggleMaximized() {
        if (visibility === Window.Maximized)
            showNormal()
        else
            showMaximized()
    }

    function revealWindow() {
        if (windowTransitionBusy)
            return
        shell.opacity = 0
        shell.scale = 0.985
        if (window.visibility === Window.Minimized)
            window.showNormal()
        else
            window.show()
        window.raise()
        window.requestActivate()
        windowEnterAnimation.restart()
    }

    function minimizeAnimated() {
        if (!windowTransitionBusy) {
            pendingWindowAction = "minimize"
            windowTransitionBusy = true
            windowExitAnimation.restart()
        }
    }

    function requestClose() {
        if (windowTransitionBusy)
            return
        if (trayController.shouldCloseToTray()) {
            pendingWindowAction = "hide"
            windowTransitionBusy = true
            windowExitAnimation.restart()
        } else {
            quitConfirmPopup.open()
        }
    }

    function confirmQuitAnimated() {
        quitConfirmPopup.close()
        allowImmediateClose = true
        if (detailsWindowInstance) {
            detailsWindowInstance.allowImmediateClose = true
            detailsWindowInstance.close()
        }
        pendingWindowAction = "quit"
        windowTransitionBusy = true
        windowExitAnimation.restart()
    }

    function openDetails(downloadId) {
        if (detailsWindowInstance) {
            detailsWindowInstance.allowImmediateClose = true
            detailsWindowInstance.close()
            detailsWindowInstance.destroy()
        }
        detailsWindowInstance = detailsWindowComponent.createObject(
            null, {"downloadId": downloadId}
        )
        detailsWindowInstance.show()
        detailsWindowInstance.raise()
        detailsWindowInstance.requestActivate()
    }

    function presentMessage(message, isError) {
        if (addPopup.opened) {
            addDownloadMessage = message
            addDownloadMessageIsError = isError
        } else {
            errorMessage = message
            errorToast.visible = true
            errorTimer.restart()
        }
    }

    function torrentSelectedCount() {
        let count = 0
        for (let index = 0; index < torrentFiles.length; ++index) {
            if (torrentFiles[index].selected)
                count++
        }
        return count
    }

    function setAllTorrentFiles(selected) {
        let updated = []
        for (let index = 0; index < torrentFiles.length; ++index) {
            const item = torrentFiles[index]
            updated.push({
                "index": item.index, "path": item.path, "name": item.name,
                "size": item.size, "sizeBytes": item.sizeBytes,
                "selected": selected
            })
        }
        torrentFiles = updated
    }

    function batchSelectedCount() {
        let count = 0
        for (let index = 0; index < batchItems.length; ++index) {
            if (batchItems[index].selected)
                count++
        }
        return count
    }

    function setBatchItemSelected(row, selected) {
        let updated = []
        for (let index = 0; index < batchItems.length; ++index) {
            const item = batchItems[index]
            updated.push({
                "url": item.url,
                "title": item.title,
                "kind": item.kind,
                "pageUrl": item.pageUrl,
                "headers": item.headers,
                "expectedSize": item.expectedSize || 0,
                "selected": index === row ? selected : item.selected
            })
        }
        batchItems = updated
    }

    function setAllBatchItems(selected) {
        let updated = []
        for (let index = 0; index < batchItems.length; ++index) {
            const item = batchItems[index]
            updated.push({
                "url": item.url,
                "title": item.title,
                "kind": item.kind,
                "pageUrl": item.pageUrl,
                "headers": item.headers,
                "expectedSize": item.expectedSize || 0,
                "selected": selected
            })
        }
        batchItems = updated
    }

    function removeBatchItem(row) {
        let updated = []
        for (let index = 0; index < batchItems.length; ++index) {
            if (index !== row)
                updated.push(batchItems[index])
        }
        batchItems = updated
        if (batchItems.length === 0)
            batchReviewPopup.close()
    }

    function importBatchItems() {
        let added = 0
        let attempted = 0
        let mediaPage = ""
        for (let index = 0; index < batchItems.length; ++index) {
            const item = batchItems[index]
            if (!item.selected)
                continue
            attempted++
            if (item.kind === "stream" && item.pageUrl.length > 0) {
                if (mediaPage.length === 0)
                    mediaPage = item.pageUrl
                continue
            }
            if (downloadsModel.addDownloadToQueue(
                item.url,
                selectedDirectory,
                selectedQueueId,
                batchStartImmediately,
                "",
                item.headers || "",
                "",
                "",
                [],
                item.expectedSize || 0
            ))
                added++
        }
        if (added > 0 || mediaPage.length > 0) {
            batchReviewPopup.close()
            batchItems = []
            if (mediaPage.length > 0) {
                urlField.text = mediaPage
                selectedExpectedSize = 0
                selectedTorrentFiles = []
                resolvedMediaReady = false
                resolvedTorrentReady = false
                addPopup.open()
            } else {
                presentMessage(
                    added + " " + translate("batchDownloadsAdded"),
                    added !== attempted
                )
            }
        }
    }

    function toggleTorrentFile(row) {
        let updated = []
        for (let index = 0; index < torrentFiles.length; ++index) {
            const item = torrentFiles[index]
            updated.push({
                "index": item.index, "path": item.path, "name": item.name,
                "size": item.size, "sizeBytes": item.sizeBytes,
                "selected": index === row ? !item.selected : item.selected
            })
        }
        torrentFiles = updated
    }

    Component {
        id: detailsWindowComponent
        DownloadDetails {}
    }

    Shortcut {
        sequence: "Ctrl+V"
        context: Qt.ApplicationShortcut
        enabled: !window.editableFieldFocused()
        onActivated: window.openPastedLinks(clipboardMonitor.text())
    }

    Connections {
        target: protocolCapabilities
        function onMediaResolved(title, url, headers) {
            urlField.text = url
            requestHeadersField.text = headers
            window.mediaInternalHeaders = ""
            window.showAdvancedRequest = true
            if (window.resolvingMedia) {
                window.resolvingMedia = false
                window.resolvedMediaReady = true
                startDownloadButton.clicked()
            }
        }
        function onMediaFormatsResolved(title, formats) {
            window.resolvingMedia = false
            window.mediaFormatTitle = title
            window.mediaFormats = formats
            window.selectedMediaFormat = formats.length > 0 ? formats[0] : ({})
            mediaFormatPopup.open()
        }
        function onTorrentFilesResolved(title, files) {
            window.resolvingTorrent = false
            window.torrentTitle = title
            window.torrentFiles = files
            torrentFilePopup.open()
        }
        function onErrorOccurred(message) {
            if (window.resolvingMedia)
                window.resolvedMediaReady = true
            if (window.resolvingTorrent)
                window.resolvedTorrentReady = true
            window.resolvingMedia = false
            window.resolvingTorrent = false
            window.presentMessage(message, true)
        }
    }

    Connections {
        target: clipboardMonitor
        function onUrlDetected(url) {
            urlField.text = url
            window.selectedExpectedSize = 0
            window.selectedTorrentFiles = []
            window.resolvedMediaReady = false
            window.resolvedTorrentReady = false
            if (!addPopup.opened)
                addPopup.open()
            window.show()
            window.raise()
            window.requestActivate()
        }
    }

    Connections {
        target: trayController
        function onShowRequested() {
            window.revealWindow()
        }
        function onAddDownloadRequested() {
            window.revealWindow()
            addPopup.open()
        }
        function onDownloadDetailsRequested(downloadId) {
            window.revealWindow()
            window.openDetails(downloadId)
        }
        function onQuitRequested() {
            window.revealWindow()
            quitConfirmPopup.open()
        }
    }

    Connections {
        target: browserBridge
        function onSingleUrlReceived(url) {
            urlField.text = url
            window.selectedExpectedSize = 0
            window.selectedTorrentFiles = []
            window.resolvedMediaReady = false
            window.resolvedTorrentReady = false
            if (!addPopup.opened)
                addPopup.open()
        }
        function onBatchReceived(items, sourceTitle) {
            window.batchItems = items
            window.batchSourceTitle = sourceTitle
            window.batchStartImmediately = false
            batchReviewPopup.open()
        }
    }

    Connections {
        target: updateChecker
        function onUpdateAvailable(version, url, notes) {
            window.latestUpdateVersion = version
            window.latestUpdateUrl = url
            window.latestUpdateNotes = notes
            updateAvailablePopup.open()
        }
    }

    Connections {
        target: downloadsModel
        function onErrorOccurred(message) {
            window.presentMessage(message, true)
        }
    }

    Connections {
        target: queuesModel
        function onErrorOccurred(message) {
            window.errorMessage = message
            errorToast.visible = true
            errorTimer.restart()
        }
    }

    Connections {
        target: systemActions
        function onActionStarted(message) {
            window.errorMessage = message
            errorToast.visible = true
            errorTimer.restart()
        }
        function onErrorOccurred(message) {
            window.errorMessage = message
            errorToast.visible = true
            errorTimer.restart()
        }
    }

    Connections {
        target: autostartController
        function onErrorOccurred(message) {
            window.presentMessage(message, true)
        }
    }

    FolderDialog {
        id: folderDialog
        title: window.translate("chooseFolder")
        onAccepted: {
            window.selectedDirectory = selectedFolder.toString()
            directoryField.text = selectedFolder.toString().replace("file:///", "")
        }
    }

    FileDialog {
        id: importListDialog
        title: window.translate("importDownloadList")
        nameFilters: [
            window.translate("pidmDownloadListsFilter") + " (*.json)",
            window.translate("jsonFilesFilter") + " (*.json)"
        ]
        fileMode: FileDialog.OpenFile
        onAccepted: {
            const count = downloadsModel.importDownloadList(selectedFile.toString())
            window.errorMessage = count + " " + window.translate("downloadsImported")
            errorToast.visible = true
            errorTimer.restart()
        }
    }

    FileDialog {
        id: exportListDialog
        title: window.translate("exportDownloadList")
        nameFilters: [
            window.translate("pidmDownloadListsFilter") + " (*.json)"
        ]
        fileMode: FileDialog.SaveFile
        onAccepted: {
            if (downloadsModel.exportDownloadList(selectedFile.toString())) {
                window.errorMessage = window.translate("downloadListExported")
                errorToast.visible = true
                errorTimer.restart()
            }
        }
    }

    FileDialog {
        id: completionSoundDialog
        title: window.translate("chooseCompletionSound")
        nameFilters: [window.translate("wavAudioFilter") + " (*.wav)"]
        fileMode: FileDialog.OpenFile
        onAccepted: downloadSettings.completionSound = selectedFile.toString()
    }

    FileDialog {
        id: errorSoundDialog
        title: window.translate("chooseErrorSound")
        nameFilters: [window.translate("wavAudioFilter") + " (*.wav)"]
        fileMode: FileDialog.OpenFile
        onAccepted: downloadSettings.errorSound = selectedFile.toString()
    }

    Timer {
        id: errorTimer
        interval: 5000
        onTriggered: errorToast.visible = false
    }

    ParallelAnimation {
        id: windowExitAnimation
        NumberAnimation {
            target: shell
            property: "opacity"
            to: 0
            duration: 150
            easing.type: Easing.InCubic
        }
        NumberAnimation {
            target: shell
            property: "scale"
            to: 0.975
            duration: 170
            easing.type: Easing.InCubic
        }
        onFinished: {
            const action = window.pendingWindowAction
            if (action === "minimize")
                window.showMinimized()
            else if (action === "hide")
                window.hide()
            else if (action === "quit")
                trayController.confirmQuit()
            shell.opacity = 1
            shell.scale = 1
            window.pendingWindowAction = ""
            window.windowTransitionBusy = false
        }
    }

    ParallelAnimation {
        id: windowEnterAnimation
        NumberAnimation {
            target: shell
            property: "opacity"
            to: 1
            duration: 170
            easing.type: Easing.OutCubic
        }
        NumberAnimation {
            target: shell
            property: "scale"
            to: 1
            duration: 190
            easing.type: Easing.OutCubic
        }
    }

    Rectangle {
        id: shell
        anchors.fill: parent
        radius: window.visibility === Window.Maximized ? 0 : 14
        color: theme.background
        border.width: window.visibility === Window.Maximized ? 0 : 1
        border.color: theme.line
        clip: true

        Rectangle {
            id: titleBar
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.top: parent.top
            height: 56
            color: theme.sidebar
            border.width: 0
            z: 20
            LayoutMirroring.enabled: false
            LayoutMirroring.childrenInherit: true

            Rectangle {
                anchors.left: parent.left
                anchors.right: parent.right
                anchors.bottom: parent.bottom
                height: 1
                color: theme.line
            }

            DragHandler {
                target: null
                onActiveChanged: if (active) window.startSystemMove()
            }

            TapHandler {
                acceptedButtons: Qt.LeftButton
                onDoubleTapped: window.toggleMaximized()
            }

            RowLayout {
                anchors.fill: parent
                anchors.leftMargin: 18
                anchors.rightMargin: 10
                spacing: 11

                Rectangle {
                    Layout.preferredWidth: 30
                    Layout.preferredHeight: 30
                    radius: 9
                    color: appLogoUrl.length > 0 ? "transparent" : theme.accent

                    Image {
                        visible: appLogoUrl.length > 0
                        anchors.fill: parent
                        anchors.margins: 2
                        source: appLogoUrl
                        fillMode: Image.PreserveAspectFit
                        smooth: true
                        mipmap: true
                    }

                    Row {
                        visible: appLogoUrl.length === 0
                        anchors.centerIn: parent
                        spacing: 2
                        Repeater {
                            model: [8, 15, 11]
                            delegate: Rectangle {
                                required property int modelData
                                width: 3
                                height: modelData
                                radius: 2
                                color: "white"
                                anchors.bottom: parent.bottom
                            }
                        }
                    }
                }

                ColumnLayout {
                    spacing: -2
                    AppText {
                        text: "PIDM"
                        color: theme.text
                        font.pixelSize: 13
                        font.weight: Font.Bold
                    }
                    AppText {
                        text: window.translate("appTagline")
                        color: theme.subtle
                        font.pixelSize: 8
                        font.letterSpacing: 0.5
                    }
                }

                Item { Layout.fillWidth: true }

                Rectangle {
                    visible: downloadsModel.activeCount > 0
                    Layout.preferredWidth: activeTitle.implicitWidth + 28
                    Layout.preferredHeight: 28
                    radius: 14
                    color: theme.accentSoft
                    Row {
                        anchors.centerIn: parent
                        spacing: 7
                        Rectangle {
                            width: 6
                            height: 6
                            radius: 3
                            color: theme.success
                            anchors.verticalCenter: parent.verticalCenter
                        }
                        AppText {
                            id: activeTitle
                            text: downloadsModel.totalSpeed
                            color: theme.text
                            font.pixelSize: 10
                            font.weight: Font.DemiBold
                        }
                    }
                }

                IconButton {
                    iconName: "settings"
                    onClicked: quickSettings.open()
                }

                Rectangle {
                    Layout.preferredWidth: 1
                    Layout.preferredHeight: 22
                    color: theme.line
                }

                Rectangle {
                    Layout.preferredWidth: windowControls.implicitWidth + 8
                    Layout.preferredHeight: 38
                    radius: 10
                    color: theme.surface
                    border.width: 1
                    border.color: theme.line

                    Row {
                        id: windowControls
                        anchors.centerIn: parent
                        spacing: 1

                        WindowButton {
                            iconName: "minus"
                            onClicked: window.minimizeAnimated()
                        }
                        WindowButton {
                            iconName: window.visibility === Window.Maximized
                                ? "restore" : "maximize"
                            onClicked: window.toggleMaximized()
                        }
                        WindowButton {
                            iconName: "close"
                            closeButton: true
                            onClicked: window.requestClose()
                        }
                    }
                }
            }
        }

        Item {
            id: mainBody
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.top: titleBar.bottom
            anchors.bottom: parent.bottom
            LayoutMirroring.enabled: false
            LayoutMirroring.childrenInherit: false

            Rectangle {
                id: sidebar
                x: I18n.rtl ? mainBody.width - width : 0
                width: window.compactSidebar ? 78 : 226
                anchors.top: parent.top
                anchors.bottom: parent.bottom
                color: theme.sidebar

                Rectangle {
                    x: I18n.rtl ? 0 : parent.width - width
                    width: 1
                    anchors.top: parent.top
                    anchors.bottom: parent.bottom
                    color: theme.line
                }

                ColumnLayout {
                    anchors.fill: parent
                    anchors.margins: 14
                    spacing: 7
                    LayoutMirroring.enabled: I18n.rtl
                    LayoutMirroring.childrenInherit: true

                    AppText {
                        visible: !window.compactSidebar
                        Layout.fillWidth: true
                        text: window.translate("workspace").toUpperCase()
                        color: theme.subtle
                        font.pixelSize: 8
                        font.weight: Font.DemiBold
                        font.letterSpacing: 1.2
                        horizontalAlignment: I18n.rtl
                            ? Text.AlignRight : Text.AlignLeft
                        Layout.leftMargin: 11
                        Layout.topMargin: 8
                        Layout.bottomMargin: 6
                    }

                    NavButton {
                        Layout.fillWidth: true
                        text: window.translate("dashboard")
                        iconName: "dashboard"
                        collapsed: window.compactSidebar
                        active: window.pageIndex === 0 && window.showOverview
                        onClicked: {
                            window.pageIndex = 0
                            window.showOverview = true
                        }
                    }

                    NavButton {
                        Layout.fillWidth: true
                        text: window.translate("downloads")
                        iconName: "download"
                        badge: downloadsModel.totalCount.toString()
                        collapsed: window.compactSidebar
                        active: window.pageIndex === 0 && !window.showOverview
                        onClicked: {
                            window.pageIndex = 0
                            window.showOverview = false
                        }
                    }

                    AppText {
                        visible: !window.compactSidebar
                        Layout.fillWidth: true
                        text: window.translate("manage").toUpperCase()
                        color: theme.subtle
                        font.pixelSize: 8
                        font.weight: Font.DemiBold
                        font.letterSpacing: 1.2
                        horizontalAlignment: I18n.rtl
                            ? Text.AlignRight : Text.AlignLeft
                        Layout.leftMargin: 11
                        Layout.topMargin: 18
                        Layout.bottomMargin: 5
                    }

                    NavButton {
                        Layout.fillWidth: true
                        text: window.translate("queue")
                        iconName: "list"
                        collapsed: window.compactSidebar
                        active: window.pageIndex === 1
                        onClicked: window.pageIndex = 1
                    }
                    Item { Layout.fillHeight: true }

                    NavButton {
                        Layout.fillWidth: true
                        text: window.translate("settings")
                        iconName: "settings"
                        collapsed: window.compactSidebar
                        active: window.pageIndex === 2
                        onClicked: window.pageIndex = 2
                    }

                    Rectangle {
                        Layout.fillWidth: true
                        Layout.preferredHeight: window.compactSidebar ? 48 : 58
                        radius: 11
                        color: theme.surface
                        border.width: 1
                        border.color: theme.line

                        RowLayout {
                            anchors.fill: parent
                            anchors.margins: 9
                            spacing: 9

                            Rectangle {
                                Layout.preferredWidth: 30
                                Layout.preferredHeight: 30
                                radius: 9
                                color: theme.accentSoft
                                AppText {
                                    anchors.centerIn: parent
                                    text: "SM"
                                    color: theme.accent
                                    font.pixelSize: 9
                                    font.weight: Font.Bold
                                }
                            }
                            ColumnLayout {
                                visible: !window.compactSidebar
                                Layout.fillWidth: true
                                spacing: 0
                                AppText {
                                    text: "Saeed M."
                                    color: theme.text
                                    font.pixelSize: 10
                                    font.weight: Font.DemiBold
                                }
                                AppText {
                                    text: window.translate("localMode")
                                    color: theme.subtle
                                    font.pixelSize: 8
                                }
                            }
                        }
                    }
                }
            }

            Item {
                id: contentPanel
                x: I18n.rtl ? 0 : sidebar.width
                width: mainBody.width - sidebar.width
                anchors.top: parent.top
                anchors.bottom: parent.bottom

                StackLayout {
                    anchors.fill: parent
                    currentIndex: window.pageIndex
                    LayoutMirroring.enabled: I18n.rtl
                    LayoutMirroring.childrenInherit: true

                    Item {
                    ColumnLayout {
                        anchors.fill: parent
                        anchors.leftMargin: window.compactSidebar ? 22 : 30
                        anchors.rightMargin: window.compactSidebar ? 22 : 30
                        anchors.topMargin: 24
                        anchors.bottomMargin: 24
                        spacing: 18

                        RowLayout {
                            Layout.fillWidth: true
                            spacing: 12
                            ColumnLayout {
                                spacing: 3
                                AppText {
                                    text: window.showOverview
                                        ? window.translate("dashboardTitle")
                                        : window.translate("downloadsTitle")
                                    color: theme.text
                                    font.pixelSize: 25
                                    font.weight: Font.Bold
                                }
                                AppText {
                                    text: window.showOverview
                                        ? window.translate("dashboardSubtitle")
                                        : window.translate("downloadsSubtitle")
                                    color: theme.muted
                                    font.pixelSize: 11
                                }
                            }
                            Item { Layout.fillWidth: true }
                            AppButton {
                                visible: downloadsModel.activeCount > 0
                                text: window.translate("pauseAll")
                                iconName: "pause"
                                onClicked: downloadsModel.pauseAll()
                            }
                            AppButton {
                                text: window.translate("addDownload")
                                iconName: "plus"
                                tone: "primary"
                                onClicked: addPopup.open()
                            }
                            AppButton {
                                text: window.translate("addMultipleLinks")
                                iconName: "list"
                                onClicked: window.openMultipleLinks()
                            }
                        }

                        GridLayout {
                            visible: window.showOverview
                            Layout.fillWidth: true
                            columns: window.width < 1160 ? 2 : 4
                            columnSpacing: 12
                            rowSpacing: 12

                            StatCard {
                                Layout.fillWidth: true
                                label: window.translate("active")
                                value: downloadsModel.activeCount.toString()
                                helper: window.translate("activeHelper")
                                iconName: "download"
                                tone: theme.accent
                            }
                            StatCard {
                                Layout.fillWidth: true
                                label: window.translate("completed")
                                value: downloadsModel.completedCount.toString()
                                helper: window.translate("completedHelper")
                                iconName: "check"
                                tone: theme.success
                            }
                            StatCard {
                                Layout.fillWidth: true
                                label: window.translate("currentSpeed")
                                value: downloadsModel.totalSpeed
                                helper: window.translate("speedHelper")
                                iconName: "network"
                                tone: theme.warning
                            }
                            StatCard {
                                Layout.fillWidth: true
                                label: window.translate("needsAttention")
                                value: downloadsModel.problemCount.toString()
                                helper: window.translate("attentionHelper")
                                iconName: "alert"
                                tone: theme.danger
                            }
                        }

                        Rectangle {
                            Layout.fillWidth: true
                            Layout.fillHeight: true
                            Layout.minimumHeight: 260
                            radius: 15
                            color: theme.surface
                            border.width: 1
                            border.color: theme.line

                            ColumnLayout {
                                anchors.fill: parent
                                spacing: 0

                                RowLayout {
                                    Layout.fillWidth: true
                                    Layout.preferredHeight: 64
                                    Layout.leftMargin: 18
                                    Layout.rightMargin: 18
                                    spacing: 12

                                    AppText {
                                        text: window.translate("transfers")
                                        color: theme.text
                                        font.pixelSize: 14
                                        font.weight: Font.Bold
                                    }
                                    Rectangle {
                                        width: countText.implicitWidth + 12
                                        height: 21
                                        radius: 10
                                        color: theme.surfaceRaised
                                        AppText {
                                            id: countText
                                            anchors.centerIn: parent
                                            text: downloadsModel.totalCount
                                            color: theme.muted
                                            font.pixelSize: 9
                                            font.weight: Font.DemiBold
                                        }
                                    }
                                        Item { Layout.fillWidth: true }
                                        ComboBox {
                                            id: sortCombo
                                            Layout.preferredWidth: window.width < 1080 ? 126 : 154
                                            Layout.preferredHeight: 31
                                            model: [
                                                window.translate("sortNewest"),
                                                window.translate("sortOldest"),
                                                window.translate("sortName"),
                                                window.translate("sortLargest"),
                                                window.translate("sortStatus")
                                            ]
                                            onActivated: {
                                                const keys = ["date", "date", "name", "size", "status"]
                                                const descending = [true, false, false, true, false]
                                                downloadsModel.sortDownloads(
                                                    keys[currentIndex], descending[currentIndex]
                                                )
                                            }
                                            contentItem: AppText {
                                                leftPadding: I18n.rtl ? 28 : 11
                                                rightPadding: I18n.rtl ? 11 : 28
                                                text: sortCombo.displayText
                                                color: theme.text
                                                font.pixelSize: 9
                                                font.weight: Font.DemiBold
                                                verticalAlignment: Text.AlignVCenter
                                                horizontalAlignment: I18n.rtl
                                                    ? Text.AlignRight : Text.AlignLeft
                                                elide: Text.ElideRight
                                            }
                                            indicator: UiIcon {
                                                name: "chevron"
                                                color: theme.muted
                                                width: 13
                                                height: 13
                                                x: I18n.rtl ? 10
                                                    : sortCombo.width - width - 10
                                                y: (sortCombo.height - height) / 2
                                                rotation: 90
                                            }
                                            background: Rectangle {
                                                radius: 8
                                                color: theme.surfaceRaised
                                                border.width: 1
                                                border.color: sortCombo.activeFocus
                                                    ? theme.accent : theme.line
                                            }
                                            delegate: ItemDelegate {
                                                required property var modelData
                                                width: sortCombo.width
                                                height: 32
                                                contentItem: AppText {
                                                    text: modelData
                                                    color: theme.text
                                                    font.pixelSize: 9
                                                    verticalAlignment: Text.AlignVCenter
                                                }
                                                background: Rectangle {
                                                    radius: 6
                                                    color: highlighted
                                                        ? theme.accentSoft : "transparent"
                                                }
                                            }
                                            popup: Popup {
                                                y: sortCombo.height + 4
                                                width: sortCombo.width
                                                implicitHeight: contentItem.implicitHeight + 8
                                                padding: 4
                                                contentItem: ListView {
                                                    clip: true
                                                    implicitHeight: contentHeight
                                                    model: sortCombo.popup.visible
                                                        ? sortCombo.delegateModel : null
                                                    currentIndex: sortCombo.highlightedIndex
                                                }
                                                background: Rectangle {
                                                    radius: 9
                                                    color: theme.surface
                                                    border.width: 1
                                                    border.color: theme.line
                                                }
                                            }
                                        }
                                        AppTextField {
                                        id: searchField
                                        Layout.preferredWidth: window.width < 1080 ? 170 : 230
                                        implicitHeight: 38
                                        iconName: "search"
                                        placeholderText: window.translate("searchDownloads")
                                    }
                                }

                                Rectangle {
                                    Layout.fillWidth: true
                                    Layout.preferredHeight: 45
                                    color: theme.background
                                    border.width: 0

                                    RowLayout {
                                        anchors.fill: parent
                                        anchors.leftMargin: 16
                                        anchors.rightMargin: 16
                                        spacing: 4

                                        Repeater {
                                            model: [
                                                ["all", "filterAll"],
                                                ["active", "filterActive"],
                                                ["completed", "filterCompleted"],
                                                ["problems", "filterProblems"]
                                            ]
                                            delegate: Rectangle {
                                                required property var modelData
                                                Layout.preferredWidth: filterText.implicitWidth + 22
                                                Layout.preferredHeight: 29
                                                radius: 8
                                                color: window.currentFilter === modelData[0]
                                                    ? theme.accentSoft : "transparent"
                                                AppText {
                                                    id: filterText
                                                    anchors.centerIn: parent
                                                    text: window.translate(modelData[1])
                                                    color: window.currentFilter === modelData[0]
                                                        ? theme.accent : theme.muted
                                                    font.pixelSize: 9
                                                    font.weight: Font.DemiBold
                                                }
                                                TapHandler {
                                                    onTapped: window.currentFilter = modelData[0]
                                                }
                                            }
                                        }
                                        Item { Layout.fillWidth: true }
                                        AppButton {
                                            visible: downloadsModel.problemCount > 0
                                            compact: window.width < 1080
                                            text: window.translate("resumeAll")
                                            iconName: "play"
                                            implicitHeight: 31
                                            onClicked: downloadsModel.resumeAll()
                                        }
                                        AppButton {
                                            visible: downloadsModel.completedCount > 0
                                            compact: window.width < 1080
                                            text: window.translate("clearCompleted")
                                            iconName: "trash"
                                            implicitHeight: 31
                                            onClicked: downloadsModel.clearCompleted()
                                        }
                                    }
                                }

                                Rectangle {
                                    Layout.fillWidth: true
                                    Layout.preferredHeight: 1
                                    color: theme.line
                                }

                                Item {
                                    Layout.fillWidth: true
                                    Layout.fillHeight: true

                                    ListView {
                                        id: downloadList
                                        anchors.fill: parent
                                        anchors.leftMargin: 8
                                        anchors.rightMargin: 8
                                        anchors.topMargin: 6
                                        anchors.bottomMargin: 6
                                        clip: true
                                        spacing: 4
                                        model: downloadsModel

                                        ScrollBar.vertical: ScrollBar {
                                            policy: ScrollBar.AsNeeded
                                            width: 5
                                            contentItem: Rectangle {
                                                radius: 3
                                                color: theme.subtle
                                            }
                                            background: Item {}
                                        }

                                        delegate: Rectangle {
                                            id: downloadRow
                                            required property int downloadId
                                            required property string name
                                            required property string source
                                            required property string size
                                            required property int progress
                                            required property string speed
                                            required property string eta
                                            required property string status
                                            required property string protocol
                                            required property string errorMessage
                                            required property string createdAt
                                            required property int index

                                            readonly property bool matches: window.visibleForFilter(status, name)
                                            width: ListView.view.width - 4
                                            height: matches ? 86 : 0
                                            visible: matches
                                            radius: 11
                                            color: rowHover.hovered ? theme.surfaceHover : "transparent"
                                            clip: true
                                            Behavior on color { ColorAnimation { duration: 120 } }

                                            HoverHandler { id: rowHover }
                                            TapHandler {
                                                acceptedButtons: Qt.LeftButton
                                                onDoubleTapped: window.openDetails(downloadRow.downloadId)
                                            }

                                            RowLayout {
                                                anchors.fill: parent
                                                anchors.leftMargin: 12
                                                anchors.rightMargin: 8
                                                spacing: 12

                                                Rectangle {
                                                    Layout.preferredWidth: 40
                                                    Layout.preferredHeight: 44
                                                    radius: 10
                                                    color: theme.accentSoft
                                                    AppText {
                                                        anchors.centerIn: parent
                                                        text: name.includes(".")
                                                            ? name.split(".").pop().slice(0, 3).toUpperCase()
                                                            : window.translate("file")
                                                        color: theme.accent
                                                        font.pixelSize: 8
                                                        font.weight: Font.Bold
                                                    }
                                                }

                                                ColumnLayout {
                                                    Layout.fillWidth: true
                                                    Layout.minimumWidth: 180
                                                    spacing: 4
                                                    RowLayout {
                                                        Layout.fillWidth: true
                                                        spacing: 7
                                                        AppText {
                                                            Layout.fillWidth: true
                                                            text: name
                                                            color: theme.text
                                                            font.pixelSize: 11
                                                            font.weight: Font.DemiBold
                                                            horizontalAlignment: I18n.rtl
                                                                ? Text.AlignRight : Text.AlignLeft
                                                            elide: Text.ElideMiddle
                                                        }
                                                        Rectangle {
                                                            Layout.preferredWidth: protocolText.implicitWidth + 12
                                                            Layout.preferredHeight: 19
                                                            radius: 6
                                                            color: theme.surfaceRaised
                                                            AppText {
                                                                id: protocolText
                                                                anchors.centerIn: parent
                                                                text: protocol
                                                                color: theme.muted
                                                                font.pixelSize: 8
                                                                font.weight: Font.DemiBold
                                                            }
                                                        }
                                                    }
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: errorMessage.length
                                                            ? errorMessage
                                                            : source + "  ·  " + size
                                                                + "  ·  " + createdAt
                                                        color: errorMessage.length ? theme.danger : theme.muted
                                                        font.pixelSize: 9
                                                        LayoutMirroring.enabled: false
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                        elide: Text.ElideRight
                                                    }
                                                    Rectangle {
                                                        Layout.fillWidth: true
                                                        Layout.preferredHeight: 4
                                                        radius: 2
                                                        color: theme.line
                                                            Rectangle {
                                                                width: parent.width * Math.max(0, Math.min(100, progress)) / 100
                                                                x: I18n.rtl ? parent.width - width : 0
                                                                height: parent.height
                                                            radius: 2
                                                            color: status === "complete" ? theme.success : theme.accent
                                                            Behavior on width { NumberAnimation { duration: 180 } }
                                                        }
                                                    }
                                                }

                                                ColumnLayout {
                                                    visible: window.width >= 1060
                                                    Layout.preferredWidth: 74
                                                    spacing: 2
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: window.translate("progress").toUpperCase()
                                                        color: theme.subtle
                                                        font.pixelSize: 7
                                                        font.letterSpacing: 0.6
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                    }
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: progress + "%"
                                                        color: theme.text
                                                        font.pixelSize: 10
                                                        font.weight: Font.DemiBold
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                    }
                                                }

                                                ColumnLayout {
                                                    visible: window.width >= 1160
                                                    Layout.preferredWidth: 92
                                                    spacing: 2
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: window.translate("speed").toUpperCase()
                                                        color: theme.subtle
                                                        font.pixelSize: 7
                                                        font.letterSpacing: 0.6
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                    }
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: status === "paused"
                                                            ? window.translate("paused")
                                                            : status === "pausing"
                                                                ? window.translate("pausing") : speed
                                                        color: theme.text
                                                        font.pixelSize: 10
                                                        font.weight: Font.DemiBold
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                    }
                                                }

                                                ColumnLayout {
                                                    visible: window.width >= 1240
                                                    Layout.preferredWidth: 62
                                                    spacing: 2
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: window.translate("eta").toUpperCase()
                                                        color: theme.subtle
                                                        font.pixelSize: 7
                                                        font.letterSpacing: 0.6
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                    }
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: eta
                                                        color: theme.text
                                                        font.pixelSize: 10
                                                        font.weight: Font.DemiBold
                                                        horizontalAlignment: I18n.rtl
                                                            ? Text.AlignRight : Text.AlignLeft
                                                    }
                                                }

                                                AppButton {
                                                    compact: true
                                                    iconName: status === "complete" ? "external"
                                                        : status === "downloading" || status === "pausing"
                                                            ? "pause" : "play"
                                                    enabled: status !== "pausing"
                                                    onClicked: status === "complete"
                                                        ? downloadsModel.openDownload(index)
                                                        : downloadsModel.togglePaused(index)
                                                }
                                                IconButton {
                                                    iconName: "chevron"
                                                    onClicked: window.openDetails(downloadRow.downloadId)
                                                }
                                                IconButton {
                                                    iconName: "trash"
                                                    danger: true
                                                    onClicked: downloadsModel.removeDownload(index)
                                                }
                                            }
                                        }
                                    }

                                    Column {
                                        visible: downloadsModel.totalCount === 0
                                        anchors.centerIn: parent
                                        spacing: 10
                                        Rectangle {
                                            anchors.horizontalCenter: parent.horizontalCenter
                                            width: 54
                                            height: 54
                                            radius: 16
                                            color: theme.accentSoft
                                            UiIcon {
                                                anchors.centerIn: parent
                                                name: "download"
                                                color: theme.accent
                                                width: 23
                                                height: 23
                                            }
                                        }
                                        AppText {
                                            anchors.horizontalCenter: parent.horizontalCenter
                                            text: window.translate("emptyTitle")
                                            color: theme.text
                                            font.pixelSize: 13
                                            font.weight: Font.DemiBold
                                        }
                                        AppText {
                                            anchors.horizontalCenter: parent.horizontalCenter
                                            text: window.translate("emptySubtitle")
                                            color: theme.muted
                                            font.pixelSize: 10
                                        }
                                        AppButton {
                                            anchors.horizontalCenter: parent.horizontalCenter
                                            text: window.translate("addDownload")
                                            iconName: "plus"
                                            tone: "primary"
                                            onClicked: addPopup.open()
                                        }
                                        AppButton {
                                            anchors.horizontalCenter: parent.horizontalCenter
                                            text: window.translate("addMultipleLinks")
                                            iconName: "list"
                                            onClicked: window.openMultipleLinks()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                QueuePage {
                    scheduleView: false
                    onOpenDetailsRequested: downloadId => window.openDetails(downloadId)
                }

                Item {
                    Flickable {
                        anchors.fill: parent
                        contentWidth: width
                        contentHeight: settingsColumn.implicitHeight + 64
                        clip: true

                        ColumnLayout {
                            id: settingsColumn
                            width: Math.min(parent.width - 60, 880)
                            x: (parent.width - width) / 2
                            y: 28
                            spacing: 18

                            AppText {
                                text: window.translate("settingsTitle")
                                color: theme.text
                                font.pixelSize: 25
                                font.weight: Font.Bold
                            }
                            AppText {
                                text: window.translate("settingsSubtitle")
                                color: theme.muted
                                font.pixelSize: 11
                                Layout.bottomMargin: 8
                            }

                            Rectangle {
                                Layout.fillWidth: true
                                Layout.preferredHeight: 184
                                radius: 15
                                color: theme.surface
                                border.width: 1
                                border.color: theme.line

                                ColumnLayout {
                                    anchors.fill: parent
                                    anchors.margins: 18
                                    spacing: 14
                                    AppText {
                                        text: window.translate("appearance")
                                        color: theme.text
                                        font.pixelSize: 13
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("appearanceHelper")
                                        color: theme.muted
                                        font.pixelSize: 10
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 10
                                        Repeater {
                                            model: [
                                                ["midnight", "#0A0D14", "#7357FF", "themeMidnight"],
                                                ["light", "#F3F5F9", "#654CE6", "themeLight"],
                                                ["ocean", "#061318", "#12A8C2", "themeOcean"]
                                            ]
                                            delegate: Rectangle {
                                                required property var modelData
                                                Layout.fillWidth: true
                                                Layout.preferredHeight: 74
                                                radius: 11
                                                color: theme.name === modelData[0]
                                                    ? theme.accentSoft : theme.surfaceRaised
                                                border.width: 1
                                                border.color: theme.name === modelData[0]
                                                    ? theme.accent : theme.line

                                                RowLayout {
                                                    anchors.fill: parent
                                                    anchors.margins: 12
                                                    spacing: 10
                                                    Rectangle {
                                                        Layout.preferredWidth: 34
                                                        Layout.preferredHeight: 34
                                                        radius: 10
                                                        color: modelData[1]
                                                        border.width: 1
                                                        border.color: modelData[2]
                                                        Rectangle {
                                                            anchors.centerIn: parent
                                                            width: 10
                                                            height: 10
                                                            radius: 5
                                                            color: modelData[2]
                                                        }
                                                    }
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: window.translate(modelData[3])
                                                        color: theme.text
                                                        font.pixelSize: 11
                                                        font.weight: Font.DemiBold
                                                    }
                                                    UiIcon {
                                                        visible: theme.name === modelData[0]
                                                        name: "check"
                                                        color: theme.accent
                                                        width: 17
                                                        height: 17
                                                    }
                                                }
                                                TapHandler { onTapped: theme.select(modelData[0]) }
                                            }
                                        }
                                    }
                                }
                            }

                            Rectangle {
                                Layout.fillWidth: true
                                Layout.preferredHeight: 154
                                radius: 15
                                color: theme.surface
                                border.width: 1
                                border.color: theme.line

                                ColumnLayout {
                                    anchors.fill: parent
                                    anchors.margins: 18
                                    spacing: 14
                                    AppText {
                                        text: window.translate("language")
                                        color: theme.text
                                        font.pixelSize: 13
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("languageHelper")
                                        color: theme.muted
                                        font.pixelSize: 10
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 8
                                        Repeater {
                                            model: [["en", "English"], ["fa", "فارسی"], ["ru", "Русский"], ["zh", "中文"]]
                                            delegate: AppButton {
                                                required property var modelData
                                                Layout.fillWidth: true
                                                text: modelData[1]
                                                tone: I18n.language === modelData[0] ? "primary" : "neutral"
                                                onClicked: I18n.language = modelData[0]
                                            }
                                        }
                                    }
                                }
                            }

                            Rectangle {
                                Layout.fillWidth: true
                                Layout.preferredHeight: 840
                                radius: 15
                                color: theme.surface
                                border.width: 1
                                border.color: theme.line

                                ColumnLayout {
                                    anchors.fill: parent
                                    anchors.margins: 18
                                    spacing: 11
                                    AppText {
                                        text: window.translate("downloadBehavior")
                                        color: theme.text
                                        font.pixelSize: 13
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("downloadBehaviorHelper")
                                        color: theme.muted
                                        font.pixelSize: 10
                                    }

                                    AppText {
                                        text: window.translate("defaultConnections")
                                        color: theme.muted
                                        font.pixelSize: 9
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 8
                                        Repeater {
                                            model: [1, 2, 4, 8]
                                            delegate: AppButton {
                                                required property int modelData
                                                Layout.fillWidth: true
                                                text: modelData.toString()
                                                tone: downloadSettings.maxConnections === modelData
                                                    ? "primary" : "neutral"
                                                onClicked: downloadSettings.maxConnections = modelData
                                            }
                                        }
                                    }

                                    AppText {
                                        text: window.translate("speedUnit")
                                        color: theme.muted
                                        font.pixelSize: 9
                                    }
                                    AppText {
                                        text: window.translate("speedUnitHelper")
                                        color: theme.subtle
                                        font.pixelSize: 8
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 8
                                        Repeater {
                                            model: [
                                                ["auto", "speedUnitAuto"],
                                                ["KB/s", "speedUnitKbps"],
                                                ["MB/s", "speedUnitMegabytes"],
                                                ["Mbps", "speedUnitMbps"]
                                            ]
                                            delegate: AppButton {
                                                required property var modelData
                                                Layout.fillWidth: true
                                                text: window.translate(modelData[1])
                                                tone: downloadSettings.speedUnit === modelData[0]
                                                    ? "primary" : "neutral"
                                                onClicked: downloadSettings.speedUnit = modelData[0]
                                            }
                                        }
                                    }

                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 10
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            AppText {
                                                text: window.translate("speedLimitKbps")
                                                color: theme.muted
                                                font.pixelSize: 9
                                            }
                                            AppTextField {
                                                Layout.fillWidth: true
                                                text: downloadSettings.speedLimitKbps.toString()
                                                validator: IntValidator { bottom: 0; top: 10000000 }
                                                onEditingFinished: downloadSettings.speedLimitKbps =
                                                    parseInt(text || "0")
                                            }
                                        }
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            AppText {
                                                text: window.translate("retryCount")
                                                color: theme.muted
                                                font.pixelSize: 9
                                            }
                                            AppTextField {
                                                Layout.fillWidth: true
                                                text: downloadSettings.maxRetries.toString()
                                                validator: IntValidator { bottom: 0; top: 10 }
                                                onEditingFinished: downloadSettings.maxRetries =
                                                    parseInt(text || "0")
                                            }
                                        }
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            AppText {
                                                text: window.translate("retryDelaySeconds")
                                                color: theme.muted
                                                font.pixelSize: 9
                                            }
                                            AppTextField {
                                                Layout.fillWidth: true
                                                text: downloadSettings.retryDelay.toString()
                                                validator: IntValidator { bottom: 1; top: 300 }
                                                onEditingFinished: downloadSettings.retryDelay =
                                                    parseInt(text || "3")
                                            }
                                        }
                                    }

                                    Rectangle {
                                        Layout.fillWidth: true
                                        Layout.preferredHeight: 1
                                        color: theme.line
                                    }
                                    AppText {
                                        text: window.translate("eventSounds")
                                        color: theme.text
                                        font.pixelSize: 11
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("eventSoundsHelper")
                                        color: theme.muted
                                        font.pixelSize: 9
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 10
                                        Repeater {
                                            model: [
                                                ["complete", "completionSound", "completionSoundHelper"],
                                                ["error", "errorSound", "errorSoundHelper"]
                                            ]
                                            delegate: Rectangle {
                                                required property var modelData
                                                readonly property bool isCompletion:
                                                    modelData[0] === "complete"
                                                readonly property bool soundEnabled: isCompletion
                                                    ? downloadSettings.completionSoundEnabled
                                                    : downloadSettings.errorSoundEnabled
                                                readonly property string soundSource: isCompletion
                                                    ? downloadSettings.completionSound
                                                    : downloadSettings.errorSound
                                                Layout.fillWidth: true
                                                Layout.preferredHeight: 116
                                                radius: 10
                                                color: theme.surfaceRaised
                                                border.width: 1
                                                border.color: soundEnabled
                                                    ? theme.accent : theme.line

                                                ColumnLayout {
                                                    anchors.fill: parent
                                                    anchors.margins: 11
                                                    spacing: 7
                                                    RowLayout {
                                                        Layout.fillWidth: true
                                                        ColumnLayout {
                                                            Layout.fillWidth: true
                                                            spacing: 2
                                                            AppText {
                                                                text: window.translate(modelData[1])
                                                                color: theme.text
                                                                font.pixelSize: 9
                                                                font.weight: Font.DemiBold
                                                            }
                                                            AppText {
                                                                Layout.fillWidth: true
                                                                text: window.translate(modelData[2])
                                                                color: theme.muted
                                                                font.pixelSize: 8
                                                                elide: Text.ElideRight
                                                            }
                                                        }
                                                        Rectangle {
                                                            width: 34
                                                            height: 20
                                                            radius: 10
                                                            color: soundEnabled
                                                                ? theme.accent : theme.line
                                                            Rectangle {
                                                                width: 14
                                                                height: 14
                                                                radius: 7
                                                                y: 3
                                                                x: soundEnabled !== I18n.rtl ? 17 : 3
                                                                color: "white"
                                                            }
                                                            TapHandler {
                                                                onTapped: {
                                                                    if (isCompletion)
                                                                        downloadSettings.completionSoundEnabled =
                                                                            !soundEnabled
                                                                    else
                                                                        downloadSettings.errorSoundEnabled =
                                                                            !soundEnabled
                                                                }
                                                            }
                                                        }
                                                    }
                                                    AppText {
                                                        Layout.fillWidth: true
                                                        text: soundSource.length > 0
                                                            ? decodeURIComponent(
                                                                soundSource.split("/").pop())
                                                            : window.translate("bundledDefault")
                                                        color: theme.subtle
                                                        font.pixelSize: 8
                                                        elide: Text.ElideMiddle
                                                    }
                                                    RowLayout {
                                                        Layout.fillWidth: true
                                                        spacing: 7
                                                        AppButton {
                                                            Layout.fillWidth: true
                                                            text: window.translate("chooseSound")
                                                            onClicked: isCompletion
                                                                ? completionSoundDialog.open()
                                                                : errorSoundDialog.open()
                                                        }
                                                        AppButton {
                                                            text: window.translate("previewSound")
                                                            onClicked: isCompletion
                                                                ? soundController.previewCompletion()
                                                                : soundController.previewError()
                                                        }
                                                        AppButton {
                                                            text: window.translate("useDefault")
                                                            enabled: soundSource.length > 0
                                                            onClicked: {
                                                                if (isCompletion)
                                                                    downloadSettings.completionSound = ""
                                                                else
                                                                    downloadSettings.errorSound = ""
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    Rectangle {
                                        Layout.fillWidth: true
                                        Layout.preferredHeight: 1
                                        color: theme.line
                                    }
                                    AppText {
                                        text: window.translate("desktopIntegration")
                                        color: theme.text
                                        font.pixelSize: 11
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("desktopIntegrationHelper")
                                        color: theme.muted
                                        font.pixelSize: 9
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 10
                                        Repeater {
                                            model: [
                                                ["clipboard", "clipboardMonitoring", "clipboardMonitoringHelper"],
                                                ["notifications", "completionNotifications", "completionNotificationsHelper"],
                                                ["startup", "openAtStartup", "openAtStartupHelper"]
                                            ]
                                            delegate: Rectangle {
                                                required property var modelData
                                                visible: modelData[0] !== "startup"
                                                    || autostartController.supported
                                                readonly property bool selected: modelData[0] === "clipboard"
                                                    ? downloadSettings.clipboardMonitoring
                                                    : modelData[0] === "notifications"
                                                        ? downloadSettings.completionNotifications
                                                        : autostartController.enabled
                                                Layout.fillWidth: true
                                                Layout.preferredHeight: 62
                                                radius: 10
                                                color: selected ? theme.accentSoft : theme.surfaceRaised
                                                border.width: 1
                                                border.color: selected ? theme.accent : theme.line
                                                RowLayout {
                                                    anchors.fill: parent
                                                    anchors.margins: 11
                                                    ColumnLayout {
                                                        Layout.fillWidth: true
                                                        spacing: 2
                                                        AppText {
                                                            text: window.translate(modelData[1])
                                                            color: theme.text
                                                            font.pixelSize: 9
                                                            font.weight: Font.DemiBold
                                                        }
                                                        AppText {
                                                            Layout.fillWidth: true
                                                            text: window.translate(modelData[2])
                                                            color: theme.muted
                                                            font.pixelSize: 8
                                                            elide: Text.ElideRight
                                                        }
                                                    }
                                                    Rectangle {
                                                        width: 34
                                                        height: 20
                                                        radius: 10
                                                        color: selected ? theme.accent : theme.line
                                                        Rectangle {
                                                            width: 14
                                                            height: 14
                                                            radius: 7
                                                            y: 3
                                                            x: selected !== I18n.rtl ? 17 : 3
                                                            color: "white"
                                                        }
                                                    }
                                                }
                                                TapHandler {
                                                    onTapped: {
                                                        if (modelData[0] === "clipboard")
                                                            downloadSettings.clipboardMonitoring = !selected
                                                        else if (modelData[0] === "notifications")
                                                            downloadSettings.completionNotifications = !selected
                                                        else
                                                            autostartController.enabled = !selected
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    Rectangle {
                                        Layout.fillWidth: true
                                        Layout.preferredHeight: 1
                                        color: theme.line
                                    }

                                    AppText {
                                        text: window.translate("siteOverrides")
                                        color: theme.text
                                        font.pixelSize: 11
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("siteOverridesHelper")
                                        color: theme.muted
                                        font.pixelSize: 9
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        AppTextField {
                                            id: siteHostField
                                            Layout.fillWidth: true
                                            placeholderText: "downloads.example.com"
                                            iconName: "globe"
                                        }
                                        AppTextField {
                                            id: siteConnectionsField
                                            Layout.preferredWidth: 90
                                            text: "2"
                                            validator: IntValidator { bottom: 1; top: 8 }
                                        }
                                        AppButton {
                                            text: window.translate("add")
                                            iconName: "plus"
                                            onClicked: {
                                                if (downloadSettings.setSiteLimit(
                                                    siteHostField.text,
                                                    parseInt(siteConnectionsField.text || "1"))) {
                                                    siteHostField.clear()
                                                }
                                            }
                                        }
                                    }

                                    Flow {
                                        Layout.fillWidth: true
                                        spacing: 7
                                        Repeater {
                                            model: downloadSettings.siteLimits
                                            delegate: Rectangle {
                                                required property var modelData
                                                width: siteText.implicitWidth + 34
                                                height: 28
                                                radius: 9
                                                color: theme.surfaceRaised
                                                border.width: 1
                                                border.color: theme.line
                                                Row {
                                                    anchors.centerIn: parent
                                                    spacing: 7
                                                    AppText {
                                                        id: siteText
                                                        text: modelData.host + "  ·  "
                                                            + modelData.connections + "×"
                                                        color: theme.text
                                                        font.pixelSize: 9
                                                    }
                                                    UiIcon {
                                                        name: "close"
                                                        color: theme.danger
                                                        width: 12
                                                        height: 12
                                                        anchors.verticalCenter: parent.verticalCenter
                                                    }
                                                }
                                                TapHandler {
                                                    onTapped: downloadSettings.removeSiteLimit(
                                                        modelData.host
                                                    )
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            Rectangle {
                                Layout.fillWidth: true
                                Layout.preferredHeight: 142
                                radius: 15
                                color: theme.surface
                                border.width: 1
                                border.color: theme.line

                                ColumnLayout {
                                    anchors.fill: parent
                                    anchors.margins: 18
                                    spacing: 12
                                    AppText {
                                        text: window.translate("downloadLocation")
                                        color: theme.text
                                        font.pixelSize: 13
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("downloadLocationHelper")
                                        color: theme.muted
                                        font.pixelSize: 10
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        AppTextField {
                                            Layout.fillWidth: true
                                            text: window.selectedDirectory.toString().replace("file:///", "")
                                            readOnly: true
                                            iconName: "folder"
                                        }
                                        AppButton {
                                            text: window.translate("browse")
                                            iconName: "folder"
                                            onClicked: folderDialog.open()
                                        }
                                    }
                                }
                            }

                            Rectangle {
                                Layout.fillWidth: true
                                Layout.preferredHeight: 690
                                radius: 15
                                color: theme.surface
                                border.width: 1
                                border.color: theme.line
                                ColumnLayout {
                                    anchors.fill: parent
                                    anchors.margins: 18
                                    spacing: 11
                                    AppText {
                                        text: window.translate("requestAndProtocols")
                                        color: theme.text
                                        font.pixelSize: 13
                                        font.weight: Font.DemiBold
                                    }
                                    AppText {
                                        text: window.translate("requestAndProtocolsHelper")
                                        color: theme.muted
                                        font.pixelSize: 9
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 10
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            AppText { text: window.translate("proxyUrl"); color: theme.muted; font.pixelSize: 8 }
                                            AppTextField {
                                                Layout.fillWidth: true
                                                text: downloadSettings.proxyUrl
                                                placeholderText: "http://127.0.0.1:8080"
                                                onEditingFinished: downloadSettings.proxyUrl = text
                                            }
                                        }
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            AppText { text: window.translate("interfaceAddress"); color: theme.muted; font.pixelSize: 8 }
                                            AppTextField {
                                                Layout.fillWidth: true
                                                text: downloadSettings.interfaceAddress
                                                placeholderText: window.translate("automatic")
                                                onEditingFinished: downloadSettings.interfaceAddress = text
                                            }
                                        }
                                    }
                                    Rectangle {
                                        Layout.fillWidth: true
                                        Layout.preferredHeight: 1
                                        color: theme.line
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            spacing: 2
                                            AppText {
                                                text: window.translate("multiInterface")
                                                color: theme.text
                                                font.pixelSize: 10
                                                font.weight: Font.DemiBold
                                            }
                                            AppText {
                                                Layout.fillWidth: true
                                                text: window.translate("multiInterfaceHelper")
                                                color: theme.muted
                                                font.pixelSize: 8
                                                wrapMode: Text.WordWrap
                                            }
                                        }
                                        AppButton {
                                            compact: true
                                            iconName: "network"
                                            onClicked: downloadSettings.refreshInterfaces()
                                            ToolTip.visible: hovered
                                            ToolTip.text: window.translate("refreshInterfaces")
                                        }
                                        Rectangle {
                                            width: 38
                                            height: 22
                                            radius: 11
                                            color: downloadSettings.multiInterfaceEnabled
                                                ? theme.accent : theme.line
                                            Rectangle {
                                                width: 16
                                                height: 16
                                                radius: 8
                                                y: 3
                                                x: downloadSettings.multiInterfaceEnabled
                                                    !== I18n.rtl ? 19 : 3
                                                color: "white"
                                                Behavior on x { NumberAnimation { duration: 120 } }
                                            }
                                            TapHandler {
                                                onTapped: downloadSettings.multiInterfaceEnabled =
                                                    !downloadSettings.multiInterfaceEnabled
                                            }
                                        }
                                    }
                                    Flow {
                                        Layout.fillWidth: true
                                        spacing: 7
                                        Repeater {
                                            model: downloadSettings.availableInterfaces
                                            delegate: Rectangle {
                                                required property var modelData
                                                width: interfaceText.implicitWidth + 24
                                                height: 30
                                                radius: 8
                                                color: modelData.selected
                                                    ? theme.accentSoft : theme.surfaceRaised
                                                border.width: 1
                                                border.color: modelData.selected
                                                    ? theme.accent : theme.line
                                                AppText {
                                                    id: interfaceText
                                                    anchors.centerIn: parent
                                                    text: modelData.name + "  ·  " + modelData.address
                                                    color: modelData.selected ? theme.accent : theme.text
                                                    font.pixelSize: 8
                                                    font.weight: Font.DemiBold
                                                }
                                                TapHandler {
                                                    onTapped: downloadSettings.toggleInterface(
                                                        modelData.address
                                                    )
                                                }
                                            }
                                        }
                                        AppText {
                                            visible: downloadSettings.availableInterfaces.length === 0
                                            text: window.translate("noInterfaces")
                                            color: theme.subtle
                                            font.pixelSize: 8
                                        }
                                    }
                                    AppText {
                                        Layout.fillWidth: true
                                        text: window.translate("multiInterfaceLimit")
                                        color: theme.warning
                                        font.pixelSize: 8
                                        wrapMode: Text.WordWrap
                                    }
                                    Rectangle {
                                        Layout.fillWidth: true
                                        Layout.preferredHeight: 1
                                        color: theme.line
                                    }
                                    AppText { text: window.translate("userAgent"); color: theme.muted; font.pixelSize: 8 }
                                    AppTextField {
                                        Layout.fillWidth: true
                                        text: downloadSettings.userAgent
                                        onEditingFinished: downloadSettings.userAgent = text
                                    }
                                    AppText {
                                        text: window.translate("mediaCookies")
                                        color: theme.muted
                                        font.pixelSize: 8
                                    }
                                    AppText {
                                        text: window.translate("mediaCookiesHelper")
                                        color: theme.subtle
                                        font.pixelSize: 8
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        spacing: 7
                                        Repeater {
                                            model: [
                                                ["", "cookieBrowserOff"],
                                                ["edge", "cookieBrowserEdge"],
                                                ["chrome", "cookieBrowserChrome"],
                                                ["firefox", "cookieBrowserFirefox"]
                                            ]
                                            delegate: AppButton {
                                                required property var modelData
                                                Layout.fillWidth: true
                                                text: window.translate(modelData[1])
                                                tone: downloadSettings.mediaCookieBrowser
                                                    === modelData[0] ? "primary" : "neutral"
                                                onClicked: downloadSettings.mediaCookieBrowser =
                                                    modelData[0]
                                            }
                                        }
                                    }
                                    Rectangle { Layout.fillWidth: true; Layout.preferredHeight: 1; color: theme.line }
                                    AppText {
                                        text: window.translate("advancedProtocolStatus")
                                        color: theme.text
                                        font.pixelSize: 10
                                        font.weight: Font.DemiBold
                                    }
                                    GridLayout {
                                        Layout.fillWidth: true
                                        columns: 2
                                        columnSpacing: 8
                                        rowSpacing: 8
                                        Repeater {
                                            model: protocolCapabilities.statuses
                                            delegate: Rectangle {
                                                required property var modelData
                                                Layout.fillWidth: true
                                                Layout.preferredHeight: 54
                                                radius: 9
                                                color: theme.surfaceRaised
                                                border.width: 1
                                                border.color: modelData.available ? theme.success : theme.line
                                                RowLayout {
                                                    anchors.fill: parent
                                                    anchors.margins: 10
                                                    Rectangle {
                                                        width: 7; height: 7; radius: 4
                                                        color: modelData.available ? theme.success : theme.subtle
                                                    }
                                                    ColumnLayout {
                                                        Layout.fillWidth: true
                                                        spacing: 2
                                                        AppText {
                                                            text: window.translate(
                                                                modelData.nameKey
                                                            )
                                                            color: theme.text
                                                            font.pixelSize: 9
                                                            font.weight: Font.DemiBold
                                                        }
                                                        AppText {
                                                            Layout.fillWidth: true
                                                            text: window.translate(
                                                                modelData.detailKey
                                                            )
                                                            color: theme.muted
                                                            font.pixelSize: 7
                                                            elide: Text.ElideRight
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        Item { Layout.fillWidth: true }
                                        AppButton {
                                            text: window.translate("importDownloadList")
                                            iconName: "download"
                                            onClicked: importListDialog.open()
                                        }
                                        AppButton {
                                            text: window.translate("exportDownloadList")
                                            iconName: "external"
                                            onClicked: exportListDialog.open()
                                        }
                                    }
                                }
                            }

                            AppText {
                                text: "PIDM 2.0 Alpha  ·  Python + Qt Quick"
                                color: theme.subtle
                                font.pixelSize: 9
                                Layout.alignment: Qt.AlignHCenter
                                Layout.topMargin: 8
                            }
                        }
                    }
                }
            }
            }
        }

        Rectangle {
            id: errorToast
            visible: false
            z: 60
            anchors.horizontalCenter: parent.horizontalCenter
            anchors.bottom: parent.bottom
            anchors.bottomMargin: 24
            width: Math.min(560, parent.width - 48)
            height: 54
            radius: 12
            color: theme.surfaceRaised
            border.width: 1
            border.color: theme.danger

            RowLayout {
                anchors.fill: parent
                anchors.margins: 13
                spacing: 10
                UiIcon {
                    name: "alert"
                    color: theme.danger
                    width: 19
                    height: 19
                }
                AppText {
                    Layout.fillWidth: true
                    text: window.errorMessage
                    color: theme.text
                    font.pixelSize: 10
                    elide: Text.ElideRight
                }
                IconButton {
                    iconName: "close"
                    onClicked: errorToast.visible = false
                }
            }
        }
    }

    Popup {
        id: multipleLinksPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(620, window.width - 48)
        height: Math.min(510, window.height - 48)
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.58) }
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 150 }
                NumberAnimation {
                    property: "scale"
                    from: 0.96
                    to: 1
                    duration: 180
                    easing.type: Easing.OutCubic
                }
            }
        }
        exit: Transition {
            NumberAnimation { property: "opacity"; from: 1; to: 0; duration: 110 }
        }
        background: Rectangle {
            radius: 17
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }
        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 24
            spacing: 14

            Item {
                Layout.fillWidth: true
                Layout.preferredHeight: 58
                LayoutMirroring.enabled: false
                RowLayout {
                    anchors.left: parent.left
                    anchors.right: closeMultipleLinksButton.left
                    anchors.rightMargin: 12
                    anchors.verticalCenter: parent.verticalCenter
                    spacing: 12
                    LayoutMirroring.enabled: I18n.rtl
                    LayoutMirroring.childrenInherit: true
                    Rectangle {
                        Layout.preferredWidth: 42
                        Layout.preferredHeight: 42
                        radius: 12
                        color: theme.accentSoft
                        UiIcon {
                            anchors.centerIn: parent
                            name: "list"
                            color: theme.accent
                            width: 20
                            height: 20
                        }
                    }
                    ColumnLayout {
                        Layout.fillWidth: true
                        spacing: 2
                        AppText {
                            Layout.fillWidth: true
                            text: window.translate("addMultipleLinks")
                            color: theme.text
                            font.pixelSize: 17
                            font.weight: Font.Bold
                            horizontalAlignment: I18n.rtl
                                ? Text.AlignRight : Text.AlignLeft
                        }
                        AppText {
                            Layout.fillWidth: true
                            text: window.translate("multipleLinksHelper")
                            color: theme.muted
                            font.pixelSize: 10
                            wrapMode: Text.WordWrap
                            horizontalAlignment: I18n.rtl
                                ? Text.AlignRight : Text.AlignLeft
                        }
                    }
                }
                IconButton {
                    id: closeMultipleLinksButton
                    anchors.right: parent.right
                    anchors.top: parent.top
                    iconName: "close"
                    onClicked: multipleLinksPopup.close()
                }
            }

            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 1
                color: theme.line
            }

            AppText {
                Layout.fillWidth: true
                text: window.translate("multipleLinksLabel")
                color: theme.muted
                font.pixelSize: 9
                font.weight: Font.DemiBold
            }

            Rectangle {
                Layout.fillWidth: true
                Layout.fillHeight: true
                radius: 11
                color: theme.background
                border.width: 1
                border.color: multipleLinksField.activeFocus
                    ? theme.accent : theme.line
                TextArea {
                    id: multipleLinksField
                    anchors.fill: parent
                    anchors.margins: 4
                    padding: 12
                    color: theme.text
                    placeholderText: window.translate("multipleLinksPlaceholder")
                    placeholderTextColor: theme.subtle
                    selectionColor: theme.accent
                    selectedTextColor: "white"
                    font.family: I18n.fontFamily
                    font.pixelSize: 11
                    wrapMode: TextEdit.WrapAnywhere
                    selectByMouse: true
                    LayoutMirroring.enabled: false
                    horizontalAlignment: Text.AlignLeft
                    onTextChanged: multipleLinksMessage.text = ""
                    background: Item {}
                }
            }

            AppText {
                id: multipleLinksMessage
                Layout.fillWidth: true
                color: theme.danger
                font.pixelSize: 9
                wrapMode: Text.WordWrap
            }

            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                AppButton {
                    text: window.translate("cancel")
                    onClicked: multipleLinksPopup.close()
                }
                AppButton {
                    text: window.translate("reviewLinks")
                    iconName: "list"
                    tone: "primary"
                    enabled: multipleLinksField.text.trim().length > 0
                    onClicked: window.reviewMultipleLinks()
                }
            }
        }
        onOpened: multipleLinksField.forceActiveFocus()
    }

    Popup {
        id: addPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(560, window.width - 48)
        height: Math.min(720, window.height - 30)
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 150 }
                NumberAnimation { property: "scale"; from: 0.96; to: 1; duration: 180; easing.type: Easing.OutCubic }
            }
        }
        exit: Transition {
            NumberAnimation { property: "opacity"; from: 1; to: 0; duration: 110 }
        }

        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.58) }
        background: Rectangle {
            radius: 17
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }

        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 24
            spacing: 12

            RowLayout {
                Layout.fillWidth: true
                Layout.preferredHeight: 58
                spacing: 14
                LayoutMirroring.enabled: false
                LayoutMirroring.childrenInherit: false
                Rectangle {
                    Layout.preferredWidth: 46
                    Layout.preferredHeight: 46
                    radius: 12
                    color: theme.accentSoft
                    UiIcon {
                        anchors.centerIn: parent
                        name: "download"
                        color: theme.accent
                        width: 20
                        height: 20
                    }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 2
                    LayoutMirroring.enabled: I18n.rtl
                    LayoutMirroring.childrenInherit: true
                    AppText {
                        Layout.fillWidth: true
                        text: window.translate("addDownload")
                        color: theme.text
                        font.pixelSize: 17
                        font.weight: Font.Bold
                        horizontalAlignment: I18n.rtl
                            ? Text.AlignRight : Text.AlignLeft
                    }
                    AppText {
                        Layout.fillWidth: true
                        text: window.translate("addDownloadHelper")
                        color: theme.muted
                        font.pixelSize: 10
                        wrapMode: Text.WordWrap
                        maximumLineCount: 2
                        elide: Text.ElideRight
                        horizontalAlignment: I18n.rtl
                            ? Text.AlignRight : Text.AlignLeft
                    }
                }
                IconButton {
                    Layout.alignment: Qt.AlignTop
                    iconName: "close"
                    onClicked: addPopup.close()
                }
            }

            Rectangle { Layout.fillWidth: true; Layout.preferredHeight: 1; color: theme.line }

            AppText {
                text: window.translate("urlLabel").toUpperCase()
                color: theme.muted
                font.pixelSize: 8
                font.weight: Font.DemiBold
                font.letterSpacing: 0.8
                Layout.topMargin: 5
            }
            AppTextField {
                id: urlField
                Layout.fillWidth: true
                placeholderText: "https://example.com/file.zip"
                iconName: "globe"
                forceLeftToRight: true
                onAccepted: startDownloadButton.clicked()
                onTextEdited: {
                    window.addDownloadMessage = ""
                    window.resolvedMediaReady = false
                    window.resolvedTorrentReady = false
                    window.selectedTorrentFiles = []
                    window.selectedExpectedSize = 0
                }
            }

            Rectangle {
                visible: window.addDownloadMessage.length > 0
                Layout.fillWidth: true
                Layout.preferredHeight: 82
                radius: 10
                color: theme.surfaceRaised
                border.width: 1
                border.color: window.addDownloadMessageIsError
                    ? theme.danger : theme.success
                RowLayout {
                    anchors.fill: parent
                    anchors.margins: 11
                    spacing: 9
                    UiIcon {
                        name: window.addDownloadMessageIsError ? "alert" : "check"
                        color: window.addDownloadMessageIsError
                            ? theme.danger : theme.success
                        width: 18
                        height: 18
                    }
                    AppText {
                        Layout.fillWidth: true
                        text: window.addDownloadMessage
                        color: theme.text
                        font.pixelSize: 9
                        wrapMode: Text.WordWrap
                        maximumLineCount: 4
                        elide: Text.ElideRight
                    }
                    IconButton {
                        iconName: "close"
                        onClicked: window.addDownloadMessage = ""
                    }
                }
            }

            Rectangle {
                visible: window.addDownloadMessage.length === 0
                Layout.fillWidth: true
                Layout.preferredHeight: 38
                radius: 9
                color: theme.surfaceRaised
                RowLayout {
                    anchors.fill: parent
                    anchors.leftMargin: 12
                    anchors.rightMargin: 8
                    AppText {
                        Layout.fillWidth: true
                        text: window.translate("integrityAndRequestOptions")
                        color: theme.text
                        font.pixelSize: 9
                        font.weight: Font.DemiBold
                    }
                    UiIcon {
                        name: "chevron"
                        rotation: window.showAdvancedRequest ? -90 : 90
                        color: theme.muted
                        width: 15
                        height: 15
                    }
                }
                TapHandler {
                    onTapped: window.showAdvancedRequest = !window.showAdvancedRequest
                }
            }
            ColumnLayout {
                visible: window.showAdvancedRequest
                    && window.addDownloadMessage.length === 0
                Layout.fillWidth: true
                spacing: 8
                AppText { text: window.translate("sha256Checksum"); color: theme.muted; font.pixelSize: 8 }
                AppTextField {
                    id: checksumField
                    Layout.fillWidth: true
                    placeholderText: window.translate("optionalSha256")
                }
                AppText { text: window.translate("customRequestHeaders"); color: theme.muted; font.pixelSize: 8 }
                AppTextField {
                    id: requestHeadersField
                    Layout.fillWidth: true
                    placeholderText: "Referer: https://example.com; Cookie: name=value"
                    forceLeftToRight: true
                }
                RowLayout {
                    Layout.fillWidth: true
                    spacing: 8
                    AppTextField {
                        id: proxyOverrideField
                        Layout.fillWidth: true
                        placeholderText: window.translate("proxyOverride")
                    }
                    AppTextField {
                        id: interfaceOverrideField
                        Layout.fillWidth: true
                        placeholderText: window.translate("interfaceOverride")
                    }
                }
                AppText {
                    Layout.fillWidth: true
                    text: window.translate("sensitiveHeadersWarning")
                    color: theme.warning
                    font.pixelSize: 8
                    wrapMode: Text.WordWrap
                }
                AppButton {
                    visible: false
                    Layout.alignment: Qt.AlignRight
                    text: window.translate("resolveMediaUrl")
                    iconName: "network"
                    onClicked: protocolCapabilities.resolveMedia(urlField.text)
                }
            }

            AppText {
                text: window.translate("saveTo").toUpperCase()
                color: theme.muted
                font.pixelSize: 8
                font.weight: Font.DemiBold
                font.letterSpacing: 0.8
                Layout.topMargin: 4
            }
            RowLayout {
                Layout.fillWidth: true
                AppTextField {
                    id: directoryField
                    Layout.fillWidth: true
                    text: window.selectedDirectory.toString().replace("file:///", "")
                    readOnly: true
                    iconName: "folder"
                    forceLeftToRight: true
                }
                AppButton {
                    text: window.translate("browse")
                    iconName: "folder"
                    onClicked: folderDialog.open()
                }
            }

            AppText {
                text: window.translate("queue").toUpperCase()
                color: theme.muted
                font.pixelSize: 8
                font.weight: Font.DemiBold
                font.letterSpacing: 0.8
            }
            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 44
                radius: 10
                color: theme.background
                border.width: 1
                border.color: theme.line
                RowLayout {
                    anchors.fill: parent
                    anchors.leftMargin: 14
                    anchors.rightMargin: 11
                    UiIcon { name: "list"; color: theme.muted; width: 17; height: 17 }
                    AppText {
                        Layout.fillWidth: true
                        text: window.displayQueueName(window.selectedQueueName)
                        color: theme.text
                        font.pixelSize: 11
                    }
                    UiIcon { name: "chevron"; color: theme.subtle; width: 15; height: 15 }
                }
                TapHandler { onTapped: queuePicker.open() }
            }

            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 48
                radius: 10
                color: theme.background
                RowLayout {
                    anchors.fill: parent
                    anchors.margins: 11
                    ColumnLayout {
                        Layout.fillWidth: true
                        spacing: 1
                        AppText {
                            text: window.translate("startImmediately")
                            color: theme.text
                            font.pixelSize: 10
                            font.weight: Font.DemiBold
                        }
                        AppText {
                            text: window.translate("startImmediatelyHelper")
                            color: theme.subtle
                            font.pixelSize: 8
                        }
                    }
                    Rectangle {
                        width: 38
                        height: 22
                        radius: 11
                        color: window.startImmediately ? theme.accent : theme.line
                        Rectangle {
                            width: 16
                            height: 16
                            radius: 8
                            y: 3
                            x: window.startImmediately !== I18n.rtl ? 19 : 3
                            color: "white"
                            Behavior on x { NumberAnimation { duration: 130 } }
                        }
                    }
                }
                TapHandler { onTapped: window.startImmediately = !window.startImmediately }
            }

            Rectangle {
                visible: window.addDownloadMessage.length === 0
                Layout.fillWidth: true
                Layout.preferredHeight: 54
                radius: 10
                color: theme.background
                RowLayout {
                    anchors.fill: parent
                    anchors.margins: 11
                    UiIcon { name: "check"; color: theme.success; width: 17; height: 17 }
                    AppText {
                        Layout.fillWidth: true
                        text: window.translate("resumeHint")
                        color: theme.muted
                        font.pixelSize: 9
                        wrapMode: Text.WordWrap
                    }
                }
            }

            Item { Layout.fillHeight: true }
            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                AppButton {
                    text: window.translate("cancel")
                    onClicked: addPopup.close()
                }
                AppButton {
                    id: startDownloadButton
                    text: window.resolvingTorrent
                        ? window.translate("readingTorrent")
                        : window.resolvingMedia
                            ? window.translate("resolvingMedia")
                        : window.translate("startDownload")
                    iconName: "download"
                    tone: "primary"
                    enabled: urlField.text.trim().length > 0
                        && !window.resolvingMedia && !window.resolvingTorrent
                    onClicked: {
                        if (window.resolvedTorrentReady) {
                            window.resolvedTorrentReady = false
                        } else if (protocolCapabilities.shouldResolveTorrent(
                                       urlField.text)) {
                            window.resolvingTorrent = true
                            protocolCapabilities.resolveTorrent(urlField.text)
                            return
                        } else if (window.resolvedMediaReady) {
                            window.resolvedMediaReady = false
                        } else if (protocolCapabilities.shouldResolveMedia(urlField.text)) {
                            window.resolvingMedia = true
                            protocolCapabilities.resolveMedia(urlField.text)
                            return
                        }
                        const combinedHeaders = [
                            requestHeadersField.text,
                            window.mediaInternalHeaders
                        ].filter(value => value.length > 0).join("; ")
                        if (downloadsModel.addDownloadToQueue(
                            urlField.text,
                            window.selectedDirectory,
                            window.selectedQueueId,
                            window.startImmediately,
                            checksumField.text,
                            combinedHeaders,
                            proxyOverrideField.text,
                            interfaceOverrideField.text,
                            window.selectedTorrentFiles,
                            window.selectedExpectedSize)) {
                            urlField.clear()
                            checksumField.clear()
                            requestHeadersField.clear()
                            proxyOverrideField.clear()
                            interfaceOverrideField.clear()
                            window.selectedTorrentFiles = []
                            window.selectedExpectedSize = 0
                            window.mediaInternalHeaders = ""
                            addPopup.close()
                        }
                    }
                }
            }
        }

        onOpened: {
            window.addDownloadMessage = ""
            window.mediaInternalHeaders = ""
            urlField.forceActiveFocus()
        }
    }

    Popup {
        id: mediaFormatPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(680, window.width - 48)
        height: Math.min(650, window.height - 40)
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 150 }
                NumberAnimation { property: "scale"; from: 0.96; to: 1; duration: 180; easing.type: Easing.OutCubic }
            }
        }
        exit: Transition { NumberAnimation { property: "opacity"; duration: 110 } }
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.64) }
        background: Rectangle {
            radius: 17
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }
        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 22
            spacing: 12
            RowLayout {
                Layout.fillWidth: true
                Rectangle {
                    Layout.preferredWidth: 42
                    Layout.preferredHeight: 42
                    radius: 12
                    color: theme.accentSoft
                    UiIcon {
                        anchors.centerIn: parent
                        name: "network"
                        color: theme.accent
                        width: 20
                        height: 20
                    }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 2
                    AppText {
                        text: window.translate("selectMediaQuality")
                        color: theme.text
                        font.pixelSize: 17
                        font.weight: Font.Bold
                    }
                    AppText {
                        Layout.fillWidth: true
                        text: window.mediaFormatTitle
                        color: theme.muted
                        font.pixelSize: 10
                        elide: Text.ElideRight
                    }
                }
                IconButton {
                    iconName: "close"
                    onClicked: mediaFormatPopup.close()
                }
            }
            AppText {
                Layout.fillWidth: true
                text: window.translate("mediaQualityHelper")
                color: theme.muted
                font.pixelSize: 9
                wrapMode: Text.WordWrap
            }
            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 1
                color: theme.line
            }
            ListView {
                Layout.fillWidth: true
                Layout.fillHeight: true
                clip: true
                spacing: 7
                model: window.mediaFormats
                ScrollBar.vertical: ScrollBar {
                    policy: ScrollBar.AsNeeded
                    width: 5
                    contentItem: Rectangle { radius: 3; color: theme.subtle }
                    background: Item {}
                }
                delegate: Rectangle {
                    id: formatRow
                    required property var modelData
                    width: ListView.view.width - 8
                    height: 68
                    radius: 11
                    color: window.selectedMediaFormat.id === modelData.id
                        ? theme.accentSoft
                        : formatHover.hovered ? theme.surfaceHover : theme.surfaceRaised
                    border.width: 1
                    border.color: window.selectedMediaFormat.id === modelData.id
                        ? theme.accent : theme.line
                    HoverHandler { id: formatHover }
                    TapHandler {
                        onTapped: window.selectedMediaFormat = modelData
                    }
                    RowLayout {
                        anchors.fill: parent
                        anchors.margins: 12
                        spacing: 12
                        Rectangle {
                            Layout.preferredWidth: 100
                            Layout.preferredHeight: 40
                            radius: 9
                            color: window.selectedMediaFormat.id === modelData.id
                                ? theme.accent : theme.background
                            AppText {
                                anchors.centerIn: parent
                                text: modelData.quality
                                color: window.selectedMediaFormat.id === modelData.id
                                    ? "white" : theme.text
                                font.pixelSize: 10
                                font.weight: Font.Bold
                                elide: Text.ElideRight
                                width: parent.width - 10
                                horizontalAlignment: Text.AlignHCenter
                            }
                        }
                        ColumnLayout {
                            Layout.fillWidth: true
                            spacing: 4
                            AppText {
                                Layout.fillWidth: true
                                text: modelData.details
                                color: theme.text
                                font.pixelSize: 10
                                font.weight: Font.DemiBold
                                elide: Text.ElideRight
                            }
                            AppText {
                                text: modelData.size
                                color: theme.muted
                                font.pixelSize: 9
                            }
                        }
                        Rectangle {
                            Layout.preferredWidth: formatKind.implicitWidth + 16
                            Layout.preferredHeight: 25
                            radius: 8
                            color: modelData.kind === "combined"
                                ? theme.accentSoft : theme.background
                            border.width: 1
                            border.color: modelData.kind === "combined"
                                ? theme.accent : theme.line
                            AppText {
                                id: formatKind
                                anchors.centerIn: parent
                                text: modelData.kind === "combined"
                                    ? window.translate("videoAndAudio")
                                    : modelData.kind === "video"
                                        ? window.translate("videoOnly")
                                        : window.translate("audioOnly")
                                color: modelData.kind === "combined"
                                    ? theme.accent : theme.muted
                                font.pixelSize: 8
                                font.weight: Font.DemiBold
                            }
                        }
                    }
                }
            }
            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 1
                color: theme.line
            }
            RowLayout {
                Layout.fillWidth: true
                AppText {
                    Layout.fillWidth: true
                    text: window.mediaFormats.length + " "
                        + window.translate("formatsAvailable")
                    color: theme.subtle
                    font.pixelSize: 9
                }
                AppButton {
                    text: window.translate("cancel")
                    onClicked: mediaFormatPopup.close()
                }
                AppButton {
                    text: window.translate("downloadSelectedFormat")
                    iconName: "download"
                    tone: "primary"
                    enabled: window.selectedMediaFormat.url !== undefined
                    onClicked: {
                        urlField.text = window.selectedMediaFormat.url
                        requestHeadersField.text =
                            window.selectedMediaFormat.headers || ""
                        window.mediaInternalHeaders =
                            window.selectedMediaFormat.internalHeaders || ""
                        window.selectedExpectedSize =
                            window.selectedMediaFormat.sizeBytes || 0
                        window.showAdvancedRequest = true
                        window.resolvedMediaReady = true
                        mediaFormatPopup.close()
                        startDownloadButton.clicked()
                    }
                }
            }
        }
    }

    Popup {
        id: torrentFilePopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(700, window.width - 48)
        height: Math.min(650, window.height - 40)
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 150 }
                NumberAnimation { property: "scale"; from: 0.96; to: 1; duration: 180; easing.type: Easing.OutCubic }
            }
        }
        exit: Transition { NumberAnimation { property: "opacity"; duration: 110 } }
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.64) }
        background: Rectangle {
            radius: 17
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }
        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 22
            spacing: 12
            RowLayout {
                Layout.fillWidth: true
                Rectangle {
                    Layout.preferredWidth: 42
                    Layout.preferredHeight: 42
                    radius: 12
                    color: theme.accentSoft
                    UiIcon {
                        anchors.centerIn: parent
                        name: "list"
                        color: theme.accent
                        width: 20
                        height: 20
                    }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 2
                    AppText {
                        text: window.translate("selectTorrentFiles")
                        color: theme.text
                        font.pixelSize: 17
                        font.weight: Font.Bold
                    }
                    AppText {
                        Layout.fillWidth: true
                        text: window.torrentTitle
                        color: theme.muted
                        font.pixelSize: 10
                        elide: Text.ElideRight
                    }
                }
                IconButton {
                    iconName: "close"
                    onClicked: torrentFilePopup.close()
                }
            }
            RowLayout {
                Layout.fillWidth: true
                AppText {
                    Layout.fillWidth: true
                    text: window.translate("torrentFilesHelper")
                    color: theme.muted
                    font.pixelSize: 9
                }
                AppButton {
                    text: window.translate("selectAll")
                    onClicked: window.setAllTorrentFiles(true)
                }
                AppButton {
                    text: window.translate("selectNone")
                    onClicked: window.setAllTorrentFiles(false)
                }
            }
            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 1
                color: theme.line
            }
            ListView {
                Layout.fillWidth: true
                Layout.fillHeight: true
                clip: true
                spacing: 6
                model: window.torrentFiles
                ScrollBar.vertical: ScrollBar {
                    policy: ScrollBar.AsNeeded
                    width: 5
                    contentItem: Rectangle { radius: 3; color: theme.subtle }
                    background: Item {}
                }
                delegate: Rectangle {
                    id: torrentFileRow
                    required property var modelData
                    required property int index
                    width: ListView.view.width - 8
                    height: 62
                    radius: 10
                    color: modelData.selected
                        ? theme.accentSoft
                        : torrentHover.hovered ? theme.surfaceHover : theme.surfaceRaised
                    border.width: 1
                    border.color: modelData.selected ? theme.accent : theme.line
                    HoverHandler { id: torrentHover }
                    TapHandler {
                        onTapped: window.toggleTorrentFile(torrentFileRow.index)
                    }
                    RowLayout {
                        anchors.fill: parent
                        anchors.margins: 11
                        spacing: 11
                        Rectangle {
                            Layout.preferredWidth: 22
                            Layout.preferredHeight: 22
                            radius: 6
                            color: modelData.selected ? theme.accent : theme.background
                            border.width: 1
                            border.color: modelData.selected ? theme.accent : theme.line
                            UiIcon {
                                visible: modelData.selected
                                anchors.centerIn: parent
                                name: "check"
                                color: "white"
                                width: 13
                                height: 13
                            }
                        }
                        ColumnLayout {
                            Layout.fillWidth: true
                            spacing: 3
                            AppText {
                                Layout.fillWidth: true
                                text: modelData.name
                                color: theme.text
                                font.pixelSize: 10
                                font.weight: Font.DemiBold
                                elide: Text.ElideMiddle
                            }
                            AppText {
                                Layout.fillWidth: true
                                text: modelData.path
                                color: theme.muted
                                font.pixelSize: 8
                                elide: Text.ElideMiddle
                            }
                        }
                        AppText {
                            text: modelData.size
                            color: theme.muted
                            font.pixelSize: 9
                            font.weight: Font.DemiBold
                        }
                    }
                }
            }
            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 1
                color: theme.line
            }
            RowLayout {
                Layout.fillWidth: true
                AppText {
                    Layout.fillWidth: true
                    text: window.torrentSelectedCount() + " / "
                        + window.torrentFiles.length + " "
                        + window.translate("filesSelected")
                    color: theme.subtle
                    font.pixelSize: 9
                }
                AppButton {
                    text: window.translate("cancel")
                    onClicked: torrentFilePopup.close()
                }
                AppButton {
                    text: window.translate("downloadSelectedFiles")
                    iconName: "download"
                    tone: "primary"
                    enabled: window.torrentSelectedCount() > 0
                    onClicked: {
                        let selected = []
                        let selectedSize = 0
                        for (let index = 0; index < window.torrentFiles.length; ++index) {
                            if (window.torrentFiles[index].selected) {
                                selected.push(window.torrentFiles[index].index)
                                selectedSize += window.torrentFiles[index].sizeBytes || 0
                            }
                        }
                        window.selectedTorrentFiles = selected
                        window.selectedExpectedSize = selectedSize
                        window.resolvedTorrentReady = true
                        torrentFilePopup.close()
                        startDownloadButton.clicked()
                    }
                }
            }
        }
    }

    Popup {
        id: batchReviewPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(900, window.width - 48)
        height: Math.min(700, window.height - 40)
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.62) }
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 140 }
                NumberAnimation { property: "scale"; from: 0.97; to: 1; duration: 170 }
            }
        }
        exit: Transition { NumberAnimation { property: "opacity"; duration: 100 } }
        background: Rectangle {
            radius: 18
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }

        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 20
            spacing: 13

            RowLayout {
                Layout.fillWidth: true
                spacing: 12
                Rectangle {
                    Layout.preferredWidth: 42
                    Layout.preferredHeight: 42
                    radius: 12
                    color: theme.accentSoft
                    UiIcon {
                        anchors.centerIn: parent
                        name: "list"
                        color: theme.accent
                        width: 20
                        height: 20
                    }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 2
                    AppText {
                        text: window.translate("reviewBatchDownloads")
                        color: theme.text
                        font.pixelSize: 17
                        font.weight: Font.Bold
                    }
                    AppText {
                        Layout.fillWidth: true
                        text: window.batchSourceTitle.length > 0
                            ? window.batchSourceTitle
                            : window.translate("browserExtensionBatch")
                        color: theme.muted
                        font.pixelSize: 9
                        elide: Text.ElideRight
                    }
                }
                Rectangle {
                    Layout.preferredWidth: batchCountText.implicitWidth + 18
                    Layout.preferredHeight: 28
                    radius: 9
                    color: theme.accentSoft
                    AppText {
                        id: batchCountText
                        anchors.centerIn: parent
                        text: window.batchSelectedCount() + " / "
                            + window.batchItems.length
                        color: theme.accent
                        font.pixelSize: 9
                        font.weight: Font.Bold
                    }
                }
                IconButton {
                    iconName: "close"
                    onClicked: batchReviewPopup.close()
                }
            }

            AppText {
                Layout.fillWidth: true
                text: window.translate("batchReviewHelper")
                color: theme.muted
                font.pixelSize: 10
                wrapMode: Text.WordWrap
            }

            RowLayout {
                Layout.fillWidth: true
                spacing: 8
                AppButton {
                    text: window.translate("selectAll")
                    onClicked: window.setAllBatchItems(true)
                }
                AppButton {
                    text: window.translate("selectNone")
                    onClicked: window.setAllBatchItems(false)
                }
                Item { Layout.fillWidth: true }
                AppText {
                    text: window.translate("browserCapturePrivacy")
                    color: theme.subtle
                    font.pixelSize: 8
                }
            }

            Rectangle {
                Layout.fillWidth: true
                Layout.fillHeight: true
                radius: 12
                color: theme.background
                border.width: 1
                border.color: theme.line

                ListView {
                    anchors.fill: parent
                    anchors.margins: 8
                    clip: true
                    spacing: 7
                    model: window.batchItems
                    ScrollBar.vertical: ScrollBar {
                        policy: ScrollBar.AsNeeded
                        width: 5
                        contentItem: Rectangle { radius: 3; color: theme.subtle }
                        background: Item {}
                    }
                    delegate: Rectangle {
                        required property var modelData
                        width: ListView.view.width
                        height: 76
                        radius: 10
                        color: modelData.selected
                            ? theme.accentSoft : theme.surface
                        border.width: 1
                        border.color: modelData.selected
                            ? theme.accent : theme.line

                        RowLayout {
                            anchors.fill: parent
                            anchors.margins: 11
                            spacing: 11
                            Rectangle {
                                Layout.preferredWidth: 20
                                Layout.preferredHeight: 20
                                radius: 6
                                color: modelData.selected
                                    ? theme.accent : "transparent"
                                border.width: 1
                                border.color: modelData.selected
                                    ? theme.accent : theme.muted
                                UiIcon {
                                    visible: modelData.selected
                                    anchors.centerIn: parent
                                    name: "check"
                                    color: "white"
                                    width: 13
                                    height: 13
                                }
                                TapHandler {
                                    onTapped: window.setBatchItemSelected(
                                        index, !modelData.selected
                                    )
                                }
                            }
                            ColumnLayout {
                                Layout.fillWidth: true
                                spacing: 3
                                RowLayout {
                                    Layout.fillWidth: true
                                    AppText {
                                        Layout.fillWidth: true
                                        text: modelData.title
                                        color: theme.text
                                        font.pixelSize: 10
                                        font.weight: Font.DemiBold
                                        elide: Text.ElideRight
                                    }
                                    Rectangle {
                                        Layout.preferredWidth:
                                            batchKindText.implicitWidth + 12
                                        Layout.preferredHeight: 20
                                        radius: 7
                                        color: theme.surfaceRaised
                                        AppText {
                                            id: batchKindText
                                            anchors.centerIn: parent
                                            text: modelData.kind.toUpperCase()
                                            color: theme.accent
                                            font.pixelSize: 7
                                            font.weight: Font.Bold
                                        }
                                    }
                                }
                                AppText {
                                    Layout.fillWidth: true
                                    text: modelData.url
                                    color: theme.subtle
                                    font.pixelSize: 8
                                    elide: Text.ElideMiddle
                                }
                                AppText {
                                    visible: modelData.pageUrl.length > 0
                                        && modelData.pageUrl !== modelData.url
                                    Layout.fillWidth: true
                                    text: window.translate("capturedFrom") + " "
                                        + modelData.pageUrl
                                    color: theme.muted
                                    font.pixelSize: 7
                                    elide: Text.ElideMiddle
                                }
                            }
                            IconButton {
                                iconName: "trash"
                                danger: true
                                onClicked: window.removeBatchItem(index)
                            }
                        }
                    }
                }
            }

            Rectangle {
                Layout.fillWidth: true
                Layout.preferredHeight: 104
                radius: 12
                color: theme.surfaceRaised
                border.width: 1
                border.color: theme.line

                ColumnLayout {
                    anchors.fill: parent
                    anchors.margins: 12
                    spacing: 9
                    RowLayout {
                        Layout.fillWidth: true
                        spacing: 9
                        ColumnLayout {
                            Layout.fillWidth: true
                            spacing: 2
                            AppText {
                                text: window.translate("saveTo")
                                color: theme.subtle
                                font.pixelSize: 8
                            }
                            AppText {
                                Layout.fillWidth: true
                                text: window.selectedDirectory.toString()
                                    .replace("file:///", "")
                                color: theme.text
                                font.pixelSize: 9
                                elide: Text.ElideMiddle
                            }
                        }
                        AppButton {
                            text: window.translate("browse")
                            iconName: "folder"
                            onClicked: folderDialog.open()
                        }
                        Rectangle {
                            Layout.preferredWidth: 1
                            Layout.preferredHeight: 30
                            color: theme.line
                        }
                        ColumnLayout {
                            Layout.preferredWidth: 150
                            spacing: 2
                            AppText {
                                text: window.translate("queue")
                                color: theme.subtle
                                font.pixelSize: 8
                            }
                            AppText {
                                Layout.fillWidth: true
                                text: window.displayQueueName(
                                    window.selectedQueueName
                                )
                                color: theme.text
                                font.pixelSize: 9
                                font.weight: Font.DemiBold
                                elide: Text.ElideRight
                            }
                        }
                        AppButton {
                            text: window.translate("selectQueue")
                            iconName: "list"
                            onClicked: queuePicker.open()
                        }
                    }
                    RowLayout {
                        Layout.fillWidth: true
                        AppText {
                            Layout.fillWidth: true
                            text: window.translate("batchStartImmediatelyHelper")
                            color: theme.muted
                            font.pixelSize: 8
                        }
                        AppText {
                            text: window.translate("startImmediately")
                            color: theme.text
                            font.pixelSize: 9
                            font.weight: Font.DemiBold
                        }
                        Rectangle {
                            width: 38
                            height: 22
                            radius: 11
                            color: window.batchStartImmediately
                                ? theme.accent : theme.line
                            Rectangle {
                                width: 16
                                height: 16
                                radius: 8
                                y: 3
                                x: window.batchStartImmediately !== I18n.rtl
                                    ? 19 : 3
                                color: "white"
                                Behavior on x {
                                    NumberAnimation { duration: 120 }
                                }
                            }
                            TapHandler {
                                onTapped: window.batchStartImmediately =
                                    !window.batchStartImmediately
                            }
                        }
                    }
                }
            }

            RowLayout {
                Layout.fillWidth: true
                AppText {
                    Layout.fillWidth: true
                    text: window.translate("batchNoAutomaticStart")
                    color: theme.subtle
                    font.pixelSize: 8
                }
                AppButton {
                    text: window.translate("cancel")
                    onClicked: batchReviewPopup.close()
                }
                AppButton {
                    text: window.translate("addSelectedDownloads")
                    iconName: "download"
                    tone: "primary"
                    enabled: window.batchSelectedCount() > 0
                        && window.selectedQueueId >= 0
                    onClicked: window.importBatchItems()
                }
            }
        }
    }

    Popup {
        id: queuePicker
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: 380
        height: Math.min(420, 82 + queuesModel.count * 52)
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 130 }
                NumberAnimation { property: "scale"; from: 0.97; to: 1; duration: 150 }
            }
        }
        exit: Transition { NumberAnimation { property: "opacity"; duration: 90 } }
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.42) }
        background: Rectangle {
            radius: 15
            color: theme.surfaceRaised
            border.width: 1
            border.color: theme.line
        }
        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 14
            spacing: 8
            AppText {
                text: window.translate("selectQueue")
                color: theme.text
                font.pixelSize: 13
                font.weight: Font.Bold
            }
            ListView {
                Layout.fillWidth: true
                Layout.fillHeight: true
                model: queuesModel
                spacing: 5
                delegate: Rectangle {
                    required property int queueId
                    required property string name
                    required property int downloadCount
                    width: ListView.view.width
                    height: 44
                    radius: 9
                    color: window.selectedQueueId === queueId
                        ? theme.accentSoft : theme.surface
                    RowLayout {
                        anchors.fill: parent
                        anchors.margins: 11
                        UiIcon { name: "list"; color: theme.accent; width: 16; height: 16 }
                        AppText {
                            Layout.fillWidth: true
                            text: window.displayQueueName(name)
                            color: theme.text
                            font.pixelSize: 10
                            font.weight: Font.DemiBold
                        }
                        AppText {
                            text: downloadCount
                            color: theme.muted
                            font.pixelSize: 9
                        }
                    }
                    TapHandler {
                        onTapped: {
                            window.selectedQueueId = queueId
                            window.selectedQueueName = name
                            queuePicker.close()
                        }
                    }
                }
            }
        }
    }

    Popup {
        id: quitConfirmPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(440, window.width - 48)
        height: 270
        padding: 0
        modal: true
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.62) }
        background: Rectangle {
            radius: 17
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }
        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 22
            spacing: 12
            Rectangle {
                Layout.preferredWidth: 46
                Layout.preferredHeight: 46
                Layout.alignment: Qt.AlignHCenter
                radius: 14
                color: theme.surfaceRaised
                border.width: 1
                border.color: theme.danger
                UiIcon {
                    anchors.centerIn: parent
                    name: "alert"
                    color: theme.danger
                    width: 21
                    height: 21
                }
            }
            AppText {
                Layout.fillWidth: true
                text: window.translate("quitConfirmTitle")
                color: theme.text
                font.pixelSize: 17
                font.weight: Font.Bold
                horizontalAlignment: Text.AlignHCenter
            }
            AppText {
                Layout.fillWidth: true
                text: downloadsModel.activeCount > 0
                    ? window.translate("quitConfirmActive")
                    : window.translate("quitConfirmBody")
                color: downloadsModel.activeCount > 0 ? theme.warning : theme.muted
                font.pixelSize: 10
                wrapMode: Text.WordWrap
                horizontalAlignment: Text.AlignHCenter
            }
            Item { Layout.fillHeight: true }
            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                AppButton {
                    text: window.translate("cancel")
                    onClicked: quitConfirmPopup.close()
                }
                AppButton {
                    text: window.translate("exitPidm")
                    iconName: "close"
                    tone: "danger"
                    onClicked: window.confirmQuitAnimated()
                }
                Item { Layout.fillWidth: true }
            }
        }
    }

    Popup {
        id: updateAvailablePopup
        parent: Overlay.overlay
        x: window.width - width - 22
        y: titleBar.height + 16
        width: Math.min(430, window.width - 44)
        height: 260
        padding: 0
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 150 }
                NumberAnimation { property: "scale"; from: 0.96; to: 1; duration: 170 }
            }
        }
        exit: Transition { NumberAnimation { property: "opacity"; duration: 100 } }
        background: Rectangle {
            radius: 16
            color: theme.surfaceRaised
            border.width: 1
            border.color: theme.accent
        }
        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 18
            spacing: 11
            RowLayout {
                Layout.fillWidth: true
                spacing: 11
                Rectangle {
                    Layout.preferredWidth: 40
                    Layout.preferredHeight: 40
                    radius: 12
                    color: theme.accentSoft
                    UiIcon {
                        anchors.centerIn: parent
                        name: "download"
                        color: theme.accent
                        width: 19
                        height: 19
                    }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 2
                    AppText {
                        text: window.translate("updateAvailable")
                        color: theme.text
                        font.pixelSize: 14
                        font.weight: Font.Bold
                    }
                    AppText {
                        text: updateChecker.currentVersion + "  →  "
                            + window.latestUpdateVersion
                        color: theme.accent
                        font.pixelSize: 9
                        font.weight: Font.DemiBold
                    }
                }
                IconButton {
                    iconName: "close"
                    onClicked: updateAvailablePopup.close()
                }
            }
            AppText {
                Layout.fillWidth: true
                text: window.translate("updateAvailableHelper")
                color: theme.muted
                font.pixelSize: 9
                wrapMode: Text.WordWrap
            }
            Rectangle {
                Layout.fillWidth: true
                Layout.fillHeight: true
                radius: 10
                color: theme.background
                AppText {
                    anchors.fill: parent
                    anchors.margins: 10
                    text: window.latestUpdateNotes.length > 0
                        ? window.latestUpdateNotes
                        : window.translate("updateNoReleaseNotes")
                    color: theme.subtle
                    font.pixelSize: 8
                    wrapMode: Text.WordWrap
                    maximumLineCount: 4
                    elide: Text.ElideRight
                }
            }
            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                AppButton {
                    text: window.translate("later")
                    onClicked: updateAvailablePopup.close()
                }
                AppButton {
                    text: window.translate("viewRelease")
                    iconName: "external"
                    tone: "primary"
                    onClicked: {
                        updateChecker.openRelease(window.latestUpdateUrl)
                        updateAvailablePopup.close()
                    }
                }
            }
        }
    }

    Popup {
        id: quickSettings
        parent: Overlay.overlay
        x: window.width - width - 64
        y: 52
        width: 284
        height: 248
        padding: 0
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        enter: Transition {
            ParallelAnimation {
                NumberAnimation { property: "opacity"; from: 0; to: 1; duration: 120 }
                NumberAnimation { property: "scale"; from: 0.96; to: 1; duration: 140 }
            }
        }
        exit: Transition { NumberAnimation { property: "opacity"; duration: 90 } }

        background: Rectangle {
            radius: 14
            color: theme.surfaceRaised
            border.width: 1
            border.color: theme.line
        }

        contentItem: ColumnLayout {
            anchors.fill: parent
            anchors.margins: 16
            spacing: 10
            AppText {
                text: window.translate("appearance")
                color: theme.text
                font.pixelSize: 12
                font.weight: Font.DemiBold
            }
            RowLayout {
                Layout.fillWidth: true
                Repeater {
                    model: [["midnight", "#0A0D14"], ["light", "#F3F5F9"], ["ocean", "#061318"]]
                    delegate: Rectangle {
                        required property var modelData
                        Layout.fillWidth: true
                        Layout.preferredHeight: 52
                        radius: 10
                        color: modelData[1]
                        border.width: 2
                        border.color: theme.name === modelData[0] ? theme.accent : theme.line
                        TapHandler { onTapped: theme.select(modelData[0]) }
                    }
                }
            }
            Rectangle { Layout.fillWidth: true; Layout.preferredHeight: 1; color: theme.line }
            AppText {
                text: window.translate("language")
                color: theme.text
                font.pixelSize: 12
                font.weight: Font.DemiBold
            }
            GridLayout {
                Layout.fillWidth: true
                columns: 2
                rowSpacing: 7
                columnSpacing: 7
                Repeater {
                    model: [["en", "English"], ["fa", "فارسی"], ["ru", "Русский"], ["zh", "中文"]]
                    delegate: AppButton {
                        required property var modelData
                        Layout.fillWidth: true
                        implicitHeight: 33
                        text: modelData[1]
                        tone: I18n.language === modelData[0] ? "primary" : "neutral"
                        onClicked: I18n.language = modelData[0]
                    }
                }
            }
        }
    }

    MouseArea {
        anchors.left: parent.left
        anchors.top: parent.top
        anchors.bottom: parent.bottom
        width: 5
        z: 100
        cursorShape: Qt.SizeHorCursor
        onPressed: window.startSystemResize(Qt.LeftEdge)
    }
    MouseArea {
        anchors.right: parent.right
        anchors.top: parent.top
        anchors.bottom: parent.bottom
        width: 5
        z: 100
        cursorShape: Qt.SizeHorCursor
        onPressed: window.startSystemResize(Qt.RightEdge)
    }
    MouseArea {
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.bottom: parent.bottom
        height: 5
        z: 100
        cursorShape: Qt.SizeVerCursor
        onPressed: window.startSystemResize(Qt.BottomEdge)
    }
}
