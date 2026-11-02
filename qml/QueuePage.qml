import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import PIDM 1.0
import "components"

Item {
    id: page

    property bool scheduleView: false
    property int editingRow: -1
    property string selectedPostAction: "none"
    property string scheduleMode: "daily"
    signal openDetailsRequested(int downloadId)
    opacity: visible ? 1 : 0
    Behavior on opacity { NumberAnimation { duration: 180 } }

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

    function editQueue(row) {
        const data = queuesModel.details(row)
        editingRow = row
        queueNameField.text = data.name || ""
        queueEnabled.checked = data.enabled || false
        startTimeField.text = data.startTime || "08:00"
        stopTimeField.text = data.stopTime || ""
        daysField.text = data.days || ""
        syncScheduleMode()
        concurrentField.text = (data.maxConcurrent || 2).toString()
        pauseField.text = (data.pauseBetween || 0).toString()
        selectedPostAction = data.postAction || "none"
        editorPopup.open()
    }

    function syncScheduleMode() {
        const value = daysField.text.trim()
        scheduleMode = value.match(/^\d{4}-\d{2}-\d{2}$/)
            ? "once" : value.length ? "weekly" : "daily"
    }

    function toggleWeekday(day) {
        let days = daysField.text.split(",").map(value => value.trim()).filter(Boolean)
        const position = days.indexOf(day)
        if (position >= 0)
            days.splice(position, 1)
        else
            days.push(day)
        const order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        days.sort((left, right) => order.indexOf(left) - order.indexOf(right))
        daysField.text = days.join(",")
    }

    function hasWeekday(day) {
        return daysField.text.split(",").map(value => value.trim()).indexOf(day) >= 0
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 30
        spacing: 18

        RowLayout {
            Layout.fillWidth: true
            ColumnLayout {
                spacing: 3
                AppText {
                    text: page.scheduleView ? page.tr("schedulerTitle") : page.tr("queuesTitle")
                    color: theme.text
                    font.pixelSize: 25
                    font.weight: Font.Bold
                }
                AppText {
                    text: page.scheduleView ? page.tr("schedulerSubtitle") : page.tr("queuesSubtitle")
                    color: theme.muted
                    font.pixelSize: 11
                }
            }
            Item { Layout.fillWidth: true }
            AppButton {
                text: page.tr("newQueue")
                iconName: "plus"
                tone: "primary"
                onClicked: {
                    page.editingRow = -1
                    queueNameField.text = ""
                    queueEnabled.checked = page.scheduleView
                    startTimeField.text = "08:00"
                    stopTimeField.text = ""
                    daysField.text = ""
                    page.scheduleMode = "daily"
                    concurrentField.text = "2"
                    pauseField.text = "0"
                    page.selectedPostAction = "none"
                    editorPopup.open()
                }
            }
        }

        Rectangle {
            Layout.fillWidth: true
            Layout.fillHeight: true
            radius: 15
            color: theme.surface
            border.width: 1
            border.color: theme.line

            ListView {
                anchors.fill: parent
                anchors.margins: 12
                model: queuesModel
                spacing: 9
                clip: true

                delegate: Rectangle {
                    required property int queueId
                    required property string name
                    required property int downloadCount
                    required property int activeCount
                    required property int waitingCount
                    required property bool scheduleEnabled
                    required property string startTime
                    required property string stopTime
                    required property string daysText
                    required property int maxConcurrent
                    required property bool running
                    required property int errorCount
                    required property int index
                    property bool expanded: false
                    property var queueFiles: {
                        const revision = queuesModel.revision
                        return queuesModel.downloadsForQueue(queueId)
                    }
                    readonly property int headerHeight: page.scheduleView ? 116 : 98

                    width: ListView.view.width
                    height: headerHeight + (
                        expanded ? 42 + queueFiles.length * 50 : 0
                    )
                    radius: 12
                    color: queueHover.hovered ? theme.surfaceHover : theme.surfaceRaised
                    border.width: 1
                    border.color: running ? theme.accent : theme.line
                    HoverHandler { id: queueHover }
                    Behavior on height { NumberAnimation { duration: 180; easing.type: Easing.OutCubic } }

                    ColumnLayout {
                        anchors.fill: parent
                        anchors.margins: 12
                        spacing: 4

                        RowLayout {
                            Layout.fillWidth: true
                            Layout.preferredHeight: headerHeight - 24
                            spacing: 13

                            Rectangle {
                                Layout.preferredWidth: 42
                                Layout.preferredHeight: 42
                                radius: 11
                                color: running ? theme.accentSoft : theme.surface
                                UiIcon {
                                    anchors.centerIn: parent
                                    name: page.scheduleView ? "settings" : "list"
                                    color: running ? theme.accent : theme.muted
                                    width: 19
                                    height: 19
                                }
                            }

                            ColumnLayout {
                                Layout.fillWidth: true
                                spacing: 5
                                RowLayout {
                                    AppText {
                                        text: page.displayQueueName(name)
                                        color: theme.text
                                        font.pixelSize: 13
                                        font.weight: Font.Bold
                                    }
                                    Rectangle {
                                        width: queueState.implicitWidth + 14
                                        height: 21
                                        radius: 10
                                        color: running ? theme.accentSoft : theme.surface
                                        AppText {
                                            id: queueState
                                            anchors.centerIn: parent
                                            text: running ? page.tr("running") : page.tr("stopped")
                                            color: running ? theme.success : theme.subtle
                                            font.pixelSize: 8
                                            font.weight: Font.DemiBold
                                        }
                                    }
                                }
                                AppText {
                                    visible: !page.scheduleView
                                    text: downloadCount + " " + page.tr("files")
                                        + "  ·  " + activeCount + " " + page.tr("active").toLowerCase()
                                        + "  ·  " + waitingCount + " " + page.tr("waiting")
                                    color: theme.muted
                                    font.pixelSize: 9
                                }
                                AppText {
                                    visible: page.scheduleView
                                    text: scheduleEnabled
                                        ? page.tr("startsAt") + " " + startTime
                                            + (stopTime ? "  ·  " + page.tr("stopsAt") + " " + stopTime : "")
                                        : page.tr("scheduleDisabled")
                                    color: scheduleEnabled ? theme.text : theme.subtle
                                    font.pixelSize: 10
                                }
                                AppText {
                                    visible: page.scheduleView && scheduleEnabled
                                    text: daysText + "  ·  " + maxConcurrent + " " + page.tr("concurrent")
                                    color: theme.muted
                                    font.pixelSize: 9
                                }
                            }

                            AppButton {
                                visible: errorCount > 0
                                text: page.tr("retryFailed")
                                iconName: "play"
                                onClicked: queuesModel.retryQueue(index)
                            }
                            AppButton {
                                text: running ? page.tr("stop") : page.tr("start")
                                iconName: running ? "pause" : "play"
                                tone: running ? "neutral" : "primary"
                                onClicked: running
                                    ? queuesModel.stopQueue(index)
                                    : queuesModel.startQueue(index)
                            }
                            IconButton {
                                visible: !page.scheduleView
                                iconName: "chevron"
                                rotation: expanded ? -90 : 90
                                enabled: queueFiles.length > 0
                                onClicked: expanded = !expanded
                                Behavior on rotation { NumberAnimation { duration: 150 } }
                            }
                            IconButton {
                                iconName: "settings"
                                onClicked: page.editQueue(index)
                            }
                            IconButton {
                                visible: name !== "Main Queue"
                                iconName: "trash"
                                danger: true
                                onClicked: queuesModel.deleteQueue(index)
                            }
                        }

                        ColumnLayout {
                            visible: expanded && !page.scheduleView
                            Layout.fillWidth: true
                            spacing: 3
                            Rectangle {
                                Layout.fillWidth: true
                                Layout.preferredHeight: 1
                                color: theme.line
                            }
                            Repeater {
                                model: queueFiles
                                delegate: Rectangle {
                                    required property var modelData
                                    Layout.fillWidth: true
                                    Layout.preferredHeight: 47
                                    radius: 8
                                    color: itemHover.hovered ? theme.surfaceHover : "transparent"
                                    HoverHandler { id: itemHover }
                                    RowLayout {
                                        anchors.fill: parent
                                        anchors.leftMargin: 10
                                        anchors.rightMargin: 4
                                        spacing: 8
                                        AppText {
                                            Layout.preferredWidth: 22
                                            text: (index + 1).toString().padStart(2, "0")
                                            color: theme.subtle
                                            font.pixelSize: 8
                                        }
                                        ColumnLayout {
                                            Layout.fillWidth: true
                                            spacing: 2
                                            AppText {
                                                Layout.fillWidth: true
                                                text: modelData.name
                                                color: theme.text
                                                font.pixelSize: 10
                                                font.weight: Font.DemiBold
                                                horizontalAlignment: I18n.rtl
                                                    ? Text.AlignRight : Text.AlignLeft
                                                elide: Text.ElideMiddle
                                            }
                                            AppText {
                                                Layout.fillWidth: true
                                                text: page.statusLabel(modelData.status)
                                                    + "  ·  " + modelData.progress + "%"
                                                color: theme.muted
                                                font.pixelSize: 8
                                                horizontalAlignment: I18n.rtl
                                                    ? Text.AlignRight : Text.AlignLeft
                                            }
                                        }
                                        IconButton {
                                            iconName: "chevron"
                                            rotation: -90
                                            enabled: index > 0
                                            onClicked: queuesModel.moveDownload(
                                                queueId, modelData.id, -1
                                            )
                                        }
                                        IconButton {
                                            iconName: "chevron"
                                            rotation: 90
                                            enabled: index < queueFiles.length - 1
                                            onClicked: queuesModel.moveDownload(
                                                queueId, modelData.id, 1
                                            )
                                        }
                                        IconButton {
                                            iconName: "external"
                                            onClicked: page.openDetailsRequested(modelData.id)
                                        }
                                        IconButton {
                                            visible: name !== "Main Queue"
                                            iconName: "close"
                                            danger: true
                                            onClicked: queuesModel.removeDownload(modelData.id)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Popup {
        id: editorPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(590, page.width - 40)
        height: Math.min(700, page.height - 24)
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
            spacing: 11
            RowLayout {
                Layout.fillWidth: true
                AppText {
                    Layout.fillWidth: true
                    text: page.editingRow < 0 ? page.tr("newQueue") : page.tr("editQueue")
                    color: theme.text
                    font.pixelSize: 17
                    font.weight: Font.Bold
                }
                IconButton { iconName: "close"; onClicked: editorPopup.close() }
            }
            AppText { text: page.tr("queueName"); color: theme.muted; font.pixelSize: 9 }
            AppTextField { id: queueNameField; Layout.fillWidth: true }

            RowLayout {
                Layout.fillWidth: true
                spacing: 12
                ColumnLayout {
                    Layout.fillWidth: true
                    AppText { text: page.tr("maxConcurrent"); color: theme.muted; font.pixelSize: 9 }
                    AppTextField {
                        id: concurrentField
                        Layout.fillWidth: true
                        validator: IntValidator { bottom: 1; top: 32 }
                    }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    AppText { text: page.tr("pauseBetween"); color: theme.muted; font.pixelSize: 9 }
                    AppTextField {
                        id: pauseField
                        Layout.fillWidth: true
                        validator: IntValidator { bottom: 0; top: 3600 }
                    }
                }
            }

            Rectangle {
                id: queueEnabled
                property bool checked: false
                Layout.fillWidth: true
                Layout.preferredHeight: 52
                radius: 11
                color: checked ? theme.accentSoft : theme.surfaceRaised
                border.width: 1
                border.color: checked ? theme.accent : theme.line
                RowLayout {
                    anchors.fill: parent
                    anchors.margins: 12
                    AppText {
                        Layout.fillWidth: true
                        text: page.tr("enableSchedule")
                        color: theme.text
                        font.pixelSize: 10
                        font.weight: Font.DemiBold
                    }
                    Rectangle {
                        width: 38
                        height: 22
                        radius: 11
                        color: queueEnabled.checked ? theme.accent : theme.line
                        Rectangle {
                            width: 16
                            height: 16
                            radius: 8
                            y: 3
                            x: queueEnabled.checked !== I18n.rtl ? 19 : 3
                            color: "white"
                            Behavior on x { NumberAnimation { duration: 130 } }
                        }
                    }
                }
                TapHandler { onTapped: queueEnabled.checked = !queueEnabled.checked }
            }

            Rectangle { Layout.fillWidth: true; Layout.preferredHeight: 1; color: theme.line }
            AppText {
                text: page.tr("schedule")
                color: queueEnabled.checked ? theme.text : theme.subtle
                font.pixelSize: 12
                font.weight: Font.DemiBold
            }
            RowLayout {
                Layout.fillWidth: true
                enabled: queueEnabled.checked
                opacity: enabled ? 1 : 0.45
                spacing: 12
                ColumnLayout {
                    Layout.fillWidth: true
                    AppText { text: page.tr("startTime"); color: theme.muted; font.pixelSize: 9 }
                    AppTextField { id: startTimeField; Layout.fillWidth: true; placeholderText: "08:00" }
                }
                ColumnLayout {
                    Layout.fillWidth: true
                    AppText { text: page.tr("stopTimeOptional"); color: theme.muted; font.pixelSize: 9 }
                    AppTextField { id: stopTimeField; Layout.fillWidth: true; placeholderText: "22:00" }
                }
            }
            AppText {
                text: page.tr("scheduleFrequency")
                color: queueEnabled.checked ? theme.muted : theme.subtle
                font.pixelSize: 9
            }
            RowLayout {
                Layout.fillWidth: true
                enabled: queueEnabled.checked
                opacity: enabled ? 1 : 0.45
                spacing: 8
                Repeater {
                    model: [
                        ["daily", "everyDay"],
                        ["weekly", "selectedDays"],
                        ["once", "oneDate"]
                    ]
                    delegate: Rectangle {
                        required property var modelData
                        Layout.fillWidth: true
                        Layout.preferredHeight: 36
                        radius: 9
                        color: page.scheduleMode === modelData[0]
                            ? theme.accentSoft : theme.surfaceRaised
                        border.width: 1
                        border.color: page.scheduleMode === modelData[0]
                            ? theme.accent : theme.line
                        AppText {
                            anchors.centerIn: parent
                            text: page.tr(modelData[1])
                            color: page.scheduleMode === modelData[0] ? theme.accent : theme.text
                            font.pixelSize: 9
                            font.weight: Font.DemiBold
                        }
                        TapHandler {
                            onTapped: {
                                page.scheduleMode = modelData[0]
                                if (modelData[0] === "daily")
                                    daysField.text = ""
                                else if (modelData[0] === "weekly"
                                         && daysField.text.match(/^\d{4}-/))
                                    daysField.text = "Mon,Tue,Wed,Thu,Fri"
                                else if (modelData[0] === "once"
                                         && !daysField.text.match(/^\d{4}-/))
                                    daysField.text = Qt.formatDate(new Date(), "yyyy-MM-dd")
                            }
                        }
                    }
                }
            }
            RowLayout {
                visible: page.scheduleMode === "weekly"
                Layout.fillWidth: true
                spacing: 6
                Repeater {
                    model: [
                        ["Mon", "dayMon"], ["Tue", "dayTue"], ["Wed", "dayWed"],
                        ["Thu", "dayThu"], ["Fri", "dayFri"], ["Sat", "daySat"],
                        ["Sun", "daySun"]
                    ]
                    delegate: Rectangle {
                        required property var modelData
                        Layout.fillWidth: true
                        Layout.preferredHeight: 34
                        radius: 9
                        color: page.hasWeekday(modelData[0]) ? theme.accent : theme.surfaceRaised
                        border.width: 1
                        border.color: page.hasWeekday(modelData[0]) ? theme.accent : theme.line
                        AppText {
                            anchors.centerIn: parent
                            text: page.tr(modelData[1])
                            color: page.hasWeekday(modelData[0]) ? "white" : theme.muted
                            font.pixelSize: 8
                            font.weight: Font.DemiBold
                        }
                        TapHandler { onTapped: page.toggleWeekday(modelData[0]) }
                    }
                }
            }
            AppButton {
                visible: page.scheduleMode === "once"
                Layout.fillWidth: true
                text: daysField.text || page.tr("chooseDate")
                iconName: "settings"
                onClicked: calendarPopup.open()
            }
            AppText { id: daysField; visible: false; text: "" }
            AppText {
                text: page.tr("afterQueueFinishes")
                color: theme.muted
                font.pixelSize: 9
            }
            RowLayout {
                Layout.fillWidth: true
                spacing: 8
                Repeater {
                    model: [
                        ["none", "doNothing"],
                        ["exit", "exitPidm"],
                        ["shutdown", "shutDownPc"]
                    ]
                    delegate: Rectangle {
                        required property var modelData
                        Layout.fillWidth: true
                        Layout.preferredHeight: 38
                        radius: 9
                        color: page.selectedPostAction === modelData[0]
                            ? theme.accentSoft : theme.surfaceRaised
                        border.width: 1
                        border.color: page.selectedPostAction === modelData[0]
                            ? theme.accent : theme.line
                        AppText {
                            anchors.centerIn: parent
                            text: page.tr(modelData[1])
                            color: page.selectedPostAction === modelData[0]
                                ? theme.accent : theme.text
                            font.pixelSize: 9
                            font.weight: Font.DemiBold
                        }
                        TapHandler {
                            onTapped: page.selectedPostAction = modelData[0]
                        }
                    }
                }
            }
            AppText {
                visible: page.selectedPostAction === "shutdown"
                text: page.tr("shutdownWarning")
                color: theme.warning
                font.pixelSize: 8
            }
            Item { Layout.fillHeight: true }
            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                AppButton { text: page.tr("cancel"); onClicked: editorPopup.close() }
                AppButton {
                    text: page.tr("saveQueue")
                    iconName: "check"
                    tone: "primary"
                    onClicked: {
                        if (queuesModel.saveQueue(
                            page.editingRow,
                            queueNameField.text,
                            queueEnabled.checked,
                            startTimeField.text,
                            stopTimeField.text,
                            daysField.text,
                            parseInt(concurrentField.text || "1"),
                            parseInt(pauseField.text || "0"),
                            page.selectedPostAction)) {
                            editorPopup.close()
                        }
                    }
                }
            }
        }
    }

    Popup {
        id: calendarPopup
        parent: Overlay.overlay
        anchors.centerIn: parent
        width: Math.min(390, page.width - 48)
        height: 430
        padding: 18
        modal: true
        property int shownMonth: new Date().getMonth()
        property int shownYear: new Date().getFullYear()
        Overlay.modal: Rectangle { color: Qt.rgba(0, 0, 0, 0.58) }
        background: Rectangle {
            radius: 17
            color: theme.surface
            border.width: 1
            border.color: theme.line
        }
        contentItem: ColumnLayout {
            spacing: 12
            RowLayout {
                Layout.fillWidth: true
                IconButton {
                    iconName: "chevron"
                    rotation: 180
                    onClicked: {
                        if (--calendarPopup.shownMonth < 0) {
                            calendarPopup.shownMonth = 11
                            calendarPopup.shownYear--
                        }
                    }
                }
                AppText {
                    Layout.fillWidth: true
                    horizontalAlignment: Text.AlignHCenter
                    text: Qt.formatDate(
                        new Date(calendarPopup.shownYear, calendarPopup.shownMonth, 1),
                        "MMMM yyyy"
                    )
                    color: theme.text
                    font.pixelSize: 14
                    font.weight: Font.Bold
                }
                IconButton {
                    iconName: "chevron"
                    onClicked: {
                        if (++calendarPopup.shownMonth > 11) {
                            calendarPopup.shownMonth = 0
                            calendarPopup.shownYear++
                        }
                    }
                }
            }
            DayOfWeekRow {
                Layout.fillWidth: true
                locale: monthGrid.locale
                delegate: AppText {
                    required property var model
                    text: model.shortName
                    color: theme.muted
                    font.pixelSize: 9
                    horizontalAlignment: Text.AlignHCenter
                }
            }
            MonthGrid {
                id: monthGrid
                Layout.fillWidth: true
                Layout.fillHeight: true
                month: calendarPopup.shownMonth
                year: calendarPopup.shownYear
                delegate: Rectangle {
                    required property var model
                    readonly property string dateValue: Qt.formatDate(
                        new Date(model.year, model.month, model.day), "yyyy-MM-dd"
                    )
                    color: daysField.text === dateValue
                        ? theme.accent : dayHover.hovered ? theme.surfaceHover : "transparent"
                    radius: 9
                    opacity: model.month === monthGrid.month ? 1 : 0.35
                    AppText {
                        anchors.centerIn: parent
                        text: model.day
                        color: daysField.text === parent.dateValue ? "white" : theme.text
                        font.pixelSize: 10
                    }
                    HoverHandler { id: dayHover }
                    TapHandler {
                        onTapped: {
                            daysField.text = parent.dateValue
                            calendarPopup.close()
                        }
                    }
                }
            }
            AppButton {
                Layout.alignment: Qt.AlignHCenter
                text: page.tr("today")
                onClicked: {
                    const today = new Date()
                    daysField.text = Qt.formatDate(today, "yyyy-MM-dd")
                    calendarPopup.close()
                }
            }
        }
    }
}
