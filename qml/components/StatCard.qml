import QtQuick
import QtQuick.Layouts
import PIDM 1.0

Rectangle {
    id: card

    property string label: ""
    property string value: ""
    property string helper: ""
    property string iconName: ""
    property color tone: theme.accent

    implicitHeight: 116
    opacity: 0
    radius: 14
    color: theme.surface
    border.width: 1
    border.color: theme.line
    Component.onCompleted: opacity = 1
    Behavior on opacity { NumberAnimation { duration: 240; easing.type: Easing.OutCubic } }

    RowLayout {
        anchors.fill: parent
        anchors.margins: 17
        spacing: 13

        Rectangle {
            Layout.preferredWidth: 38
            Layout.preferredHeight: 38
            radius: 11
            color: Qt.rgba(card.tone.r, card.tone.g, card.tone.b, theme.name === "light" ? 0.10 : 0.14)
            UiIcon {
                anchors.centerIn: parent
                name: card.iconName
                color: card.tone
                width: 18
                height: 18
            }
        }

        ColumnLayout {
            Layout.fillWidth: true
            spacing: 3
            AppText {
                Layout.fillWidth: true
                text: card.label.toUpperCase()
                color: theme.muted
                font.pixelSize: 9
                font.weight: Font.DemiBold
                font.letterSpacing: 0.7
                horizontalAlignment: I18n.rtl
                    ? Text.AlignRight : Text.AlignLeft
            }
            AppText {
                text: card.value
                color: theme.text
                font.pixelSize: 21
                font.weight: Font.Bold
                elide: Text.ElideRight
                Layout.fillWidth: true
                horizontalAlignment: I18n.rtl
                    ? Text.AlignRight : Text.AlignLeft
            }
            AppText {
                text: card.helper
                color: theme.subtle
                font.pixelSize: 10
                elide: Text.ElideRight
                Layout.fillWidth: true
                horizontalAlignment: I18n.rtl
                    ? Text.AlignRight : Text.AlignLeft
            }
        }
    }
}
