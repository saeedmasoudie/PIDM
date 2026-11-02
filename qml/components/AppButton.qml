import QtQuick
import QtQuick.Controls

Button {
    id: control

    property string tone: "neutral"
    property string iconName: ""
    property bool compact: false

    implicitHeight: 40
    implicitWidth: Math.max(40, contentRow.implicitWidth + (compact ? 20 : 30))
    leftPadding: 15
    rightPadding: 15
    hoverEnabled: true

    readonly property color foreground: !enabled ? theme.subtle
        : tone === "primary" ? "white"
        : tone === "danger" ? theme.danger
        : theme.text
    readonly property color fill: !enabled ? theme.surfaceRaised
        : down ? (tone === "primary" ? theme.accentHover : theme.surfaceHover)
        : hovered ? (tone === "primary" ? theme.accentHover : theme.surfaceHover)
        : tone === "primary" ? theme.accent
        : tone === "danger" ? theme.surfaceRaised
        : theme.surfaceRaised

    background: Rectangle {
        radius: 10
        color: control.fill
        border.width: control.tone === "primary" ? 0 : 1
        border.color: control.tone === "danger" ? theme.danger : theme.line

        Behavior on color { ColorAnimation { duration: 120 } }
    }

    contentItem: Item {
        implicitWidth: contentRow.implicitWidth
        implicitHeight: contentRow.implicitHeight
        Row {
            id: contentRow
            anchors.centerIn: parent
            spacing: control.compact || control.iconName === "" || control.text === "" ? 0 : 9

            UiIcon {
                visible: control.iconName !== ""
                name: control.iconName
                color: control.foreground
                width: 17
                height: 17
                anchors.verticalCenter: parent.verticalCenter
            }

            AppText {
                visible: !control.compact && control.text !== ""
                text: control.text
                color: control.foreground
                font.pixelSize: 12
                font.weight: Font.DemiBold
                anchors.verticalCenter: parent.verticalCenter
            }
        }
    }
}
