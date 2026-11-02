import QtQuick
import QtQuick.Controls

Button {
    id: control

    property string iconName: ""
    property bool closeButton: false

    implicitWidth: 38
    implicitHeight: 32
    hoverEnabled: true
    padding: 0

    background: Rectangle {
        radius: 8
        color: control.down
            ? (control.closeButton ? theme.danger : theme.surfaceRaised)
            : control.hovered
                ? (control.closeButton ? theme.danger : theme.surfaceHover)
                : "transparent"
        opacity: control.down ? 0.82 : 1

        Behavior on color { ColorAnimation { duration: 100 } }
        Behavior on opacity { NumberAnimation { duration: 80 } }
    }

    contentItem: Item {
        UiIcon {
            anchors.centerIn: parent
            name: control.iconName
            color: !control.enabled
                ? theme.subtle
                : control.closeButton && control.hovered
                    ? "white"
                    : theme.muted
            width: control.iconName === "minus" ? 15 : 14
            height: control.iconName === "minus" ? 15 : 14
            lineWidth: 1.7

            Behavior on color { ColorAnimation { duration: 100 } }
        }
    }
}
