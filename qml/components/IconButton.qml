import QtQuick
import QtQuick.Controls

Button {
    id: control

    property string iconName: ""
    property bool danger: false
    property int buttonSize: 36

    implicitWidth: buttonSize
    implicitHeight: buttonSize
    hoverEnabled: true

    background: Rectangle {
        radius: 9
        color: control.down || control.hovered
            ? (control.danger ? theme.surfaceRaised : theme.surfaceHover)
            : "transparent"
        Behavior on color { ColorAnimation { duration: 110 } }
    }

    contentItem: Item {
        UiIcon {
            anchors.centerIn: parent
            name: control.iconName
            color: !control.enabled ? theme.subtle
                : control.danger && control.hovered ? theme.danger
                : theme.muted
            width: 18
            height: 18
        }
    }
}
