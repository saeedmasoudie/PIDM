import QtQuick
import QtQuick.Controls
import PIDM 1.0

TextField {
    id: control

    property string iconName: ""
    property bool forceLeftToRight: false

    implicitHeight: 44
    color: theme.text
    placeholderTextColor: theme.subtle
    selectionColor: theme.accent
    selectedTextColor: "white"
    font.family: I18n.fontFamily
    font.pixelSize: 12
    leftPadding: iconName === "" || (I18n.rtl && !forceLeftToRight) ? 14 : 42
    rightPadding: iconName !== "" && I18n.rtl && !forceLeftToRight ? 42 : 14
    verticalAlignment: TextInput.AlignVCenter
    horizontalAlignment: forceLeftToRight || !I18n.rtl
        ? TextInput.AlignLeft : TextInput.AlignRight
    LayoutMirroring.enabled: false
    selectByMouse: true

    background: Rectangle {
        radius: 10
        color: theme.background
        border.width: 1
        border.color: control.activeFocus ? theme.accent : theme.line
        Behavior on border.color { ColorAnimation { duration: 120 } }
    }

    UiIcon {
        visible: control.iconName !== ""
        name: control.iconName
        color: control.activeFocus ? theme.accent : theme.subtle
        width: 17
        height: 17
        x: I18n.rtl && !control.forceLeftToRight
            ? control.width - width - 14 : 14
        anchors.verticalCenter: parent.verticalCenter
    }
}
