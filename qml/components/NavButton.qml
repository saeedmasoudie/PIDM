import QtQuick
import QtQuick.Controls
import PIDM 1.0

Button {
    id: control

    property string iconName: ""
    property bool active: false
    property bool collapsed: false
    property string badge: ""

    implicitHeight: 44
    hoverEnabled: true

    background: Rectangle {
        radius: 10
        color: control.active ? theme.accentSoft
            : control.hovered && control.enabled ? theme.surfaceHover
            : "transparent"
        border.width: control.active ? 1 : 0
        border.color: control.active ? theme.accent : "transparent"
        Behavior on color { ColorAnimation { duration: 120 } }
    }

    contentItem: Row {
        spacing: 12
        anchors.left: I18n.rtl ? undefined : parent.left
        anchors.right: I18n.rtl ? parent.right : undefined
        anchors.leftMargin: I18n.rtl ? 0 : control.collapsed ? 13 : 14
        anchors.rightMargin: I18n.rtl ? control.collapsed ? 13 : 14 : 0
        anchors.verticalCenter: parent.verticalCenter
        layoutDirection: I18n.rtl ? Qt.RightToLeft : Qt.LeftToRight
        LayoutMirroring.enabled: false

        UiIcon {
            name: control.iconName
            color: !control.enabled ? theme.subtle : control.active ? theme.accent : theme.muted
            width: 18
            height: 18
            anchors.verticalCenter: parent.verticalCenter
        }
        AppText {
            visible: !control.collapsed
            text: control.text
            color: !control.enabled ? theme.subtle : control.active ? theme.text : theme.muted
            font.pixelSize: 12
            font.weight: control.active ? Font.DemiBold : Font.Medium
            anchors.verticalCenter: parent.verticalCenter
        }
        Rectangle {
            visible: !control.collapsed && control.badge !== ""
            width: badgeText.implicitWidth + 12
            height: 20
            radius: 10
            color: theme.surfaceRaised
            anchors.verticalCenter: parent.verticalCenter
            AppText {
                id: badgeText
                anchors.centerIn: parent
                text: control.badge
                color: theme.subtle
                font.pixelSize: 9
                font.weight: Font.DemiBold
            }
        }
    }
}
