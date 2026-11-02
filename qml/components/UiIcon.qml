import QtQuick

Canvas {
    id: icon

    property string name: ""
    property color color: "white"
    property real lineWidth: 1.8

    implicitWidth: 20
    implicitHeight: 20

    onNameChanged: requestPaint()
    onColorChanged: requestPaint()
    onLineWidthChanged: requestPaint()
    onWidthChanged: requestPaint()
    onHeightChanged: requestPaint()

    onPaint: {
        const ctx = getContext("2d")
        ctx.clearRect(0, 0, width, height)
        ctx.save()
        ctx.scale(width / 24, height / 24)
        ctx.strokeStyle = color
        ctx.fillStyle = color
        ctx.lineWidth = lineWidth
        ctx.lineCap = "round"
        ctx.lineJoin = "round"

        function path(points) {
            ctx.beginPath()
            ctx.moveTo(points[0][0], points[0][1])
            for (let i = 1; i < points.length; i++)
                ctx.lineTo(points[i][0], points[i][1])
            ctx.stroke()
        }

        if (name === "dashboard") {
            ctx.strokeRect(3.5, 3.5, 6.5, 6.5)
            ctx.strokeRect(14, 3.5, 6.5, 6.5)
            ctx.strokeRect(3.5, 14, 6.5, 6.5)
            ctx.strokeRect(14, 14, 6.5, 6.5)
        } else if (name === "download") {
            path([[12, 3], [12, 16], [17, 11]])
            path([[12, 16], [7, 11]])
            path([[4, 20], [20, 20]])
        } else if (name === "list") {
            path([[9, 6], [21, 6]])
            path([[9, 12], [21, 12]])
            path([[9, 18], [21, 18]])
            ctx.beginPath()
            ctx.arc(4.5, 6, 0.8, 0, Math.PI * 2)
            ctx.arc(4.5, 12, 0.8, 0, Math.PI * 2)
            ctx.arc(4.5, 18, 0.8, 0, Math.PI * 2)
            ctx.fill()
        } else if (name === "settings") {
            path([[4, 6], [8, 6]])
            path([[12, 6], [20, 6]])
            path([[4, 12], [13, 12]])
            path([[17, 12], [20, 12]])
            path([[4, 18], [7, 18]])
            path([[11, 18], [20, 18]])
            ctx.beginPath(); ctx.arc(10, 6, 2, 0, Math.PI * 2); ctx.stroke()
            ctx.beginPath(); ctx.arc(15, 12, 2, 0, Math.PI * 2); ctx.stroke()
            ctx.beginPath(); ctx.arc(9, 18, 2, 0, Math.PI * 2); ctx.stroke()
        } else if (name === "plus") {
            path([[12, 5], [12, 19]])
            path([[5, 12], [19, 12]])
        } else if (name === "minus") {
            path([[6, 12], [18, 12]])
        } else if (name === "maximize") {
            ctx.strokeRect(5, 5, 14, 14)
        } else if (name === "restore") {
            ctx.strokeRect(4, 7, 12, 12)
            path([[8, 7], [8, 4], [20, 4], [20, 16], [16, 16]])
        } else if (name === "close") {
            path([[6, 6], [18, 18]])
            path([[18, 6], [6, 18]])
        } else if (name === "search") {
            ctx.beginPath(); ctx.arc(10.5, 10.5, 6.5, 0, Math.PI * 2); ctx.stroke()
            path([[15.5, 15.5], [21, 21]])
        } else if (name === "pause") {
            path([[9, 6], [9, 18]])
            path([[15, 6], [15, 18]])
        } else if (name === "play") {
            ctx.beginPath()
            ctx.moveTo(8, 5)
            ctx.lineTo(19, 12)
            ctx.lineTo(8, 19)
            ctx.closePath()
            ctx.stroke()
        } else if (name === "folder") {
            ctx.beginPath()
            ctx.moveTo(3, 7)
            ctx.lineTo(9, 7)
            ctx.lineTo(11, 9)
            ctx.lineTo(21, 9)
            ctx.lineTo(20, 19)
            ctx.lineTo(4, 19)
            ctx.closePath()
            ctx.stroke()
            path([[3, 7], [4, 5], [9, 5], [11, 7], [20, 7], [21, 9]])
        } else if (name === "trash") {
            path([[5, 7], [19, 7]])
            path([[9, 7], [9, 4], [15, 4], [15, 7]])
            path([[7, 7], [8, 20], [16, 20], [17, 7]])
            path([[10, 11], [10.5, 17]])
            path([[14, 11], [13.5, 17]])
        } else if (name === "external") {
            path([[13, 5], [20, 5], [20, 12]])
            path([[20, 5], [11, 14]])
            path([[17, 14], [17, 19], [5, 19], [5, 7], [10, 7]])
        } else if (name === "check") {
            path([[5, 12], [10, 17], [20, 7]])
        } else if (name === "alert") {
            ctx.beginPath()
            ctx.moveTo(12, 3)
            ctx.lineTo(22, 20)
            ctx.lineTo(2, 20)
            ctx.closePath()
            ctx.stroke()
            path([[12, 9], [12, 14]])
            ctx.beginPath(); ctx.arc(12, 17.5, 0.7, 0, Math.PI * 2); ctx.fill()
        } else if (name === "globe") {
            ctx.beginPath(); ctx.arc(12, 12, 9, 0, Math.PI * 2); ctx.stroke()
            ctx.beginPath(); ctx.ellipse(12, 12, 4, 9, 0, 0, Math.PI * 2); ctx.stroke()
            path([[3, 12], [21, 12]])
        } else if (name === "sun") {
            ctx.beginPath(); ctx.arc(12, 12, 4, 0, Math.PI * 2); ctx.stroke()
            for (let i = 0; i < 8; i++) {
                const a = i * Math.PI / 4
                path([[12 + Math.cos(a) * 7, 12 + Math.sin(a) * 7],
                      [12 + Math.cos(a) * 9, 12 + Math.sin(a) * 9]])
            }
        } else if (name === "network") {
            ctx.beginPath(); ctx.arc(12, 19, 1, 0, Math.PI * 2); ctx.fill()
            ctx.beginPath(); ctx.arc(12, 19, 5, Math.PI * 1.2, Math.PI * 1.8); ctx.stroke()
            ctx.beginPath(); ctx.arc(12, 19, 10, Math.PI * 1.2, Math.PI * 1.8); ctx.stroke()
        } else if (name === "chevron") {
            path([[9, 6], [15, 12], [9, 18]])
        }
        ctx.restore()
    }
}
