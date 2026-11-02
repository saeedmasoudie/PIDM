from pathlib import Path
import sys

from PyInstaller.utils.hooks import collect_all, collect_submodules


root = Path(SPECPATH)
ffmpeg_datas, ffmpeg_binaries, ffmpeg_hidden = collect_all("imageio_ffmpeg")
hidden_imports = (
    collect_submodules("yt_dlp")
    + collect_submodules("aioquic")
    + ffmpeg_hidden
    + ["PySide6.QtMultimedia", "libtorrent"]
)

analysis = Analysis(
    ["main.py"],
    pathex=[str(root)],
    binaries=ffmpeg_binaries,
    datas=[
        (str(root / "qml"), "qml"),
        (str(root / "translations"), "translations"),
        (str(root / "assets"), "assets"),
        *ffmpeg_datas,
    ],
    hiddenimports=hidden_imports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(analysis.pure)
executable = EXE(
    pyz,
    analysis.scripts,
    [],
    exclude_binaries=True,
    name="PIDM",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    icon=str(root / "assets" / "logo.png"),
)
collection = COLLECT(
    executable,
    analysis.binaries,
    analysis.datas,
    strip=False,
    upx=False,
    name="PIDM",
)

if sys.platform == "darwin":
    app = BUNDLE(
        collection,
        name="PIDM.app",
        icon=str(root / "assets" / "logo.png"),
        bundle_identifier="com.pidm.downloadmanager",
        info_plist={
            "CFBundleName": "PIDM",
            "CFBundleDisplayName": "PIDM",
            "NSHighResolutionCapable": True,
        },
    )
