# PIDM

PIDM is a cross-platform desktop download manager built with Python, PySide6,
and Qt Quick. It combines a native queue-based interface with resumable
downloads, segmented transfers, media extraction, torrent support, scheduling,
and optional browser integration.

## Download

You can always grab the latest version of the app from the
**[GitHub Releases](../../releases/latest)** page.
Choose the package that matches your operating system:

[![Download for Windows](https://img.shields.io/badge/Download-Windows-blue?logo=windows)](../../releases/latest)
[![Download for macOS](https://img.shields.io/badge/Download-macOS-lightgrey?logo=apple)](../../releases/latest)
[![Download for Linux](https://img.shields.io/badge/Download-Linux-green?logo=linux)](../../releases/latest)

## Highlights

- Pause, resume, retry, and recover HTTP and HTTPS downloads
- Use parallel byte-range connections when the remote server supports them
- Organize transfers into queues with schedules, concurrency limits, and
  post-completion actions
- Download torrents and magnet links with per-file selection
- Resolve downloadable media and formats with yt-dlp
- Try HTTP/3 for compatible HTTPS origins, with automatic fallback
- Apply speed limits, connection limits, proxy settings, network-interface
  selection, custom request headers, and checksum verification
- Monitor the clipboard and control PIDM from the system tray
- Choose light, dark, or system themes
- Use the interface in English, Persian, Russian, or Chinese
- Send links and detected media from Chromium-based browsers or Firefox

## Screenshots

![PIDM Dark Mode](Screenshot/Screenshot.jpg)

## Supported platforms

PIDM is developed for Windows, Linux, and macOS. GitHub Actions builds
PyInstaller packages for all three platforms.

Python 3.11 through 3.13 is recommended. Torrent support depends on
`libtorrent`, whose available wheels may vary by Python version and platform.

## Install from source

### Prerequisites

- Python 3.11–3.13
- Git
- A Python virtual environment

### Windows

```powershell
git clone https://github.com/saeedmasoudie/PIDM.git
cd PIDM
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python main.py
```

### Linux and macOS

```bash
git clone https://github.com/saeedmasoudie/PIDM.git
cd PIDM
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python main.py
```

On Linux, Qt may also require distribution packages for X11 or Wayland,
OpenGL, audio, and the system tray. Install the packages appropriate for your
desktop environment if Qt reports a missing platform plugin.

## Browser extension

PIDM includes a WebExtension for Chromium-based browsers and Firefox. The
extension can send individual links, selected batches, media pages, and
detected media to the desktop app.

PIDM must be running because the extension communicates only with a local
bridge at `127.0.0.1:17654`. No browsing data is sent to an external service.

To create installable archives:

```bash
python extension/package.py
```

The command writes separate Chromium and Firefox ZIP files to
`extension/dist/`. See [extension/README.md](extension/README.md) for unpacked
development installation, browser compatibility, behavior, and privacy
details.

## Build a desktop package

Install the build dependencies and run the checked-in PyInstaller
specification:

```bash
python -m pip install -r requirements.txt pyinstaller pillow
python scripts/write_build_info.py
python -m PyInstaller --clean --noconfirm pidm.spec
```

The application bundle is written to `dist/`. The checked-in production logo
is used for the desktop application and packaged browser extensions.

Pushing a tag whose name starts with `v`, such as `v2.0.0`, runs the
[build workflow](.github/workflows/build.yml). It builds desktop artifacts and
browser extension archives, then attaches them to the matching GitHub release.
The workflow can also be started manually.

## Development checks

This repository does not currently include an automated test suite. Before
opening a pull request, run the available static and packaging checks:

```bash
python -m compileall -q .
node --check extension/background.js
node --check extension/content.js
node --check extension/popup.js
python extension/package.py --output /tmp/pidm-extensions
```

You can also exercise the desktop application directly with `python main.py`.

## Project layout

```text
.
├── assets/          Fonts, sounds, and optional application branding
├── controllers/     Qt-facing application services
├── core/            HTTP, HTTP/3, torrent, and persistence primitives
├── extension/       Chromium and Firefox WebExtension sources
├── models/          Download and queue models
├── qml/             Qt Quick interface
├── scripts/         Build metadata tooling
├── translations/    Application translations
├── main.py           Application entry point
└── pidm.spec         PyInstaller configuration
```

Download history is stored in `downloads-v2.db` inside the platform-specific
PIDM user-data directory. Application preferences use the operating system's
standard Qt settings location. Neither file is stored in the repository.

## Contributing

Issues and focused pull requests are welcome. Please describe the operating
system, Python version, source type, and reproduction steps when reporting a
download problem. Keep changes scoped and run the development checks above
before submitting code.

## Responsible use

Only download files and media that you are authorized to save. Users are
responsible for complying with copyright law, website terms, and local
regulations.

## License

PIDM is released under the [MIT License](LICENSE). The bundled Vazirmatn font
is distributed under the SIL Open Font License included in
`assets/fonts/OFL.txt`.
