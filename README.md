# PIDM - Python Internet Download Manager

<img src="screenshots/pidm.png" alt="PIDM logo" width="128" height="128">

**PIDM** is a modern, open-source internet download manager written in Python and powered by PySide6 (Qt). It features full support for stream and file downloads, Chrome extension integration, smart UI features, and multi-language support — all in a beautiful, theme-aware desktop application.

---

## 🔥 Key Features

- 🎞️ **Stream downloads** from YouTube, Twitch, Netflix, and more (via `yt_dlp`)
- 📥 **File downloads** with multithreading and parallel chunking (`httpx`)
- 🧩 **Browser extension** for Chrome-based browsers (via [PIDM-ext](https://github.com/saeedmasoudie/PIDM-ext))
- 🗂️ **Queue system** with concurrent downloads and scheduling
- 🌈 **Theme-aware UI** (automatic light/dark mode)
- 🌐 **Multi-language support** (English, فارسی, ...)
- 💻 **Lightweight and fast** standalone Windows build via Nuitka
- 🔐 **Login/auth support**, global and per-download speed limits
- 💽 **SQLite-based session persistence** (downloads remembered between launches)
- 🌍 **Proxy communication support** for external triggers

---

## 📷 Screenshots

![Main Window](screenshots/Screenshot-1.png)
![New Download Dialog](screenshots/Screenshot-2.png)

---

## 🛠️ Installation

### 🔽 Download for Windows

Get the latest `.exe` from the [Releases](https://github.com/saeedmasoudie/PIDM/releases) page.

### 🧩 Install Browser Extension

Source and updates available at [PIDM Extension Repository](https://github.com/saeedmasoudie/PIDM-ext)

### ⚙️ Run from Source (Linux/Mac/Dev)

1. Clone the repo:
    ```bash
    git clone https://github.com/saeedmasoudie/PIDM.git
    cd PIDM
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Run the app:
    ```bash
    python main.py
    ```

---

## 📦 Requirements (for source version)

- Python 3.9+
- [ffmpeg](https://ffmpeg.org/) (for stream muxing)
- PySide6
- httpx
- yt_dlp
- Others as listed in `requirements.txt`

---

## 🌍 Translations

PIDM is available in multiple languages. You can help improve translations or add your own by editing `.ts` files in `assets/translations`.

---

## 🚧 Roadmap

**Completed:**
- ✅ Stream downloads with format selection
- ✅ Browser extension integration
- ✅ Smart UI with metadata previews
- ✅ Concurrent download queuing
- ✅ Calendar-based scheduling for download start times

**Planned / In Progress:**
- 🔁 Rework scheduling with more advanced rules and options
- 🧲 Torrent support
- 🌐 Network interface selection

---

## 🤝 Credits

PIDM uses the following open-source tools:

- [`yt_dlp`](https://github.com/yt-dlp/yt-dlp) – for stream downloads
- [`ffmpeg`](https://ffmpeg.org/) – for media processing
- [`httpx`](https://www.python-httpx.org/) – for robust HTTP file downloads
- [`PySide6`](https://doc.qt.io/qtforpython/) – Qt-based GUI toolkit
- [`Nuitka`](https://nuitka.net/) – compiler for generating standalone builds

---

## 🛡️ License

MIT License © [Saeed Masoudi](https://github.com/saeedmasoudie)

---

## ❤️ Contributing

Pull requests and ideas are welcome!

1. Fork the repo
2. Create a branch: `git checkout -b feature/your-feature`
3. Commit changes: `git commit -m "Add feature"`
4. Push: `git push origin feature/your-feature`
5. Open a pull request

---

## 📫 Contact

Have suggestions or questions?  
Use [GitHub Issues](https://github.com/saeedmasoudie/PIDM/issues) or join the [Discussions](https://github.com/saeedmasoudie/PIDM/discussions).
