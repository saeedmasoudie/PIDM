from __future__ import annotations

import argparse
import json
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SHARED_FILES = (
    "background.js",
    "content.js",
    "popup.css",
    "popup.html",
    "popup.js",
    "README.md",
)


def build(browser: str, output_directory: Path) -> Path:
    manifest_name = (
        "manifest.firefox.json" if browser == "firefox" else "manifest.json"
    )
    manifest = json.loads((ROOT / manifest_name).read_text(encoding="utf-8"))
    logo_path = ROOT / "logo.png"
    if logo_path.is_file():
        icons = {size: "logo.png" for size in ("16", "32", "48", "128")}
        manifest["icons"] = icons
        manifest["action"]["default_icon"] = icons
    version = manifest["version"]
    output_directory.mkdir(parents=True, exist_ok=True)
    output = output_directory / f"pidm-capture-{browser}-{version}.zip"
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        )
        for relative_path in SHARED_FILES:
            archive.write(ROOT / relative_path, relative_path)
        if logo_path.is_file():
            archive.write(logo_path, "logo.png")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Package the PIDM browser extension."
    )
    parser.add_argument(
        "browser",
        choices=("chromium", "firefox", "all"),
        default="all",
        nargs="?",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "dist",
    )
    arguments = parser.parse_args()
    browsers = (
        ("chromium", "firefox")
        if arguments.browser == "all"
        else (arguments.browser,)
    )
    for browser in browsers:
        print(build(browser, arguments.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
