# PIDM browser extension

This WebExtension sends media pages, direct media, ordinary links, and selected
batches to the PIDM desktop app over a loopback-only bridge at
`127.0.0.1:17654`. PIDM must be running.

## Supported browsers

- Chrome, Edge, Brave, Opera, Vivaldi, and other Chromium browsers use
  `manifest.json`.
- Firefox uses `manifest.firefox.json` renamed to `manifest.json` in the
  packaged Firefox build.
- Safari requires Apple's Safari Web Extension conversion/signing tool. The
  shared JavaScript is written against the standard WebExtension API, but a
  signed Safari package is not included in this source tree.

## Load for development

### Chromium

1. Open the browser's extensions page.
2. Enable developer mode.
3. Choose **Load unpacked** and select this `extension/` folder.

### Firefox

1. Copy this folder to a temporary location.
2. Replace `manifest.json` with `manifest.firefox.json`.
3. Open `about:debugging` → **This Firefox** → **Load Temporary Add-on**.
4. Select the temporary `manifest.json`.

Temporary Firefox add-ons are removed when Firefox closes. Permanent
installation in standard Firefox requires Mozilla signing. Firefox Developer
Edition and Nightly can be configured for unsigned extension testing.

### Release downloads

The GitHub release page includes separate Chromium and Firefox ZIP files.
Extract the archive before loading it:

- Chromium browsers: enable developer mode and choose **Load unpacked**.
- Firefox: open `about:debugging#/runtime/this-firefox`, choose
  **Load Temporary Add-on**, and select `manifest.json`.

PIDM must be running before the extension can submit or capture downloads.

## Package ZIP files

From the repository root:

```bash
python extension/package.py
```

This creates separate Chromium and Firefox archives under `extension/dist/`,
with the correct manifest stored as `manifest.json` in each archive.

## Logo

Replace the bundled fallback PNG with your production logo at:

```text
extension/logo.png
```

The popup, in-page control, browser toolbar, and extension-management page use
this file. Reload the unpacked extension after replacing it. The packaging
script includes the same PNG in Chromium and Firefox archives.

## Behavior and privacy

- PIDM must be running before the extension can resolve qualities or submit
  links. The popup shows a prominent warning and disables actions while the
  desktop bridge is unavailable; in-page controls check PIDM periodically.
- **Open media page** sends one page URL into PIDM's existing yt-dlp quality
  selection flow.
- **Review in PIDM** sends selected direct links into PIDM's batch-review
  window. Nothing starts until the user confirms the batch.
- **Capture browser downloads** sends downloads started by the browser to PIDM
  and cancels the browser transfer after PIDM accepts the URL. Disable this
  behavior at any time from the extension popup.
- When PIDM is waiting to refresh an expired download link, the next browser
  download captured by the extension replaces that download's URL instead of
  creating a new item.
- The extension observes completed media requests only to identify direct
  audio/video resources. Captures remain in extension memory for the lifetime
  of the browser tab.
- Optional in-page PIDM controls appear only while PIDM is running and a
  meaningful media player, eligible YouTube video, detected player stream, or
  explicit download link is present. Background/autoplay media and ordinary
  page links do not trigger the label. Disable the controls at any time from
  the extension popup.
- Media pages can be resolved from the popup or in-page control. PIDM returns
  labeled quality, codec/container, stream type, and size choices; selecting
  one sends that exact format into the normal PIDM review flow.
- Recommended video-with-audio qualities are shown first and deduplicated by
  quality. Video-only and audio-only choices are hidden behind an advanced
  toggle.
- Link detection refreshes after normal navigation and single-page-app route or
  DOM changes.
- YouTube watch, Shorts, and live pages are passed to yt-dlp as pages. Raw
  `googlevideo` audio/video tracks are intentionally ignored because YouTube
  commonly delivers them as separate streams. YouTube home, search, channel,
  and subscription pages do not show capture actions.
- No browsing data is sent anywhere except the PIDM process on the local
  computer.

Only download media and files you are authorized to save.
