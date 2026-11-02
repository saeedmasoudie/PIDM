const api = globalThis.browser ?? globalThis.chrome;
const OVERLAY_ID = "pidm-page-control";
let overlayHost = null;
let refreshTimer = 0;
let lastUrl = location.href;
let lastSignature = "";
let showPageControls = true;
let dismissedUrl = "";
let networkMediaDetected = false;
let qualityPanelOpen = false;
let logoDataUrl = "";
let pidmReady = null;

async function loadOptionalLogo() {
  try {
    const response = await fetch(api.runtime.getURL("logo.png"));
    if (!response.ok)
      return;
    const blob = await response.blob();
    logoDataUrl = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = reject;
      reader.readAsDataURL(blob);
    });
    refreshPageState(true);
  } catch {
  }
}

function absoluteUrl(value) {
  try {
    const url = new URL(value, document.baseURI);
    return ["http:", "https:", "magnet:"].includes(url.protocol) ? url.href : "";
  } catch {
    return "";
  }
}

function isYouTubeHost(hostname) {
  return hostname === "youtube.com"
    || hostname.endsWith(".youtube.com")
    || hostname === "youtu.be"
    || hostname.endsWith(".youtu.be");
}

function classifyPage() {
  const url = new URL(location.href);
  if (!isYouTubeHost(url.hostname)) {
    return {
      siteMode: "standard",
      pageEligible: true,
      scanLinks: true,
      message: ""
    };
  }
  const eligible = url.hostname.endsWith("youtu.be")
    ? url.pathname.split("/").filter(Boolean).length === 1
    : (
      (url.pathname === "/watch" && Boolean(url.searchParams.get("v")))
      || /^\/shorts\/[^/]+/.test(url.pathname)
      || /^\/live\/[^/]+/.test(url.pathname)
    );
  return {
    siteMode: "youtube",
    pageEligible: eligible,
    scanLinks: false,
    message: eligible
      ? "YouTube video detected. PIDM will show available qualities."
      : "Open a YouTube video, Short, or live stream to enable PIDM capture."
  };
}

function kindFor(url, element) {
  const clean = url.split(/[?#]/, 1)[0].toLowerCase();
  if (/\.(?:m3u8|mpd)$/.test(clean))
    return "stream";
  if (element instanceof HTMLVideoElement || element.tagName === "SOURCE")
    return "video";
  if (element instanceof HTMLAudioElement)
    return "audio";
  if (/\.(?:mp4|webm|mkv|mov|m4v|avi|wmv)$/.test(clean))
    return "video";
  if (/\.(?:mp3|m4a|aac|ogg|opus|flac|wav)$/.test(clean))
    return "audio";
  if (/\.(?:zip|7z|rar|tar|gz|bz2|xz|iso|exe|msi|dmg|pkg|apk|pdf|epub|torrent)$/.test(clean))
    return "file";
  return "link";
}

function isMeaningfulPlayer(element) {
  const player = element?.tagName === "SOURCE"
    ? element.parentElement
    : element;
  if (!(player instanceof HTMLVideoElement)
      && !(player instanceof HTMLAudioElement))
    return false;
  if (player.controls)
    return true;
  if (player instanceof HTMLAudioElement)
    return false;
  if (player.muted && player.autoplay)
    return false;
  const bounds = player.getBoundingClientRect();
  return bounds.width >= 320 && bounds.height >= 180;
}

function hasMeaningfulPlayer() {
  return Array.from(document.querySelectorAll("video, audio"))
    .some(isMeaningfulPlayer);
}

function collectPageLinks(classification = classifyPage()) {
  if (!classification.scanLinks)
    return [];
  const found = new Map();
  const add = (value, title, element) => {
    const url = absoluteUrl(value);
    if (!url || url.startsWith("blob:") || found.has(url))
      return;
    found.set(url, {
      url,
      title: String(title || "").trim().slice(0, 300)
        || url.split("/").pop()
        || url,
      kind: kindFor(url, element),
      pageUrl: location.href,
      confidence: isMeaningfulPlayer(element)
        ? "player"
        : element?.hasAttribute?.("download")
          ? "explicit"
          : "discovered"
    });
  };

  document.querySelectorAll("video, audio").forEach(element => {
    add(element.currentSrc || element.src, element.title || document.title, element);
  });
  document.querySelectorAll("source[src]").forEach(element => {
    add(element.src, element.title || document.title, element);
  });
  document.querySelectorAll("a[href]").forEach(element => {
    add(element.href, element.download || element.textContent, element);
  });
  return Array.from(found.values()).slice(0, 250);
}

function directCandidates(links) {
  return links.filter(item => item.kind !== "link");
}

function strongCandidates(links) {
  return directCandidates(links).filter(
    item => item.confidence === "player" || item.confidence === "explicit"
  );
}

function pageState() {
  const classification = classifyPage();
  const links = collectPageLinks(classification);
  return {
    title: document.title,
    pageUrl: location.href,
    links,
    directCount: directCandidates(links).length,
    networkMediaDetected,
    ...classification
  };
}

function removeOverlay() {
  overlayHost?.remove();
  overlayHost = null;
}

function overlayTemplate(state) {
  const meaningfulPlayer = hasMeaningfulPlayer();
  const canSendPage = state.pageEligible
    && (
      state.siteMode === "youtube"
      || (state.networkMediaDetected && meaningfulPlayer)
      || meaningfulPlayer
    );
  const candidates = directCandidates(state.links);
  const strong = strongCandidates(state.links);
  const shouldOffer = (
    (state.siteMode === "youtube" && state.pageEligible)
    || (meaningfulPlayer && (state.networkMediaDetected || strong.length > 0))
    || strong.some(item => item.confidence === "explicit")
  );
  const label = pidmReady === false
    ? "PIDM is not running"
    : state.siteMode === "youtube"
    ? "Video ready"
    : state.networkMediaDetected
      ? "Stream detected"
    : strong.length === 1
      ? "1 download found"
      : `${strong.length} downloads found`;
  const primaryLabel = pidmReady === false
    ? "Open PIDM first"
    : canSendPage
    ? "Choose quality"
    : strong.length > 1
      ? "Review links"
      : "Download";
  return { canSendPage, candidates, strong, shouldOffer, label, primaryLabel };
}

function renderOverlay(state, pulse = false) {
  const details = overlayTemplate(state);
  const shouldShow = showPageControls
    && pidmReady === true
    && state.pageEligible
    && dismissedUrl !== state.pageUrl
    && details.shouldOffer;
  if (!shouldShow) {
    removeOverlay();
    return;
  }
  if (!overlayHost) {
    overlayHost = document.createElement("div");
    overlayHost.id = OVERLAY_ID;
    document.documentElement.append(overlayHost);
    overlayHost.attachShadow({ mode: "open" });
  }
  const root = overlayHost.shadowRoot;
  root.innerHTML = `
    <style>
      :host { all: initial; }
      .panel {
        position: fixed; right: 22px; bottom: 22px; z-index: 2147483647;
        display: flex; align-items: center; gap: 10px; padding: 9px;
        border: 1px solid #30394a; border-radius: 14px;
        background: rgba(13, 17, 26, .96); color: #f3f6fc;
        box-shadow: 0 16px 48px rgba(0, 0, 0, .38);
        font: 12px/1.25 Inter, "Segoe UI", system-ui, sans-serif;
        backdrop-filter: blur(14px);
        transform-origin: right bottom;
        animation: pidm-in .18s ease-out both;
      }
      .panel.pulse { animation: pidm-pulse .55s ease-out; }
      .panel.stable { animation: none; }
      .mark {
        position: relative; display: grid; width: 34px; height: 34px; place-items: center;
        border-radius: 10px; background: #7357ff; color: white;
        overflow: hidden; font-size: 14px; font-weight: 800;
      }
      .mark img { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: contain; background: #0d111a; }
      .copy { min-width: 112px; }
      strong, span { display: block; }
      strong { font-size: 11px; }
      span { margin-top: 2px; color: #8b96aa; font-size: 9px; }
      button {
        min-height: 32px; padding: 0 12px; border: 0; border-radius: 9px;
        background: #7357ff; color: white; cursor: pointer;
        font: 600 10px Inter, "Segoe UI", system-ui, sans-serif;
      }
      button:hover { background: #846cff; }
      button:disabled { cursor: default; opacity: .52; }
      button.close {
        width: 30px; padding: 0; background: transparent; color: #8b96aa;
        font-size: 16px;
      }
      button.close:hover { background: #1b2433; color: #f3f6fc; }
      .panel.qualities { display: block; width: min(380px, calc(100vw - 44px)); padding: 14px; }
      .quality-header { display: flex; align-items: center; gap: 10px; }
      .quality-header .copy { min-width: 0; flex: 1; }
      .quality-header strong, .quality-header span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .quality-label { margin: 13px 0 6px; color: #8b96aa; font-size: 9px; font-weight: 700; text-transform: uppercase; }
      .quality-note { margin: 7px 0 0; color: #8b96aa; font-size: 8px; line-height: 1.45; }
      select {
        width: 100%; height: 38px; padding: 0 10px; border: 1px solid #30394a;
        border-radius: 9px; outline: none; background: #171e2b; color: #f3f6fc;
        font: 10px Inter, "Segoe UI", system-ui, sans-serif;
      }
      select:focus { border-color: #7357ff; }
      .quality-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 11px; }
      button.secondary { border: 1px solid #30394a; background: #171e2b; color: #f3f6fc; }
      button.secondary:hover { background: #1b2433; }
      @keyframes pidm-in {
        from { opacity: 0; transform: translateY(8px) scale(.97); }
        to { opacity: 1; transform: none; }
      }
      @keyframes pidm-pulse {
        0%, 100% { box-shadow: 0 16px 48px rgba(0, 0, 0, .38); }
        45% { box-shadow: 0 0 0 7px rgba(115, 87, 255, .18), 0 16px 48px rgba(0, 0, 0, .38); }
      }
      @media (prefers-reduced-motion: reduce) {
        .panel, .panel.pulse { animation: none; }
      }
    </style>
    <div class="panel${pulse ? " pulse" : " stable"}" role="region" aria-label="PIDM download controls">
      <div class="mark" aria-hidden="true">P${logoDataUrl ? `<img src="${logoDataUrl}" alt="">` : ""}</div>
      <div class="copy"><strong>PIDM</strong><span>${details.label}</span></div>
      <button class="primary" type="button"${pidmReady === false ? " disabled" : ""}>${details.primaryLabel}</button>
      <button class="close" type="button" title="Hide PIDM controls" aria-label="Hide PIDM controls">×</button>
    </div>
  `;
  root.querySelector(".mark img")?.addEventListener("error", event => {
    event.currentTarget.remove();
  });
  root.querySelector(".close").addEventListener("click", () => {
    dismissedUrl = state.pageUrl;
    removeOverlay();
  });
  root.querySelector(".primary").addEventListener("click", async () => {
    if (pidmReady === false)
      return;
    const button = root.querySelector(".primary");
    button.disabled = true;
    button.textContent = "Sending…";
    if (details.canSendPage) {
      button.textContent = "Loading qualities…";
      const response = await api.runtime.sendMessage({
        type: "pidm-resolve-formats",
        url: state.pageUrl
      });
      if (response?.ok) {
        renderQualityPicker(state, response.result);
        return;
      }
      button.disabled = false;
      button.textContent = response?.error || "Could not load qualities";
      return;
    }
    const response = await api.runtime.sendMessage({
        type: "pidm-send-batch",
        sourceTitle: state.title,
        links: details.strong
      });
    button.disabled = false;
    button.textContent = response?.ok ? "Sent to PIDM" : "PIDM unavailable";
    setTimeout(() => {
      if (button.isConnected)
        button.textContent = details.primaryLabel;
    }, 1600);
  });
}

function renderQualityPicker(state, result) {
  const root = overlayHost?.shadowRoot;
  const panel = root?.querySelector(".panel");
  const recommended = Array.isArray(result?.formats) ? result.formats : [];
  const videoOnly = Array.isArray(result?.videoOnlyFormats)
    ? result.videoOnlyFormats : [];
  const audioOnly = Array.isArray(result?.audioFormats)
    ? result.audioFormats : [];
  const allFormats = [...recommended, ...videoOnly, ...audioOnly];
  if (!panel || !recommended.length)
    return;
  qualityPanelOpen = true;
  panel.className = "panel qualities";
  panel.replaceChildren();

  const header = document.createElement("div");
  header.className = "quality-header";
  const mark = document.createElement("div");
  mark.className = "mark";
  mark.textContent = "P";
  const logo = document.createElement("img");
  logo.src = logoDataUrl;
  logo.alt = "";
  logo.addEventListener("error", () => logo.remove());
  if (logoDataUrl)
    mark.append(logo);
  const copy = document.createElement("div");
  copy.className = "copy";
  const brand = document.createElement("strong");
  brand.textContent = "Choose quality";
  const title = document.createElement("span");
  title.textContent = result.title || state.title || "Media";
  copy.append(brand, title);
  const close = document.createElement("button");
  close.className = "close";
  close.type = "button";
  close.title = "Hide PIDM controls";
  close.setAttribute("aria-label", "Hide PIDM controls");
  close.textContent = "×";
  close.addEventListener("click", () => {
    dismissedUrl = state.pageUrl;
    qualityPanelOpen = false;
    removeOverlay();
  });
  header.append(mark, copy, close);

  const label = document.createElement("div");
  label.className = "quality-label";
  label.textContent = result.recommendedLabel || "Recommended video";
  const select = document.createElement("select");
  select.setAttribute("aria-label", "Media quality");
  let showAdvanced = false;
  const appendGroup = (groupLabel, formats) => {
    if (!formats.length)
      return;
    const group = document.createElement("optgroup");
    group.label = groupLabel;
    formats.forEach(format => {
      const option = document.createElement("option");
      option.value = format.token;
      option.textContent = [
        format.quality,
        format.details,
        format.size
      ].filter(Boolean).join(" · ");
      group.append(option);
    });
    select.append(group);
  };
  const rebuildOptions = () => {
    select.replaceChildren();
    appendGroup(result.recommendedLabel || "Recommended video", recommended);
    if (showAdvanced) {
      appendGroup("Video only — no audio", videoOnly);
      appendGroup("Audio only", audioOnly);
    }
  };
  rebuildOptions();
  const note = document.createElement("div");
  note.className = "quality-note";
  note.textContent = "Recommended formats include video and audio when available.";

  const actions = document.createElement("div");
  actions.className = "quality-actions";
  const back = document.createElement("button");
  back.className = "secondary";
  back.type = "button";
  back.textContent = "Back";
  back.addEventListener("click", () => {
    qualityPanelOpen = false;
    renderOverlay(state);
  });
  const advancedCount = videoOnly.length + audioOnly.length;
  const advanced = document.createElement("button");
  advanced.className = "secondary";
  advanced.type = "button";
  advanced.textContent = `Show ${advancedCount} advanced`;
  advanced.hidden = advancedCount === 0;
  advanced.addEventListener("click", () => {
    showAdvanced = !showAdvanced;
    advanced.textContent = showAdvanced
      ? "Hide advanced"
      : `Show ${advancedCount} advanced`;
    note.textContent = showAdvanced
      ? "Advanced formats can be silent video or audio-only."
      : "Recommended formats include video and audio when available.";
    rebuildOptions();
  });
  const download = document.createElement("button");
  download.type = "button";
  download.textContent = "Download";
  download.addEventListener("click", async () => {
    const selected = allFormats.find(format => format.token === select.value);
    if (!selected)
      return;
    download.disabled = true;
    download.textContent = "Sending…";
    const response = await api.runtime.sendMessage({
      type: "pidm-download-format",
      token: selected.token
    });
    download.textContent = response?.ok ? "Sent to PIDM" : "Try again";
    if (response?.ok)
      setTimeout(removeOverlay, 700);
    else
      download.disabled = false;
  });
  actions.append(back, advanced, download);
  panel.append(header, label, select, note, actions);
}

async function checkPidmAvailability() {
  try {
    const response = await api.runtime.sendMessage({
      type: "pidm-check-health"
    });
    const next = response?.ready === true;
    if (next !== pidmReady) {
      pidmReady = next;
      if (!qualityPanelOpen)
        renderOverlay(pageState());
    }
  } catch {
    if (pidmReady !== false) {
      pidmReady = false;
      if (!qualityPanelOpen)
        renderOverlay(pageState());
    }
  }
}

function refreshPageState(forcePulse = false) {
  const state = pageState();
  const signature = JSON.stringify({
    url: state.pageUrl,
    eligible: state.pageEligible,
    network: state.networkMediaDetected,
    strong: strongCandidates(state.links).map(item => item.url),
    direct: directCandidates(state.links).map(item => item.url)
  });
  const changed = signature !== lastSignature;
  lastSignature = signature;
  if ((changed || forcePulse) && !qualityPanelOpen)
    renderOverlay(state, forcePulse || !overlayHost);
  if (changed) {
    api.runtime.sendMessage({
      type: "pidm-page-state-changed",
      state: {
        pageUrl: state.pageUrl,
        pageEligible: state.pageEligible,
        directCount: state.directCount
      }
    }).catch(() => {});
  }
}

function scheduleRefresh(delay = 350) {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => {
    if (location.href !== lastUrl) {
      lastUrl = location.href;
      lastSignature = "";
      dismissedUrl = "";
      networkMediaDetected = false;
      qualityPanelOpen = false;
      api.runtime.sendMessage({ type: "pidm-page-updated" }).catch(() => {});
    }
    refreshPageState();
  }, delay);
}

function patchHistory(method) {
  const original = history[method];
  history[method] = function(...args) {
    const result = original.apply(this, args);
    window.dispatchEvent(new Event("pidm-location-change"));
    return result;
  };
}

patchHistory("pushState");
patchHistory("replaceState");
window.addEventListener("pidm-location-change", () => scheduleRefresh(100));
window.addEventListener("popstate", () => scheduleRefresh(100));
window.addEventListener("hashchange", () => scheduleRefresh(100));
window.addEventListener("yt-navigate-finish", () => scheduleRefresh(150));
setInterval(() => {
  if (location.href !== lastUrl)
    scheduleRefresh(0);
}, 750);
setInterval(checkPidmAvailability, 5000);

new MutationObserver(() => scheduleRefresh()).observe(document.documentElement, {
  childList: true,
  subtree: true,
  attributes: true,
  attributeFilter: ["href", "src"]
});

api.storage.sync.get({ showPageControls: true }).then(settings => {
  showPageControls = settings.showPageControls !== false;
  refreshPageState(true);
});
checkPidmAvailability();
loadOptionalLogo();
api.storage.onChanged.addListener((changes, areaName) => {
  if (areaName === "sync" && changes.showPageControls) {
    showPageControls = changes.showPageControls.newValue !== false;
    if (!showPageControls) {
      qualityPanelOpen = false;
      removeOverlay();
    } else {
      refreshPageState(true);
    }
  }
});

api.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type === "pidm-collect-links") {
    sendResponse(pageState());
    return true;
  }
  if (message?.type === "pidm-network-media-detected") {
    networkMediaDetected = true;
    qualityPanelOpen = false;
    refreshPageState(true);
    sendResponse({ ok: true });
    return true;
  }
  return false;
});
