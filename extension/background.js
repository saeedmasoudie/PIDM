const api = globalThis.browser ?? globalThis.chrome;
const BRIDGE = "http://127.0.0.1:17654";
const mediaByTab = new Map();

function parsedUrl(value) {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function isYouTubeHost(hostname) {
  return hostname === "youtube.com"
    || hostname.endsWith(".youtube.com")
    || hostname === "youtu.be"
    || hostname.endsWith(".youtu.be");
}

function isYouTubeUrl(value) {
  const url = parsedUrl(value);
  return Boolean(url && isYouTubeHost(url.hostname));
}

function isYouTubeMediaPage(value) {
  const url = parsedUrl(value);
  if (!url || !isYouTubeHost(url.hostname))
    return true;
  if (url.hostname.endsWith("youtu.be"))
    return url.pathname.split("/").filter(Boolean).length === 1;
  return (
    (url.pathname === "/watch" && Boolean(url.searchParams.get("v")))
    || /^\/shorts\/[^/]+/.test(url.pathname)
    || /^\/live\/[^/]+/.test(url.pathname)
  );
}

function isDownloadable(url, contentType = "") {
  const clean = String(url || "").split(/[?#]/, 1)[0].toLowerCase();
  return /\.(?:mp4|webm|mkv|mov|m4v|mp3|m4a|aac|ogg|oga|opus|flac|wav|avi|wmv)$/.test(clean)
    || /^(?:video|audio)\//i.test(contentType);
}

function isManifest(url, contentType = "") {
  const clean = String(url || "").split(/[?#]/, 1)[0].toLowerCase();
  return /\.(?:m3u8|mpd)$/.test(clean)
    || /(?:mpegurl|dash\+xml)/i.test(contentType);
}

function isMediaSegment(url) {
  const clean = String(url || "").split(/[?#]/, 1)[0].toLowerCase();
  return /\.(?:ts|m4s|cmfv|cmfa)$/.test(clean);
}

function isYouTubeTransport(details) {
  const resource = parsedUrl(details.url);
  return resource?.hostname.endsWith(".googlevideo.com")
    || isYouTubeUrl(details.documentUrl || "")
    || isYouTubeUrl(details.initiator || "");
}

function rememberMedia(tabId, item) {
  if (tabId < 0 || !item.url || item.url.startsWith("blob:"))
    return;
  let items = mediaByTab.get(tabId) || [];
  const samePage = existing => (
    existing.pageUrl
    && item.pageUrl
    && existing.pageUrl === item.pageUrl
  );
  if (item.kind === "stream") {
    if (items.some(existing => existing.kind === "stream" && samePage(existing)))
      return;
    items = items.filter(existing => !samePage(existing));
  } else if (items.some(existing => existing.kind === "stream" && samePage(existing))) {
    return;
  } else if (item.kind === "video") {
    if (
      items.some(
        existing => existing.url === item.url
          || (existing.kind === "video" && samePage(existing))
      )
    )
      return;
    items = items.filter(
      existing => !(existing.kind === "audio" && samePage(existing))
    );
  } else if (item.kind === "audio") {
    if (
      items.some(
        existing => ["video", "audio"].includes(existing.kind)
          && (existing.url === item.url || samePage(existing))
      )
    )
      return;
  } else if (items.some(existing => existing.url === item.url)) {
    return;
  }
  items.unshift(item);
  mediaByTab.set(tabId, items.slice(0, 100));
  Promise.resolve(api.tabs.sendMessage(tabId, {
    type: "pidm-network-media-detected",
    kind: item.kind,
    pageUrl: item.pageUrl
  })).catch(() => {});
}

async function postToPidm(path, payload) {
  const response = await fetch(`${BRIDGE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.error || `PIDM returned ${response.status}`);
  }
  return response.json();
}

async function pidmIsReady() {
  try {
    const response = await fetch(`${BRIDGE}/health`);
    if (!response.ok)
      return false;
    const status = await response.json();
    return status?.application === "PIDM" && status?.ready === true;
  } catch {
    return false;
  }
}

async function captureBrowserDownload(item) {
  if (item?.id === undefined || !/^https?:/i.test(item.finalUrl || item.url || ""))
    return;
  const settings = await api.storage.sync.get({
    captureBrowserDownloads: true
  });
  if (settings.captureBrowserDownloads === false)
    return;
  let paused = false;
  await Promise.resolve(api.downloads.pause(item.id))
    .then(() => { paused = true; })
    .catch(() => {});
  if (!(await pidmIsReady())) {
    if (paused)
      await Promise.resolve(api.downloads.resume(item.id)).catch(() => {});
    return;
  }
  try {
    await postToPidm("/single", {
      url: item.finalUrl || item.url,
      source: "browser-download",
      filename: item.filename || ""
    });
    await Promise.resolve(api.downloads.cancel(item.id)).catch(() => {});
    await Promise.resolve(api.downloads.erase({ id: item.id })).catch(() => {});
  } catch {
    if (paused)
      await Promise.resolve(api.downloads.resume(item.id)).catch(() => {});
  }
}

api.webRequest.onCompleted.addListener(
  details => {
    if (isYouTubeTransport(details))
      return;
    const contentType = (details.responseHeaders || [])
      .find(header => header.name.toLowerCase() === "content-type")?.value || "";
    if (isManifest(details.url, contentType)) {
      rememberMedia(details.tabId, {
        url: details.url,
        title: "Adaptive media stream",
        kind: "stream",
        pageUrl: details.documentUrl || details.initiator || ""
      });
    } else if (
      !isMediaSegment(details.url)
      && (details.type === "media" || isDownloadable(details.url, contentType))
    ) {
      rememberMedia(details.tabId, {
        url: details.url,
        title: details.url.split("/").pop() || "Captured media",
        kind: contentType.startsWith("audio/") ? "audio" : "video",
        pageUrl: details.documentUrl || details.initiator || ""
      });
    }
  },
  { urls: ["<all_urls>"] },
  ["responseHeaders"]
);

api.downloads.onCreated.addListener(captureBrowserDownload);

api.runtime.onInstalled.addListener(() => {
  api.storage.sync.get({
    showPageControls: true,
    captureBrowserDownloads: true
  }).then(settings => {
    const updates = {};
    if (typeof settings.showPageControls !== "boolean")
      updates.showPageControls = true;
    if (typeof settings.captureBrowserDownloads !== "boolean")
      updates.captureBrowserDownloads = true;
    if (Object.keys(updates).length)
      return api.storage.sync.set(updates);
  });
  api.contextMenus.create({
    id: "pidm-link",
    title: "Download link with PIDM",
    contexts: ["link"]
  });
  api.contextMenus.create({
    id: "pidm-media",
    title: "Download media with PIDM",
    contexts: ["video", "audio"]
  });
  api.contextMenus.create({
    id: "pidm-page",
    title: "Open media page in PIDM",
    contexts: ["page"]
  });
});

api.contextMenus.onClicked.addListener(async info => {
  const youtubePage = isYouTubeUrl(info.pageUrl || "");
  const pageEligible = isYouTubeMediaPage(info.pageUrl || "");
  if (
    youtubePage
    && ["pidm-page", "pidm-media"].includes(info.menuItemId)
    && !pageEligible
  )
    return;
  const url = youtubePage && info.menuItemId === "pidm-media"
    ? info.pageUrl
    : info.linkUrl || info.srcUrl || info.pageUrl;
  if (!url)
    return;
  try {
    await postToPidm("/single", { url });
  } catch {
  }
});

api.contextMenus.onShown?.addListener((info, tab) => {
  const pageUrl = info.pageUrl || tab?.url || "";
  const visible = !isYouTubeUrl(pageUrl) || isYouTubeMediaPage(pageUrl);
  api.contextMenus.update("pidm-page", { visible });
  api.contextMenus.update("pidm-media", { visible });
  api.contextMenus.refresh?.();
});

api.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === "pidm-media-for-tab") {
    sendResponse({ items: mediaByTab.get(message.tabId) || [] });
    return true;
  }
  if (message?.type === "pidm-clear-tab") {
    mediaByTab.delete(message.tabId);
    sendResponse({ ok: true });
    return true;
  }
  if (message?.type === "pidm-page-updated") {
    if (sender.tab?.id >= 0)
      mediaByTab.delete(sender.tab.id);
    sendResponse({ ok: true });
    return true;
  }
  if (message?.type === "pidm-send-single") {
    postToPidm("/single", { url: message.url })
      .then(result => sendResponse({ ok: true, result }))
      .catch(error => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  if (message?.type === "pidm-send-batch") {
    postToPidm("/batch", {
      sourceTitle: message.sourceTitle || "",
      links: message.links || []
    })
      .then(result => sendResponse({ ok: true, result }))
      .catch(error => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  if (message?.type === "pidm-resolve-formats") {
    postToPidm("/media/formats", { url: message.url })
      .then(result => sendResponse({ ok: true, result }))
      .catch(error => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  if (message?.type === "pidm-download-format") {
    postToPidm("/media/download", { token: message.token })
      .then(result => sendResponse({ ok: true, result }))
      .catch(error => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  if (message?.type === "pidm-check-health") {
    pidmIsReady().then(ready => sendResponse({ ready }));
    return true;
  }
  return false;
});

api.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (changeInfo.status === "loading" || changeInfo.url)
    mediaByTab.delete(tabId);
});
api.tabs.onRemoved.addListener(tabId => mediaByTab.delete(tabId));
