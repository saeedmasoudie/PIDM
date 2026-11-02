const api = globalThis.browser ?? globalThis.chrome;
const BRIDGE = "http://127.0.0.1:17654";
const state = {
  tab: null,
  title: "",
  pageUrl: "",
  pageEligible: true,
  pageMessage: "",
  siteMode: "standard",
  items: [],
  formats: [],
  qualityResult: null,
  showAdvancedFormats: false,
  pidmReady: false
};
let refreshTimer = 0;

const elements = {
  pageTitle: document.querySelector("#pageTitle"),
  pageHint: document.querySelector("#pageHint"),
  items: document.querySelector("#items"),
  count: document.querySelector("#count"),
  status: document.querySelector("#status"),
  pidmWarning: document.querySelector("#pidmWarning"),
  sendPage: document.querySelector("#sendPage"),
  sendBatch: document.querySelector("#sendBatch"),
  showPageControls: document.querySelector("#showPageControls"),
  captureBrowserDownloads: document.querySelector("#captureBrowserDownloads"),
  qualityPicker: document.querySelector("#qualityPicker"),
  qualityTitle: document.querySelector("#qualityTitle"),
  qualitySummary: document.querySelector("#qualitySummary"),
  qualitySelect: document.querySelector("#qualitySelect"),
  toggleAdvancedFormats: document.querySelector("#toggleAdvancedFormats"),
  downloadQuality: document.querySelector("#downloadQuality")
};
document.querySelector("#brandLogo").addEventListener("error", event => {
  event.currentTarget.remove();
});

function selectedItems() {
  return state.items.filter(item => item.selected);
}

function updateCount() {
  const count = selectedItems().length;
  elements.count.textContent = `${count} selected`;
  elements.sendBatch.disabled = count === 0 || !state.pidmReady;
}

function render() {
  elements.pageTitle.textContent = state.title || state.pageUrl || "Current page";
  elements.sendPage.disabled = !state.pageEligible || !state.pidmReady;
  elements.sendPage.textContent = state.siteMode === "youtube"
    ? "Choose video quality"
    : "Open media page";
  elements.pageHint.hidden = !state.pageMessage;
  elements.pageHint.textContent = state.pageMessage;
  elements.items.replaceChildren();
  if (!state.items.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = state.pageEligible
      ? "No direct links were found. Open the media page so PIDM can inspect it with yt-dlp."
      : state.pageMessage || "PIDM capture is not available on this page.";
    elements.items.append(empty);
  }
  state.items.forEach((item, index) => {
    const row = document.createElement("label");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = item.selected;
    checkbox.addEventListener("change", () => {
      state.items[index].selected = checkbox.checked;
      updateCount();
    });
    const copy = document.createElement("div");
    copy.className = "item-copy";
    const title = document.createElement("strong");
    title.textContent = item.title || item.url;
    const url = document.createElement("span");
    url.textContent = item.url;
    copy.append(title, url);
    const kind = document.createElement("span");
    kind.className = "kind";
    kind.textContent = item.kind || "link";
    row.append(checkbox, copy, kind);
    elements.items.append(row);
  });
  updateCount();
}

function appendFormatGroup(label, formats) {
  if (!formats?.length)
    return;
  const group = document.createElement("optgroup");
  group.label = label;
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
  elements.qualitySelect.append(group);
}

function populateQualitySelect() {
  const result = state.qualityResult;
  if (!result)
    return;
  elements.qualitySelect.replaceChildren();
  appendFormatGroup(
    result.recommendedLabel || "Recommended video",
    result.formats || []
  );
  if (state.showAdvancedFormats) {
    appendFormatGroup(
      "Video only — no audio",
      result.videoOnlyFormats || []
    );
    appendFormatGroup("Audio only", result.audioFormats || []);
  }
  const advancedCount = (
    (result.videoOnlyFormats?.length || 0)
    + (result.audioFormats?.length || 0)
  );
  elements.toggleAdvancedFormats.hidden = advancedCount === 0;
  elements.toggleAdvancedFormats.textContent = state.showAdvancedFormats
    ? "Hide advanced formats"
    : `Show ${advancedCount} video-only and audio formats`;
  elements.qualitySummary.textContent = state.showAdvancedFormats
    ? "Advanced formats can be silent video or audio-only."
    : "Recommended formats include both video and audio when available.";
}

async function checkPidmAvailability() {
  const previous = state.pidmReady;
  try {
    const response = await api.runtime.sendMessage({
      type: "pidm-check-health"
    });
    state.pidmReady = response?.ready === true;
  } catch {
    state.pidmReady = false;
  }
  elements.pidmWarning.hidden = state.pidmReady;
  if (!state.pidmReady) {
    elements.status.className = "error";
    elements.status.textContent = "PIDM is not running";
  }
  if (state.pidmReady !== previous)
    render();
  return state.pidmReady;
}

function mergeItems(...groups) {
  const seen = new Set();
  return groups.flat().filter(item => {
    if (!item?.url || seen.has(item.url))
      return false;
    seen.add(item.url);
    return true;
  }).map(item => ({
    ...item,
    pageUrl: item.kind === "stream"
      ? state.pageUrl
      : item.pageUrl || state.pageUrl,
    selected: ["video", "audio", "file", "stream"].includes(item.kind)
  }));
}

async function scan() {
  elements.status.className = "";
  elements.status.textContent = "Scanning page…";
  const [tab] = await api.tabs.query({ active: true, currentWindow: true });
  state.tab = tab;
  state.formats = [];
  state.qualityResult = null;
  state.showAdvancedFormats = false;
  elements.qualityPicker.hidden = true;
  state.pageUrl = tab?.url || "";
  state.title = tab?.title || "";
  let page = { links: [], pageEligible: false };
  let captured = { items: [] };
  try {
    page = await api.tabs.sendMessage(tab.id, { type: "pidm-collect-links" });
  } catch {
    page.message = "This browser page does not allow extensions to inspect its content.";
  }
  try {
    captured = await api.runtime.sendMessage({
      type: "pidm-media-for-tab",
      tabId: tab.id
    });
  } catch {
  }
  state.title = page?.title || state.title;
  state.pageUrl = page?.pageUrl || state.pageUrl;
  state.pageEligible = page?.pageEligible !== false;
  state.pageMessage = page?.message || "";
  state.siteMode = page?.siteMode || "standard";
  state.items = state.siteMode === "youtube"
    ? []
    : mergeItems(captured?.items || [], page?.links || []);
  await checkPidmAvailability();
  if (state.pidmReady) {
    elements.status.className = "";
    elements.status.textContent = state.pageEligible
      ? "PIDM is ready"
      : "Waiting for a supported page";
  }
  render();
}

async function post(path, payload) {
  let response;
  try {
    response = await fetch(`${BRIDGE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  } catch {
    state.pidmReady = false;
    elements.pidmWarning.hidden = false;
    render();
    throw new Error("PIDM is not running. Open the desktop app and try again.");
  }
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.error || `PIDM returned ${response.status}`);
  }
  return response.json();
}

document.querySelector("#refresh").addEventListener("click", scan);
document.querySelector("#selectAll").addEventListener("click", () => {
  state.items.forEach(item => { item.selected = true; });
  render();
});
document.querySelector("#selectNone").addEventListener("click", () => {
  state.items.forEach(item => { item.selected = false; });
  render();
});
elements.sendPage.addEventListener("click", async () => {
  if (!state.pageEligible || !state.pidmReady)
    return;
  try {
    elements.sendPage.disabled = true;
    elements.sendPage.textContent = "Loading qualities…";
    const result = await post("/media/formats", { url: state.pageUrl });
    state.qualityResult = result;
    state.formats = [
      ...(result.formats || []),
      ...(result.videoOnlyFormats || []),
      ...(result.audioFormats || [])
    ];
    elements.qualityTitle.textContent = result.title || "Choose quality";
    populateQualitySelect();
    elements.qualityPicker.hidden = state.formats.length === 0;
    elements.sendPage.textContent = "Refresh qualities";
    elements.sendPage.disabled = false;
  } catch (error) {
    elements.sendPage.disabled = false;
    elements.sendPage.textContent = state.siteMode === "youtube"
      ? "Choose video quality"
      : "Open media page";
    elements.status.className = "error";
    elements.status.textContent = error.message;
  }
});
elements.toggleAdvancedFormats.addEventListener("click", () => {
  state.showAdvancedFormats = !state.showAdvancedFormats;
  populateQualitySelect();
});
elements.downloadQuality.addEventListener("click", async () => {
  const format = state.formats.find(
    item => item.token === elements.qualitySelect.value
  );
  if (!format)
    return;
  elements.downloadQuality.disabled = true;
  elements.downloadQuality.textContent = "Sending to PIDM…";
  try {
    await post("/media/download", { token: format.token });
    window.close();
  } catch (error) {
    elements.downloadQuality.disabled = false;
    elements.downloadQuality.textContent = "Download selected quality";
    elements.status.className = "error";
    elements.status.textContent = error.message;
  }
});
elements.sendBatch.addEventListener("click", async () => {
  try {
    const result = await post("/batch", {
      sourceTitle: state.title,
      links: selectedItems()
    });
    elements.status.className = "";
    elements.status.textContent = `${result.accepted} links sent`;
    setTimeout(() => window.close(), 350);
  } catch (error) {
    elements.status.className = "error";
    elements.status.textContent = error.message;
  }
});
elements.showPageControls.addEventListener("change", () => {
  api.storage.sync.set({
    showPageControls: elements.showPageControls.checked
  });
});
elements.captureBrowserDownloads.addEventListener("change", () => {
  api.storage.sync.set({
    captureBrowserDownloads: elements.captureBrowserDownloads.checked
  });
});
api.storage.sync.get({
  showPageControls: true,
  captureBrowserDownloads: true
}).then(settings => {
  elements.showPageControls.checked = settings.showPageControls !== false;
  elements.captureBrowserDownloads.checked =
    settings.captureBrowserDownloads !== false;
});
api.runtime.onMessage.addListener(message => {
  if (message?.type !== "pidm-page-state-changed")
    return false;
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(scan, 120);
  return false;
});
setInterval(checkPidmAvailability, 4000);

scan();
