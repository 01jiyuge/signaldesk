"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const activeStatuses = new Set(["queued", "running"]);
  const statusNames = { queued: "等待执行", running: "研究中", interrupted: "等待继续", completed: "已完成", failed: "执行失败", cancelled: "已取消" };
  const categoryNames = { pricing: "价格与套餐", features: "产品功能", positioning: "市场定位", other: "其他发现" };
  const impactNames = { high: "高影响", medium: "中等影响", low: "低影响" };
  const changeNames = { baseline: "首次记录", changed: "检测到变化", unchanged: "与上次一致", new: "新增来源" };
  const stageNames = { collect: "采集原始来源", collecting: "采集原始来源", fetch: "采集原始来源", compare: "比较历史快照", analyze: "分析变化与影响", analyzing: "分析变化与影响", research: "开展自主调研", verify: "检查引用一致性", verify_evidence: "检查引用一致性", report: "整理研究报告", persist: "保存研究记录", completed: "研究已完成", queued: "任务已加入队列", running: "研究正在进行" };
  const stages = ["collect", "compare", "research", "verify", "persist"];
  const state = { projects: [], project: null, runs: [], run: null, events: [], eventAfter: 0, health: null, tab: "report", timer: null, epoch: 0, actionBusy: false, exportBusy: false, runsLoading: false, runGenerations: new Map(), searchEpoch: 0, searchQuery: "", searchController: null, pollingError: "", editProjectId: null, projectFormEpoch: 0, extraFocus: [], disconnected: false, connectionEpoch: 0, connectionStatus: "connecting", connectionBlocked: true, connectionController: null, streamController: null, streamTimer: null, streamRunId: null, streamStatus: "idle", streamSupported: null, backgroundStreams: new Map() };
  let toastTimer;
  let memoryEpoch = 0;
  let memoryBusy = false;
  let memoryController = null;
  let memoryFormEpoch = 0;
  const extensions = { capabilities: null, capabilityError: "", loading: false, capabilityEpoch: 0, context: null, submissions: new Map() };
  const sandboxActive = new Set(["queued", "running", "pending", "cancelling", "canceling"]);
  const delegationActive = new Set(["TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"]);

  function node(tag, className, text) {
    const item = document.createElement(tag);
    if (className) item.className = className;
    if (text !== undefined && text !== null) item.textContent = String(text);
    return item;
  }

  function token() {
    try { return sessionStorage.getItem("signaldesk.apiToken") || ""; } catch { return ""; }
  }

  function readableError(detail) {
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) return detail.map((x) => typeof x === "string" ? x : `${(x.loc || []).join(".")}: ${x.msg || "请求参数有误"}`).join("\n");
    if (detail && typeof detail === "object") return detail.message || detail.error || JSON.stringify(detail);
    return "服务暂时无法完成请求，请稍后重试。";
  }

  function renderConnectionControls() {
    const blocked = state.connectionBlocked || state.disconnected;
    $("submit-project").disabled = blocked || state.actionBusy;
    $("save-memory").disabled = blocked || memoryBusy;
    $("search-evidence").disabled = blocked || !!state.searchController;
    document.querySelectorAll('[data-write-action="memory-delete"]').forEach((button) => { button.disabled = blocked || memoryBusy; });
  }

  function connectionState(disconnected, message = "") {
    state.disconnected = disconnected;
    const labels = { connecting: "正在验证服务连接", auth_error: "访问验证失败 · 检查 Token", error: "服务连接失败 · 点击重连", ready: "服务已连接" };
    const messages = { connecting: "正在验证 API 访问权限，验证完成后可提交操作。已有任务继续在后台运行。", auth_error: "API 访问验证未通过。请在「服务连接」填写正确的 API Token 后重新连接。", error: "暂时无法验证 API 访问权限，请重新连接。已有结果仍可查看，后台任务继续运行。" };
    $("connection-notice").hidden = !disconnected && !state.connectionBlocked;
    $("connection-message").textContent = message || (disconnected ? "与研究服务的连接暂时中断。已有结果仍可查看，后台任务不会因关闭页面而停止。" : messages[state.connectionStatus]) || "";
    $("health-dot").className = `health-dot ${disconnected || state.connectionBlocked ? "offline" : "online"}`;
    $("health-label").textContent = disconnected ? "连接中断 · 点击重连" : labels[state.connectionStatus] || labels.error;
    if (!state.projects.length) renderProjects();
    renderMode(); renderExport(); renderProgress(); renderExtensionAvailability();
  }

  async function request(path, options = {}) {
    const connectionEpoch = state.connectionEpoch;
    if (!["GET", "HEAD"].includes((options.method || "GET").toUpperCase()) && (state.connectionBlocked || state.disconnected)) throw new Error("服务访问权限尚未验证，请在「服务连接」重新连接后再提交操作。");
    const headers = new Headers(options.headers || {});
    if (!headers.has("Accept")) headers.set("Accept", "application/json");
    if (options.body !== undefined) headers.set("Content-Type", "application/json");
    const auth = token();
    if (auth) headers.set("Authorization", `Bearer ${auth}`);
    let response;
    try { response = await fetch(`/api${path}`, { ...options, headers, cache: "no-store" }); }
    catch (error) {
      if (options.signal?.aborted || error.name === "AbortError") throw error;
      if (connectionEpoch === state.connectionEpoch) connectionState(true);
      throw new Error("无法连接研究服务，正在等待恢复连接。请确认服务已启动，或点击「重新连接」。");
    }
    if (connectionEpoch === state.connectionEpoch) {
      if ([401, 403].includes(response.status)) { state.connectionStatus = "auth_error"; state.connectionBlocked = true; connectionState(false); }
      else if (state.disconnected) connectionState(false);
    }
    return response;
  }

  async function responseError(response) {
    const text = await response.text();
    let data;
    try { data = text ? JSON.parse(text) : null; } catch { data = null; }
    const error = new Error(response.status === 401 || response.status === 403 ? "API 访问验证未通过。请在左下角「服务连接」填写正确的 API Token。" : readableError(data?.detail || data?.message || data?.error || `请求失败（HTTP ${response.status}）`));
    error.status = response.status;
    return error;
  }

  async function api(path, options = {}) {
    const response = await request(path, options);
    if (!response.ok) throw await responseError(response);
    const text = await response.text();
    let data;
    try { data = text ? JSON.parse(text) : null; } catch { data = null; }
    return data;
  }

  function collection(data, name) {
    return Array.isArray(data) ? data : (Array.isArray(data?.[name]) ? data[name] : Array.isArray(data?.items) ? data.items : []);
  }

  function safeUrl(value) {
    try { const url = new URL(String(value)); return ["http:", "https:"].includes(url.protocol) ? url.href : null; } catch { return null; }
  }

  function externalLink(value, text, className) {
    const href = safeUrl(value);
    const result = node(href ? "a" : "span", className, text || value || "未提供来源链接");
    if (href) { result.href = href; result.target = "_blank"; result.rel = "noopener noreferrer"; }
    return result;
  }

  function domain(value) { try { return new URL(value).hostname; } catch { return "无有效链接"; } }
  function dateTime(value, short = false, withSeconds = false) {
    if (!value) return "";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", ...(short ? {} : { year: "numeric" }), ...(withSeconds ? { second: "2-digit" } : {}) }).format(date);
  }
  function timeOnly(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? "" : new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(date);
  }
  function isDemoProject(project) { return !!project?.sources?.length && project.sources.every((source) => ["northstar.example", "orbit.example"].includes(domain(source.url))); }
  function findings() { return Array.isArray(state.run?.report?.findings) ? state.run.report.findings : []; }
  function evidence() { return Array.isArray(state.run?.report?.evidence) ? state.run.report.evidence : []; }
  function currentEvidence(report = state.run?.report) { return (Array.isArray(report?.evidence) ? report.evidence : []).filter((item) => !item.is_previous); }
  function isActive() { return !!state.run && activeStatuses.has(state.run.status); }
  function projectActive() { return state.runs.some((run) => activeStatuses.has(run.status)); }

  function reportResult(report = state.run?.report) {
    const current = currentEvidence(report);
    const counts = { baseline: 0, changed: 0, unchanged: 0 };
    current.forEach((item) => { if (Object.hasOwn(counts, item.change_type)) counts[item.change_type]++; });
    const hasFindings = !!report?.findings?.length;
    if (current.length && counts.unchanged === current.length && !hasFindings) return { kind: "unchanged", label: "来源无变化", counts, total: current.length };
    if (counts.changed) return { kind: "changed", label: "检测到变化", counts, total: current.length };
    if (counts.baseline) return { kind: "baseline", label: counts.baseline === current.length ? "首次基线" : "新增来源基线", counts, total: current.length };
    return { kind: "research", label: hasFindings ? "研究发现" : "暂无新的关键发现", counts, total: current.length };
  }

  function showError(message) { $("global-error").textContent = message; $("global-error").hidden = !message; }
  function toast(message, error = false) {
    clearTimeout(toastTimer);
    $("toast").textContent = message;
    $("toast").classList.toggle("error", error);
    $("toast").hidden = false;
    toastTimer = setTimeout(() => { $("toast").hidden = true; }, 4200);
  }

  function empty(container, title, detail, symbol = "⌁", running = false) {
    const wrapper = node("div", `empty-state${running ? " research-active" : ""}`);
    const icon = node("div", "empty-symbol", symbol); icon.setAttribute("aria-hidden", "true");
    wrapper.append(icon, node("h3", "", title), node("p", "", detail));
    container.replaceChildren(wrapper);
  }

  function renderProjects() {
    const list = $("project-list"); list.replaceChildren();
    if (!state.projects.length) {
      const hints = { connecting: "正在验证工作区访问权限…", auth_error: "访问未验证，请检查 Token 后重新连接。", error: "工作区连接未验证，请重新连接。" };
      const message = state.connectionBlocked ? hints[state.connectionStatus] || hints.error : state.disconnected ? "工作区暂时无法连接，请稍后重试。" : "暂无项目，开始第一份研究";
      list.append(node("p", "sidebar-hint", message)); return;
    }
    state.projects.forEach((project, index) => {
      const button = node("button", `project-nav${state.project?.id === project.id ? " active" : ""}`);
      button.type = "button"; button.title = project.name;
      if (state.project?.id === project.id) button.setAttribute("aria-current", "page");
      const dot = node("span", "project-dot"); dot.setAttribute("aria-hidden", "true");
      button.append(dot, node("span", "project-name", project.name), node("span", "project-number", String(index + 1).padStart(2, "0")));
      button.addEventListener("click", () => { if (state.project?.id !== project.id) selectProject(project); });
      list.append(button);
    });
  }

  function renderSources() {
    const sources = state.project?.sources || [];
    $("metric-sources").textContent = sources.length;
    $("source-button-count").textContent = `(${sources.length})`;
    $("sources-total").textContent = `${sources.length} 个`;
    $("sources-list").replaceChildren();
    sources.forEach((source) => {
      const item = node("div", "source-item");
      const details = node("div", "source-details");
      details.append(node("strong", "", source.name || domain(source.url)), externalLink(source.url, domain(source.url), ""));
      const arrow = node("span", "source-external", "↗"); arrow.setAttribute("aria-hidden", "true");
      item.append(node("div", "source-avatar", (source.name || "S").slice(0, 1).toUpperCase()), details, arrow);
      $("sources-list").append(item);
    });
    $("focus-list").replaceChildren(...(state.project?.focus || []).map((focus) => node("span", "focus-tag", categoryNames[focus] || focus)));
  }

  function renderMode() {
    const demo = $("research-mode").value === "demo";
    $("demo-revision").hidden = !demo;
    $("demo-notice").hidden = !demo;
    $("live-config-notice").hidden = demo || state.health?.live_configured !== false;
    $("run-mode-hint").textContent = demo ? "内置虚构样本 · 无需模型 Key" : state.health?.live_configured ? "真实来源 · 使用服务端模型配置" : "真实调研未配置：请在服务端配置模型 Key";
    $("start-run").disabled = state.actionBusy || projectActive() || state.runsLoading || state.connectionBlocked || state.disconnected || (!demo && state.health?.live_configured === false);
    $("start-run").title = !demo && state.health?.live_configured === false ? "请先配置服务端模型环境变量，再重新连接服务" : "";
    $("edit-project").disabled = state.actionBusy || projectActive() || state.runsLoading || state.connectionBlocked || state.disconnected;
    $("edit-project").title = projectActive() ? "项目有等待或执行中的任务，结束后可编辑；已有历史会保留" : state.runsLoading ? "正在读取任务状态" : "编辑项目、关注来源与研究重点";
    $("edit-lock-note").hidden = !projectActive();
    renderConnectionControls();
  }

  function renderExport() {
    $("report-actions").hidden = state.run?.status !== "completed" || !state.run?.report;
    const result = reportResult();
    $("report-result-type").textContent = result.label;
    $("report-result-type").className = `result-label ${result.kind}`;
    $("export-markdown").disabled = state.exportBusy || state.connectionBlocked || state.disconnected;
    $("export-json").disabled = state.exportBusy || state.connectionBlocked || state.disconnected;
  }

  function renderPollingError() {
    $("poll-error").hidden = !state.pollingError;
    $("poll-error-message").textContent = state.pollingError;
  }

  function renderTransport() {
    const labels = { idle: "等待事件连接", connecting: "AG-UI · 连接中", connected: "AG-UI · 实时连接", reconnecting: "AG-UI · 正在重连", ended: "AG-UI · 事件已同步", fallback: "JSON · 兼容轮询" };
    const background = state.backgroundStreams.size;
    const retrying = [...state.backgroundStreams.values()].filter((stream) => stream.status === "reconnecting").length;
    $("transport-state").textContent = `${labels[state.streamStatus] || labels.idle}${background ? ` · ${background} 个后台订阅` : ""}${retrying ? `（${retrying} 个重连中）` : ""}`;
    $("transport-state").className = `transport-state ${state.streamStatus}`;
    $("retry-stream").hidden = !["reconnecting", "fallback"].includes(state.streamStatus);
    $("transport-note").textContent = state.streamStatus === "fallback" ? "当前服务未提供可用的 AG-UI 流，使用 JSON 轮询兼容模式。" : state.streamStatus === "idle" ? "选择研究后订阅 AG-UI 任务事件。" : "订阅已有任务的 AG-UI SSE 事件；断开页面不会取消后台研究。";
  }

  function updateElapsed() {
    const run = state.run;
    $("run-timing").hidden = !run;
    if (!run) return;
    const start = new Date(run.created_at).getTime();
    const end = isActive() ? Date.now() : new Date(run.updated_at).getTime();
    if (!Number.isFinite(start) || !Number.isFinite(end)) { $("run-timing").hidden = true; return; }
    const seconds = Math.max(0, Math.floor((end - start) / 1000));
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    $("elapsed-time").textContent = `${hours ? `${hours} 小时 ` : ""}${minutes ? `${minutes} 分 ` : ""}${seconds % 60} 秒`;
    $("run-timing").title = "从任务创建至当前状态的时间，包含排队与暂停；不是模型调用耗时。";
  }

  function renderStages() {
    const run = state.run;
    const list = $("stage-list");
    list.hidden = !run;
    const activeIndex = stages.indexOf(run?.stage);
    list.replaceChildren(...stages.map((stage, index) => {
      const done = run?.status === "completed" || (activeIndex >= 0 && index < activeIndex);
      const current = activeIndex === index && run?.status !== "completed";
      const status = done ? "done" : current ? (run?.status === "failed" ? "failed" : "current") : "pending";
      const item = node("li", `stage-item ${status}`);
      if (current) item.setAttribute("aria-current", "step");
      const mark = node("span", "stage-mark", done ? "✓" : String(index + 1)); mark.setAttribute("aria-hidden", "true");
      const label = stage === "research" && run?.mode === "demo" ? "生成演示分析" : stage === "research" && run?.report?.stats?.model_call_skipped ? "记录来源一致结果" : stageNames[stage];
      item.append(mark, node("span", "", label), node("span", "sr-only", done ? "已完成" : current ? statusNames[run.status] : "未开始"));
      return item;
    }));
  }

  function setTab(name, focus = false) {
    state.tab = name;
    ["report", "evidence", "history"].forEach((tab) => {
      $("tab-" + tab).classList.toggle("active", name === tab);
      $("tab-" + tab).setAttribute("aria-selected", String(name === tab));
      $("tab-" + tab).tabIndex = name === tab ? 0 : -1;
      $("view-" + tab).hidden = name !== tab;
    });
    if (focus) $("tab-" + name).focus();
  }

  function compareRunTime(left, right) {
    const a = Date.parse(left); const b = Date.parse(right);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return Number.isFinite(a) ? 1 : Number.isFinite(b) ? -1 : 0;
    if (a !== b) return a - b;
    // Date.parse truncates sub-millisecond precision; server timestamps retain microseconds.
    const remainder = (value) => Number((String(value).match(/\.(\d+)(?:Z|[+-]\d{2}:?\d{2})$/i)?.[1] || "").slice(3).padEnd(6, "0").slice(0, 6));
    return remainder(left) - remainder(right);
  }

  function runGeneration(runId) { return state.runGenerations.get(runId) || 0; }

  function invalidateRunObservation(runId) {
    const generation = runGeneration(runId) + 1; state.runGenerations.set(runId, generation);
    if (state.run?.id === runId) {
      stopPolling(); state.streamController?.abort(); state.streamController = null;
      clearTimeout(state.streamTimer); state.streamTimer = null; state.streamRunId = null; state.streamStatus = "idle";
    }
    const background = state.backgroundStreams.get(runId);
    if (background) { background.controller?.abort(); clearTimeout(background.timer); state.backgroundStreams.delete(runId); }
    return generation;
  }

  function updateRunList(run) {
    const index = state.runs.findIndex((item) => item.id === run.id);
    if (index >= 0 && compareRunTime(state.runs[index].updated_at, run.updated_at) > 0) return state.runs[index];
    if (index === -1) state.runs.unshift(run); else state.runs[index] = run;
    return run;
  }

  function renderRunSelector() {
    $("metric-runs").textContent = state.runs.length;
    $("run-selector-wrap").hidden = !state.runs.length;
    $("run-selector").replaceChildren(...state.runs.map((run) => {
      const option = node("option", "", `${dateTime(run.created_at, true)} · ${run.mode === "demo" ? "演示" : "真实调研"} ${run.report ? reportResult(run.report).label : run.mode === "demo" ? (run.demo_revision === 2 ? "变化版本" : "基线版本") : ""}`);
      option.value = run.id; option.selected = run.id === state.run?.id; return option;
    }));
    const completed = state.runs.filter((item) => item.status === "completed").length;
    $("metric-runs-note").textContent = completed ? `其中 ${completed} 次已完成` : "项目的研究任务总数";
  }

  function renderProgress() {
    const run = state.run;
    $("progress-live").hidden = !isActive();
    $("run-actions").hidden = !run || run.status === "completed";
    $("resume-run").hidden = !["interrupted", "failed", "cancelled"].includes(run?.status);
    $("resume-run").textContent = run?.status === "interrupted" ? "继续研究" : "恢复研究";
    $("cancel-run").hidden = !isActive();
    $("cancel-run").disabled = state.actionBusy || state.connectionBlocked || state.disconnected;
    $("resume-run").disabled = state.actionBusy || state.connectionBlocked || state.disconnected;
    const summary = $("progress-summary"); summary.replaceChildren();
    if (!run) summary.append(node("strong", "", "等待开始研究"), node("small", "", "选择模式并启动任务后，在这里查看过程。"));
    else {
      const stageLabel = activeStatuses.has(run.status) ? stageNames[run.stage] || run.stage || statusNames[run.status] : statusNames[run.status];
      summary.append(node("strong", "", stageLabel || "研究任务"));
      const description = run.status === "interrupted" ? "任务已暂停，确认配置与执行事件后，可继续研究。" : run.status === "failed" ? "研究失败，请根据错误信息修复配置或来源，再尝试恢复。" : run.status === "completed" ? "研究结果已保存，可通过历史记录再次查看。" : run.status === "cancelled" ? "本次研究已停止，可修正配置后恢复或重新研究。" : "任务在服务端后台运行。你可以关闭页面，稍后返回查看结果。";
      summary.append(node("small", "", description));
    }
    updateElapsed(); renderStages();
    const list = $("event-list");
    const runKey = run?.id || "";
    if (list.dataset.runId !== runKey) { list.replaceChildren(); list.dataset.runId = runKey; }
    const previousCount = list.childElementCount;
    const wasNearEnd = list.scrollTop + list.clientHeight >= list.scrollHeight - 50;
    // Append only new events so screen readers do not repeat the whole history each poll.
    const additions = state.events.slice(previousCount).map((event) => {
      const item = node("li", `event${/error|fail|warning/.test(event.kind) ? " error" : ""}`);
      const time = node("time", "", timeOnly(event.created_at));
      if (event.created_at) time.dateTime = event.created_at;
      item.append(node("strong", "", event.message || event.kind), time);
      const suggestion = typeof event.data?.suggestion === "string" ? event.data.suggestion : "";
      if (suggestion) item.append(node("small", "event-error-hint", suggestion));
      else if (/error|fail/.test(event.kind)) item.append(node("small", "event-error-hint", "此事件表示执行受阻，请查看任务错误；修正后可尝试恢复。"));
      else if (/warning/.test(event.kind)) item.append(node("small", "event-error-hint", "部分来源或步骤未完成，请结合研究范围与局限判断结果。"));
      return item;
    });
    list.append(...additions);
    if (state.events.length > previousCount && wasNearEnd) list.scrollTop = list.scrollHeight;
  }

  function viewCitation(id) {
    setTab("evidence");
    resetSearch();
    $("evidence-query").value = "";
    renderEvidence();
    const item = Array.from($("evidence-content").children).find((element) => element.dataset.evidenceId === String(id));
    if (!item) { toast("该引用的原文未包含在本次报告中，可尝试搜索项目历史证据。"); return; }
    const comparison = item.querySelector("details.evidence-diff");
    if (comparison) comparison.open = true;
    item.classList.add("highlight"); item.tabIndex = -1; item.focus({ preventScroll: true });
    item.scrollIntoView({ block: "center", behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth" });
    setTimeout(() => item.classList.remove("highlight"), 2500);
  }

  function diffRows(diff) {
    return String(diff || "").split("\n").filter((line) => !line.startsWith("--- ") && !line.startsWith("+++ ") && !line.startsWith("@@") && line.trim());
  }

  function readableDiff(diff) {
    const wrapper = node("div", "readable-diff");
    const legend = node("div", "diff-legend"); legend.append(node("span", "diff-old-label", "− 历史快照移除"), node("span", "diff-new-label", "+ 本次快照新增")); wrapper.append(legend);
    const rows = node("div", "diff-rows");
    diffRows(diff).forEach((line) => {
      const kind = line.startsWith("+") ? "added" : line.startsWith("-") ? "removed" : "context";
      const row = node("div", `diff-line ${kind}`);
      row.append(node("span", "diff-symbol", kind === "added" ? "+" : kind === "removed" ? "−" : "·"), node("span", "diff-copy", line.slice(1)));
      const screenLabel = node("span", "sr-only", kind === "added" ? "本次新增：" : kind === "removed" ? "历史移除：" : "上下文：");
      row.prepend(screenLabel); rows.append(row);
    });
    wrapper.append(rows); return wrapper;
  }

  function findingDetail(finding) {
    const text = finding.detail || finding.description || "";
    if (!/^@@|^--- |^\+\+\+ /m.test(text)) return node("p", "finding-detail", text);
    const detail = node("div", "finding-detail finding-comparison");
    detail.append(node("p", "", "与历史快照相比，来源内容发生变化。"));
    [["removed", "历史", "-"], ["added", "本次", "+"]].forEach(([kind, label, prefix]) => {
      const lines = diffRows(text).filter((line) => line.startsWith(prefix)).map((line) => line.slice(1).trim());
      if (!lines.length) return;
      const row = node("div", `finding-change-row ${kind}`);
      row.append(node("span", "", label), node("p", "", lines.slice(0, 2).join(" · ") + (lines.length > 2 ? ` · 另 ${lines.length - 2} 项` : "")));
      detail.append(row);
    });
    const current = (finding.citations || []).map((citation) => evidence().find((item) => item.id === citation.evidence_id)).find((item) => item && !item.is_previous && item.diff);
    if (current) { const button = node("button", "text-button", "展开完整证据对比 ↗"); button.addEventListener("click", () => viewCitation(current.id)); detail.append(button); }
    return detail;
  }

  function renderResultSummary(container, report) {
    const result = reportResult(report);
    const stats = report.stats;
    const card = node("div", `research-result ${result.kind}`);
    const title = result.kind === "unchanged" ? "本轮来源保持稳定" : result.kind === "baseline" ? "基线已建立，为后续变化提供参照" : result.kind === "changed" ? "捕捉到值得关注的来源变化" : findings().length ? "本轮研究已完成" : "研究完成，暂无新的关键发现";
    card.append(node("strong", "", title));
    const counts = stats ? { changed: stats.changed_sources, baseline: stats.baseline_sources, unchanged: stats.unchanged_sources } : result.counts;
    card.append(node("p", "", `${counts.changed || 0} 个来源变化 · ${counts.baseline || 0} 个首次记录 · ${counts.unchanged || 0} 个与历史一致`));
    if (stats?.failed_sources) card.append(node("p", "result-caution", `${stats.failed_sources} 个来源未采集成功，结论范围以本次成功采集的来源为准。`));
    if (result.kind === "unchanged") card.append(node("small", "", "已采集的页面正文与上次成功快照一致。没有变化同样是有效的监测结果。"));
    if (report.mode === "live" && stats?.model_call_skipped) card.append(node("small", "", "页面正文与上次一致，本次未调用模型。"));
    container.append(card);
  }

  function renderReport() {
    const container = $("report-content");
    const run = state.run;
    const report = run?.report;
    $("metric-findings").textContent = report ? findings().length : "—";
    const high = findings().filter((finding) => finding.impact === "high").length;
    $("metric-findings-note").textContent = report ? (reportResult().kind === "unchanged" ? "来源保持稳定，监测结果已记录" : high ? `${high} 项高影响发现值得优先关注` : "来自本次研究的结构化分析") : "研究完成后显示结果";
    $("run-status-badge").textContent = statusNames[run?.status] || run?.status || "";
    $("run-status-badge").className = `status-badge ${run?.status || ""}`;
    $("run-error").hidden = !run?.error;
    $("run-error").textContent = typeof run?.error === "string" ? run.error : run?.error ? readableError(run.error) : "";
    $("report-mode-badge").hidden = !report;
    $("report-mode-badge").textContent = (report?.mode || run?.mode) === "demo" ? "演示报告" : "真实来源";
    if (!report) {
      if (isActive()) empty(container, "正在从信息中寻找信号", "研究过程会在右侧持续更新。完成后，关键发现、原始证据和行动建议会出现在这里。", "⌁", true);
      else if (run?.status === "interrupted") empty(container, "研究等待继续", "任务状态已保留。查看执行进度后，点击「继续研究」恢复任务。", "Ⅱ");
      else if (run?.status === "failed") empty(container, "本次研究未能完成", "查看上方错误与右侧任务事件，修复来源或服务配置后重新研究。", "!");
      else if (run?.status === "cancelled") empty(container, "研究已取消", "你可以调整模式或来源，启动新一轮研究。", "□");
      else empty(container, "第一份情报，从建立基线开始", isDemoProject(state.project) ? "研究将采集关注来源，生成带证据的发现和建议。演示项目请先选择「① 建立基线」。" : "配置真实调研后，研究将采集关注来源，建立首次快照，并生成带证据的分析与行动建议。", "▤");
      return;
    }
    container.replaceChildren();
    const kicker = node("div", "report-kicker"); kicker.append(node("span", "small-dot"), node("span", "", `${report.mode === "demo" ? "DEMO RESEARCH" : "RESEARCH BRIEF"} · ${dateTime(run.updated_at || run.created_at, true)}`));
    container.append(kicker, node("h2", "report-title", report.title || "竞品情报研究报告"), node("p", "report-summary", Array.isArray(report.summary) ? report.summary.join("\n") : report.summary || ""));
    renderResultSummary(container, report);
    const heading = node("h3", "report-section-heading", "关键发现"); heading.append(node("span", "section-count", `${findings().length} 项`)); container.append(heading);
    if (!findings().length) container.append(node("p", "no-findings-note", reportResult().kind === "unchanged" ? "本次没有新增关键发现。你可以查看各来源的本次快照，或稍后再次研究。" : "本次未形成新的关键发现，仍可查看来源证据与研究范围说明。"));
    findings().forEach((finding) => {
      const card = node("article", "finding-card");
      const top = node("div", "finding-topline");
      top.append(node("span", "category-label", categoryNames[finding.category] || finding.category || "研究发现"), node("span", `impact-label ${finding.impact || ""}`, impactNames[finding.impact] || "待评估"));
      card.append(top, node("h4", "finding-title", finding.title), findingDetail(finding));
      const citations = node("div", "citation-row");
      (finding.citations || []).forEach((citation) => {
        const source = evidence().find((item) => item.id === citation.evidence_id);
        const index = evidence().findIndex((item) => item.id === citation.evidence_id);
        const button = node("button", "citation-button", `[${index >= 0 ? index + 1 : "引"}] ${source?.source_name || "查看原始证据"} ↗`);
        button.type = "button";
        button.title = citation.quote || "查看支持这一结论的原始证据";
        button.addEventListener("click", () => viewCitation(citation.evidence_id)); citations.append(button);
      });
      card.append(citations); container.append(card);
    });
    if (report.recommendations?.length) {
      container.append(node("h3", "report-section-heading", "建议下一步"));
      const list = node("ul", "recommendation-list");
      report.recommendations.forEach((item) => list.append(node("li", "", typeof item === "string" ? item : item.detail || item.title || JSON.stringify(item)))); container.append(list);
    }
    if (report.limitations?.length) {
      const limitations = node("div", "limitations"); limitations.append(node("h3", "", "研究范围与局限"));
      report.limitations.forEach((item) => limitations.append(node("p", "", typeof item === "string" ? item : JSON.stringify(item)))); container.append(limitations);
    }
    const footer = node("div", "report-footer"); footer.append(node("span", "", `${currentEvidence().length} 份本次快照 · ${evidence().length - currentEvidence().length} 份历史引用 · ${report.mode === "demo" ? "内置虚构样本" : "公开来源研究"}`), node("span", "", "SIGNALDESK INTELLIGENCE")); container.append(footer);
  }

  function renderEvidence(items = evidence(), search = false) {
    $("evidence-count").textContent = evidence().length;
    const container = $("evidence-content"); container.replaceChildren();
    if (!items.length) { empty(container, search ? "没有找到匹配的历史证据" : "证据会与研究结果一同出现", search ? "尝试更简短的关键词，或先完成一次研究。" : "每条证据保留原始来源、内容快照和变化标记，供你核验报告结论。", "◎"); return; }
    if (search) container.append(node("p", "search-summary", `找到 ${items.length} 条相关证据`));
    items.forEach((item, index) => {
      const article = node("article", "evidence-item"); article.dataset.evidenceId = item.id || item.evidence_id;
      const top = node("div", "evidence-top");
      top.append(node("span", "evidence-number", `${String(index + 1).padStart(2, "0")}`), node("span", "evidence-source", item.source_name || domain(item.url)), node("span", `change-tag ${item.is_previous ? "previous" : item.change_type || ""}`, item.is_previous ? "历史快照" : changeNames[item.change_type] || item.change_type || "历史检索结果"));
      article.append(top, node("h3", "evidence-title", item.title || "来源证据"));
      const observedAt = item.observed_at || item.created_at;
      if (observedAt) { const observed = node("p", "evidence-observed", `${item.is_previous ? "历史观察" : search ? "记录时间" : "本次观察"} · ${dateTime(observedAt, false, true)}`); article.append(observed); }
      if (!search && !item.is_previous && item.previous_id) {
        const previous = evidence().find((record) => record.id === item.previous_id);
        if (previous?.observed_at) article.append(node("p", "evidence-previous-date", `比较参照 · ${dateTime(previous.observed_at, false, true)}`));
      }
      if (item.text || item.content || item.quote) article.append(node("p", "evidence-text", item.text || item.content || item.quote));
      if (item.diff) {
        const details = node("details", "evidence-diff"); details.append(node("summary", "", "查看新旧正文对比"), readableDiff(item.diff)); article.append(details);
      }
      article.append(externalLink(item.url, item.url ? `↗ ${item.url}` : "未提供外部链接", "evidence-link")); container.append(article);
    });
  }

  function renderHistory() {
    const container = $("history-content"); container.replaceChildren();
    if (!state.runs.length) { empty(container, "每一次研究，都会成为下一次的起点", "建立基线后，再运行一次研究，就能对比来源发生的变化。", "◷"); return; }
    state.runs.forEach((run) => {
      const item = node("article", "history-item"); const details = node("div");
      const result = run.report ? reportResult(run.report) : null;
      const heading = node("h3", "", `${run.mode === "demo" ? "演示" : "真实调研"} · ${result ? result.label : run.mode === "demo" ? (run.demo_revision === 2 ? "变化版本" : "基线版本") : "来源研究"}`);
      heading.append(node("span", `status-badge ${run.status}`, statusNames[run.status] || run.status));
      details.append(heading, node("p", "", `${dateTime(run.created_at)} · ${run.id.slice(0, 8)}`));
      const button = node("button", "button button-secondary button-small", run.id === state.run?.id ? "当前研究" : "查看研究 ↗");
      button.type = "button"; button.disabled = run.id === state.run?.id;
      button.addEventListener("click", () => { setTab("report"); selectRun(run.id); });
      item.append(details, button); container.append(item);
    });
  }

  function renderRun(preserveReport = false) {
    renderRunSelector();
    if (!preserveReport) renderReport();
    renderProgress(); renderHistory(); renderMode(); renderExport(); renderPollingError(); renderTransport(); renderExtensionAvailability();
  }

  function stopTransport() {
    stopPolling();
    if (state.streamTimer) clearTimeout(state.streamTimer);
    state.streamTimer = null;
    state.streamController?.abort(); state.streamController = null;
    for (const stream of state.backgroundStreams.values()) { stream.controller?.abort(); clearTimeout(stream.timer); }
    state.backgroundStreams.clear(); state.streamRunId = null; state.streamStatus = "idle";
  }

  async function readSSE(response, onEvent, signal) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let pending = ""; let data = []; let eventId = null; let dataSize = 0;
    async function line(value) {
      if (value.endsWith("\r")) value = value.slice(0, -1);
      if (!value) {
        if (!data.length) { eventId = null; return false; }
        const payload = data.join("\n"); const cursor = eventId;
        data = []; eventId = null; dataSize = 0;
        let event;
        try { event = JSON.parse(payload); } catch { throw new Error("AG-UI 事件内容格式无效，请重新连接。"); }
        return await onEvent(event, cursor);
      }
      if (value.startsWith(":")) return false; // SSE heartbeats carry no business event.
      const colon = value.indexOf(":");
      const field = colon < 0 ? value : value.slice(0, colon);
      let content = colon < 0 ? "" : value.slice(colon + 1);
      if (content.startsWith(" ")) content = content.slice(1);
      if (field === "data") {
        dataSize += content.length;
        if (dataSize > 2 * 1024 * 1024) throw new Error("AG-UI 单条事件超过前端接收限制。");
        data.push(content);
      }
      if (field === "id" && !content.includes("\0")) eventId = content;
      return false;
    }
    try {
      while (!signal.aborted) {
        const part = await reader.read();
        pending += decoder.decode(part.value || new Uint8Array(), { stream: !part.done });
        let end;
        while ((end = pending.indexOf("\n")) >= 0) {
          const value = pending.slice(0, end); pending = pending.slice(end + 1);
          if (await line(value)) return;
        }
        if (pending.length > 2 * 1024 * 1024) throw new Error("AG-UI 单行事件超过前端接收限制。");
        if (part.done) return;
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  }

  async function applySnapshot(snapshot, runId, epoch, selected, generation) {
    const payload = snapshot?.run;
    if (epoch !== state.epoch || generation !== runGeneration(runId) || !payload || payload.id !== runId || payload.project_id !== state.project?.id || !Object.hasOwn(statusNames, payload.status)) return;
    const cached = state.runs.find((item) => item.id === runId);
    if (cached && compareRunTime(cached.updated_at, payload.updated_at) > 0) return;
    let run = { ...cached, ...payload };
    if (snapshot.reportOmitted && !run.report && cached?.report) run.report = cached.report;
    if (snapshot.reportAvailable && snapshot.reportOmitted && !run.report && run.status === "completed") {
      const complete = await api(`/runs/${encodeURIComponent(runId)}`);
      if (epoch !== state.epoch || generation !== runGeneration(runId) || complete.id !== runId) return;
      if (compareRunTime(complete.updated_at, run.updated_at) >= 0) run = { ...run, ...complete };
    }
    if (epoch !== state.epoch || generation !== runGeneration(runId)) return;
    run = updateRunList(run);
    const reportPending = snapshot.reportAvailable && snapshot.reportOmitted && run.status === "completed" && !run.report;
    const reportPendingError = "研究已完成，报告详情尚未同步，正在重新连接获取完整报告。";
    if (!selected || state.run?.id !== runId) { renderRun(true); renderTransport(); if (reportPending) throw new Error(reportPendingError); return; }
    const old = state.run;
    const changed = old.updated_at !== run.updated_at || old.status !== run.status || old.stage !== run.stage || old.report !== run.report;
    state.run = run; state.pollingError = "";
    renderRun(!changed);
    if (changed && !$('evidence-query').value.trim()) renderEvidence();
    if (run.status === "completed" && (old.status !== "completed" || !old.report) && !reportPending) {
      toast("研究已完成，报告和原始证据已就绪。");
      if (run.mode === "demo" && run.demo_revision === 1) $("demo-revision").value = "2";
      loadMemory(run.project_id);
    }
    if (reportPending) throw new Error(reportPendingError);
  }

  function appendStreamEvent(event, cursor, runId, epoch) {
    if (epoch !== state.epoch || state.run?.id !== runId || event?.run_id !== runId || !Number.isSafeInteger(event.id) || event.id < 0) return;
    if (!state.events.some((existing) => existing.id === event.id)) {
      state.events.push(event); renderProgress();
    }
    const received = Number(cursor);
    state.eventAfter = Math.max(state.eventAfter, event.id, Number.isSafeInteger(received) ? received : 0);
  }

  async function streamResponse(runId, cursor, controller) {
    const headers = { Accept: "text/event-stream" };
    if (cursor) headers["Last-Event-ID"] = String(cursor);
    const response = await request(`/runs/${encodeURIComponent(runId)}/ag-ui?after=${cursor}`, { headers, signal: controller.signal });
    if (!response.ok) throw await responseError(response);
    if (!response.body || !response.headers.get("Content-Type")?.includes("text/event-stream")) {
      const error = new Error("服务未返回 AG-UI SSE 事件流。"); error.status = 501; throw error;
    }
    return response;
  }

  async function startSelectedStream(epoch) {
    if (epoch !== state.epoch || !state.run || state.connectionBlocked) return;
    stopPolling();
    const runId = state.run.id; const projectId = state.project.id;
    const generation = runGeneration(runId);
    const controller = new AbortController();
    state.streamController?.abort(); state.streamController = controller;
    state.streamRunId = runId; state.streamStatus = "connecting"; renderTransport();
    let terminal = false;
    try {
      const response = await streamResponse(runId, state.eventAfter, controller);
      if (epoch !== state.epoch || controller.signal.aborted || generation !== runGeneration(runId)) return;
      state.streamSupported = true; state.streamStatus = "connected"; state.pollingError = "";
      renderTransport(); renderPollingError(); syncBackgroundStreams(epoch);
      await readSSE(response, async (event, cursor) => {
        if (epoch !== state.epoch || generation !== runGeneration(runId) || state.run?.id !== runId || state.project?.id !== projectId) return true;
        if (event.type === "RUN_STARTED" && (event.runId !== runId || event.threadId !== projectId)) throw new Error("AG-UI 任务标识不匹配，已停止订阅。");
        if (event.type === "STATE_SNAPSHOT") await applySnapshot(event.snapshot, runId, epoch, true, generation);
        if (event.type === "CUSTOM" && event.name === "signaldesk.event") appendStreamEvent(event.value, cursor, runId, epoch);
        if (event.type === "RUN_FINISHED" || event.type === "RUN_ERROR") {
          if (event.type === "RUN_FINISHED" && (event.runId !== runId || event.threadId !== projectId)) throw new Error("AG-UI 结束事件标识不匹配。");
          terminal = true;
          if (isActive()) {
            const run = await api(`/runs/${encodeURIComponent(runId)}`);
            await applySnapshot({ run, reportAvailable: !!run.report }, runId, epoch, true, generation);
          }
          if (epoch === state.epoch && generation === runGeneration(runId) && state.project?.id === projectId) await loadMemory(projectId);
          return true;
        }
        return false;
      }, controller.signal);
      if (epoch !== state.epoch || controller.signal.aborted || generation !== runGeneration(runId)) return;
      if (!terminal) throw new Error("AG-UI 事件连接提前结束。");
      state.streamStatus = "ended"; state.pollingError = ""; renderTransport(); renderPollingError();
    } catch (error) {
      if (epoch !== state.epoch || controller.signal.aborted || generation !== runGeneration(runId) || error.name === "AbortError") return;
      if ([404, 405, 501].includes(error.status)) {
        state.streamSupported = false; state.streamStatus = "fallback"; state.pollingError = "";
        for (const stream of state.backgroundStreams.values()) { stream.controller.abort(); clearTimeout(stream.timer); }
        state.backgroundStreams.clear(); renderTransport(); renderPollingError(); pollRun(epoch);
      } else {
        state.streamStatus = "reconnecting";
        state.pollingError = `AG-UI 连接中断：${error.message} 将携带事件游标在 4 秒后重连，后台任务继续执行。`;
        renderTransport(); renderPollingError();
        state.streamTimer = setTimeout(() => startSelectedStream(epoch), 4000);
      }
    } finally {
      if (state.streamController === controller) state.streamController = null;
    }
  }

  function syncBackgroundStreams(epoch) {
    if (epoch !== state.epoch || state.streamSupported !== true || state.connectionBlocked) return;
    for (const run of state.runs) {
      if (run.id === state.run?.id || !activeStatuses.has(run.status) || state.backgroundStreams.has(run.id)) continue;
      const generation = runGeneration(run.id);
      const stream = { controller: null, timer: null, cursor: 0, status: "connecting" };
      state.backgroundStreams.set(run.id, stream);
      async function watch() {
        if (epoch !== state.epoch || generation !== runGeneration(run.id) || state.backgroundStreams.get(run.id) !== stream || state.connectionBlocked) return;
        const controller = new AbortController(); stream.controller = controller;
        let terminal = false;
        try {
          const response = await streamResponse(run.id, stream.cursor, controller);
          stream.status = "connected"; renderTransport();
          await readSSE(response, async (event, cursor) => {
            if (epoch !== state.epoch || generation !== runGeneration(run.id) || state.backgroundStreams.get(run.id) !== stream) return true;
            if (event.type === "STATE_SNAPSHOT") await applySnapshot(event.snapshot, run.id, epoch, false, generation);
            if (event.type === "CUSTOM" && event.name === "signaldesk.event" && event.value?.run_id === run.id) {
              const received = Number(cursor); if (Number.isSafeInteger(received)) stream.cursor = Math.max(stream.cursor, received);
            }
            if (event.type === "RUN_FINISHED" || event.type === "RUN_ERROR") { terminal = true; return true; }
            return false;
          }, controller.signal);
          if (!terminal && !controller.signal.aborted) throw new Error("后台事件流提前结束。");
          if (terminal && epoch === state.epoch && generation === runGeneration(run.id)) {
            state.backgroundStreams.delete(run.id); renderTransport(); renderMode();
            if (epoch === state.epoch && state.project?.id === run.project_id) await loadMemory(run.project_id);
          }
        } catch (error) {
          if (epoch === state.epoch && generation === runGeneration(run.id) && !controller.signal.aborted) {
            stream.status = "reconnecting";
            stream.timer = setTimeout(watch, 4000);
            renderTransport();
          }
        }
      }
      watch();
    }
    renderTransport();
  }

  function syncTransport(epoch, force = false) {
    if (epoch !== state.epoch || !state.run || state.connectionBlocked) return;
    if (state.streamSupported === false && !force) { state.streamStatus = "fallback"; renderTransport(); pollRun(epoch); return; }
    if (force) {
      state.streamSupported = null; state.streamController?.abort(); state.streamController = null;
      if (state.streamTimer) clearTimeout(state.streamTimer); state.streamTimer = null;
    }
    if (!state.streamController && (state.streamRunId !== state.run.id || state.streamStatus !== "ended" || isActive() || force)) startSelectedStream(epoch);
    syncBackgroundStreams(epoch);
  }

  function stopPolling() { if (state.timer) clearTimeout(state.timer); state.timer = null; }

  function resetSearch() {
    state.searchEpoch++;
    state.searchQuery = "";
    state.searchController?.abort(); state.searchController = null;
    $("search-status").hidden = true;
    $("search-evidence").disabled = state.connectionBlocked || state.disconnected;
    $("evidence-content").removeAttribute("aria-busy");
  }

  function schedulePoll(epoch) {
    stopPolling();
    if (state.streamSupported !== false || state.connectionBlocked) return;
    if ((!isActive() && !projectActive() && !state.pollingError) || epoch !== state.epoch) return;
    state.timer = setTimeout(() => pollRun(epoch), state.pollingError ? 4000 : 1000);
  }

  async function loadEvents(runId, epoch) {
    const result = await api(`/runs/${encodeURIComponent(runId)}/events?after=${encodeURIComponent(state.eventAfter)}`);
    if (epoch !== state.epoch || state.run?.id !== runId) return;
    const events = collection(result, "events");
    const known = new Set(state.events.map((event) => String(event.id)));
    state.events.push(...events.filter((event) => !known.has(String(event.id))));
    events.forEach((event) => { const value = Number(event.id); if (Number.isFinite(value)) state.eventAfter = Math.max(state.eventAfter, value); });
  }

  async function pollRun(epoch) {
    if (epoch !== state.epoch || !state.run || state.streamSupported !== false || state.connectionBlocked) return;
    const runId = state.run.id;
    const generation = runGeneration(runId); const generations = new Map(state.runGenerations);
    try {
      const refreshProject = state.runs.some((item) => item.id !== runId && activeStatuses.has(item.status));
      const [response, projectRuns] = await Promise.all([
        api(`/runs/${encodeURIComponent(runId)}`),
        refreshProject ? api(`/runs?project_id=${encodeURIComponent(state.project.id)}`) : Promise.resolve(null),
      ]);
      if (epoch !== state.epoch || generation !== runGeneration(runId) || state.run?.id !== runId || state.streamSupported !== false) return;
      const previousStatus = state.run.status;
      if (projectRuns) {
        collection(projectRuns, "runs").forEach((item) => { if (runGeneration(item.id) === (generations.get(item.id) || 0)) updateRunList(item); });
        state.runs.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      }
      const run = updateRunList(response);
      const selectedChanged = state.run.updated_at !== run.updated_at || previousStatus !== run.status;
      state.run = run;
      renderRun(!selectedChanged);
      if (selectedChanged && run.report && !$("evidence-query").value.trim()) renderEvidence();
      await loadEvents(runId, epoch);
      if (epoch !== state.epoch || generation !== runGeneration(runId) || state.run?.id !== runId) return;
      state.pollingError = "";
      renderRun(!selectedChanged);
      if (run.status === "completed" && previousStatus !== "completed") {
        if (!$("evidence-query").value.trim()) renderEvidence();
        toast("研究已完成，报告和原始证据已就绪。");
        if (run.mode === "demo" && run.demo_revision === 1) $("demo-revision").value = "2";
      }
      schedulePoll(epoch);
    } catch (error) {
      if (epoch !== state.epoch || generation !== runGeneration(runId)) return;
      state.pollingError = `进度暂时无法更新：${error.message} 已有任务状态和结果仍保留，4 秒后自动重试。`;
      renderPollingError(); renderExport();
      schedulePoll(epoch);
    }
  }

  async function selectRun(runId) {
    stopTransport();
    const epoch = ++state.epoch;
    const generation = runGeneration(runId);
    state.events = []; state.eventAfter = 0; state.pollingError = ""; resetSearch();
    $("evidence-query").value = "";
    state.run = state.runs.find((run) => run.id === runId) || null;
    renderRun(); renderEvidence();
    try {
      const run = await api(`/runs/${encodeURIComponent(runId)}`);
      if (epoch !== state.epoch || generation !== runGeneration(runId)) return;
      state.run = updateRunList(run);
      renderRun(); if (!$("evidence-query").value.trim()) renderEvidence();
      syncTransport(epoch);
    } catch (error) {
      if (epoch === state.epoch && generation === runGeneration(runId)) {
        state.pollingError = `读取研究详情失败：${error.message} 可点击重试恢复连接。`;
        renderPollingError();
        state.streamTimer = setTimeout(() => syncTransport(epoch), 4000);
      }
    }
  }

  async function selectProject(project) {
    stopTransport();
    stopExtensionObservers();
    const epoch = ++state.epoch;
    state.project = project; state.runs = []; state.run = null; state.events = []; state.eventAfter = 0; state.pollingError = ""; state.runsLoading = true; resetSearch();
    showError("");
    $("welcome").hidden = true; $("project-workspace").hidden = false;
    $("project-title").textContent = project.name;
    $("project-description").textContent = project.description || "持续关注竞品变化，形成有据可查的研究结论。";
    $("breadcrumb-project").textContent = project.name;
    $("research-mode").value = isDemoProject(project) ? "demo" : "live";
    $("demo-revision").value = "1"; $("evidence-query").value = "";
    setTab("report"); renderProjects(); renderSources(); renderRun(); renderEvidence();
    memoryFormEpoch++;
    $("memory-form").reset();
    loadMemory(project.id);
    loadExtensionProject(project.id);
    try { sessionStorage.setItem("signaldesk.projectId", project.id); } catch { /* session storage is optional */ }
    try {
      const result = await api(`/runs?project_id=${encodeURIComponent(project.id)}`);
      if (epoch !== state.epoch) return;
      state.runs = collection(result, "runs").sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      state.runsLoading = false;
      renderRun();
      if (state.runs.length) {
        if (state.runs.some((run) => run.mode === "demo" && run.demo_revision === 1 && run.status === "completed")) $("demo-revision").value = "2";
        await selectRun(state.runs[0].id);
      }
    } catch (error) { if (epoch === state.epoch) { state.runsLoading = true; renderMode(); showError(`${error.message} 任务状态未知，暂时禁用编辑与启动研究；请重新连接。`); } }
  }

  async function connect() {
    stopTransport(); stopExtensionObservers(); state.streamSupported = null;
    state.epoch++;
    const connectionEpoch = ++state.connectionEpoch;
    state.connectionController?.abort();
    const controller = new AbortController(); state.connectionController = controller;
    state.connectionBlocked = true; state.connectionStatus = "connecting"; state.runsLoading = true;
    resetSearch(); $("evidence-query").value = ""; renderEvidence();
    memoryEpoch++; memoryController?.abort(); memoryController = null;
    $("memory-status").textContent = "项目记忆已暂停更新，请先完成服务连接验证。";
    memoryFormEpoch++; state.projectFormEpoch++;
    extensions.capabilityEpoch++; extensions.loading = false; extensions.submissions.clear();
    showError("");
    connectionState(false); renderTransport();
    try {
      const [healthResult, projectsResult] = await Promise.allSettled([api("/health", { signal: controller.signal }), api("/projects", { signal: controller.signal })]);
      if (connectionEpoch !== state.connectionEpoch || controller.signal.aborted) return;
      state.health = healthResult.status === "fulfilled" ? healthResult.value : null;
      if (state.health?.version) $("app-version").textContent = `v${state.health.version}`;
      const access = projectsResult.status === "fulfilled" ? "API 访问验证通过" : [401, 403].includes(projectsResult.reason?.status) ? "API 访问验证失败，请检查 Token" : "API 访问尚未验证";
      $("settings-health").textContent = state.health ? `服务状态：${state.health.status || "在线"}\n访问权限：${access}\n真实调研：${state.health.live_configured ? "已配置" : "未配置模型，可先使用演示"}\n版本：${state.health.version || "未知"}` : `服务健康检查失败。\n访问权限：${access}`;
      if (projectsResult.status === "rejected") {
        state.connectionStatus = [401, 403].includes(projectsResult.reason?.status) ? "auth_error" : "error";
        connectionState(state.disconnected, projectsResult.reason.message);
        throw projectsResult.reason;
      }
      state.connectionBlocked = false; state.connectionStatus = "ready"; state.runsLoading = false;
      connectionState(false);
      loadCapabilities();
      state.projects = collection(projectsResult.value, "projects");
      renderProjects();
      if (state.projects.length) {
        let previousId;
        try { previousId = sessionStorage.getItem("signaldesk.projectId"); } catch { /* optional */ }
        await selectProject(state.projects.find((project) => project.id === previousId) || state.projects[0]);
      } else { state.project = null; $("welcome").hidden = false; $("project-workspace").hidden = true; $("breadcrumb-project").textContent = "项目总览"; }
      if (connectionEpoch === state.connectionEpoch && healthResult.status === "rejected") showError(healthResult.reason.message);
    } catch (error) {
      if (connectionEpoch !== state.connectionEpoch || controller.signal.aborted || error.name === "AbortError") return;
      if (state.connectionBlocked && state.connectionStatus === "connecting") { state.connectionStatus = "error"; connectionState(state.disconnected); }
      showError(error.message); $("welcome").hidden = !!state.project;
    } finally { if (state.connectionController === controller) state.connectionController = null; }
  }

  function openProjectDialog(edit = false) {
    // Event handlers also call this function; only explicit true selects edit mode.
    edit = edit === true;
    if (edit && (!state.project || projectActive() || state.runsLoading)) { toast("项目有等待或执行中的任务，结束后再编辑。历史报告与快照会保留。", true); return; }
    state.editProjectId = edit ? state.project.id : null;
    state.projectFormEpoch++;
    state.extraFocus = edit ? (state.project.focus || []).filter((focus) => !["pricing", "features", "positioning"].includes(focus)) : [];
    $("project-form").reset(); $("project-form-error").hidden = true;
    $("project-form-eyebrow").textContent = edit ? "EDIT PROJECT" : "NEW PROJECT";
    $("project-form-title").textContent = edit ? "编辑项目与关注来源" : "建立一个关注项目";
    $("project-edit-note").hidden = !edit;
    $("form-demo").hidden = edit;
    $("submit-project").textContent = edit ? "保存修改" : "创建项目 →";
    if (edit) {
      $("project-name").value = state.project.name;
      $("project-desc").value = state.project.description || "";
      $("project-sources").value = (state.project.sources || []).map((source) => `${source.name} | ${source.url}`).join("\n");
      document.querySelectorAll('#project-form input[name="focus"]').forEach((input) => { input.checked = (state.project.focus || []).includes(input.value); });
    }
    $("project-dialog").showModal();
    renderConnectionControls();
    $("project-name").focus();
  }

  async function createDemo() {
    if (state.actionBusy || state.connectionBlocked || state.disconnected) return;
    const connectionEpoch = state.connectionEpoch; const epoch = state.epoch;
    const existing = state.projects.find(isDemoProject);
    if (existing) { await selectProject(existing); return; }
    state.actionBusy = true; $("welcome-demo").disabled = true;
    try {
      const project = await api("/projects", { method: "POST", body: JSON.stringify({ name: "AI 产品竞品观察", description: "演示工作区 · 观察虚构产品 Northstar 与 Orbit 的套餐价格、产品能力和定位变化。", sources: [{ name: "Northstar", url: "https://northstar.example/pricing" }, { name: "Orbit", url: "https://orbit.example/changelog" }], focus: ["pricing", "features"] }) });
      if (connectionEpoch !== state.connectionEpoch) return;
      if (!state.projects.some((item) => item.id === project.id)) state.projects.unshift(project);
      renderProjects();
      if (epoch !== state.epoch || state.connectionBlocked) return;
      await selectProject(project); toast("演示项目已创建。先运行「① 建立基线」，再体验变化对比。");
    } catch (error) { if (connectionEpoch === state.connectionEpoch) showError(error.message); }
    finally { state.actionBusy = false; $("welcome-demo").disabled = false; renderMode(); }
  }

  async function createProject(event) {
    event.preventDefault();
    if (state.actionBusy || state.connectionBlocked || state.disconnected) return;
    const connectionEpoch = state.connectionEpoch; const formEpoch = state.projectFormEpoch;
    const errorBox = $("project-form-error"); errorBox.hidden = true;
    try {
      const sources = $("project-sources").value.split(/\r?\n/).filter((line) => line.trim()).map((line, index) => {
        const separator = line.indexOf("|");
        if (separator === -1) throw new Error(`第 ${index + 1} 行缺少分隔符，请使用「名称 | 网址」格式。`);
        const name = line.slice(0, separator).trim(); const url = safeUrl(line.slice(separator + 1).trim());
        if (!name || !url) throw new Error(`第 ${index + 1} 行需要有效名称和 HTTP / HTTPS 网址。`);
        return { name, url };
      });
      if (!sources.length) throw new Error("请至少添加一个关注来源。");
      const name = $("project-name").value.trim();
      if (!name) throw new Error("请填写项目名称。");
      const focus = [...Array.from(document.querySelectorAll('#project-form input[name="focus"]:checked')).map((input) => input.value), ...state.extraFocus];
      if (!focus.length) throw new Error("请至少选择一个研究重点。");
      if (state.editProjectId && projectActive()) throw new Error("项目任务正在等待或执行，当前不能编辑。请等待任务结束后保存。");
      state.actionBusy = true; $("submit-project").disabled = true;
      const editingId = state.editProjectId;
      const project = await api(editingId ? `/projects/${encodeURIComponent(editingId)}` : "/projects", { method: editingId ? "PUT" : "POST", body: JSON.stringify({ name, description: $("project-desc").value.trim(), sources, focus }) });
      if (connectionEpoch !== state.connectionEpoch) return;
      if (editingId) state.projects = state.projects.map((item) => item.id === project.id ? project : item);
      else if (!state.projects.some((item) => item.id === project.id)) state.projects.unshift(project);
      renderProjects();
      if (formEpoch !== state.projectFormEpoch || state.connectionBlocked) return;
      $("project-dialog").close();
      if (editingId) {
        if (state.project?.id === project.id) {
          state.project = project;
          $("project-title").textContent = project.name;
          $("project-description").textContent = project.description || "持续关注竞品变化，形成有据可查的研究结论。";
          $("breadcrumb-project").textContent = project.name;
          $("research-mode").value = isDemoProject(project) ? "demo" : "live";
          renderProjects(); renderSources(); renderRun();
        }
        toast("修改已保存，仅影响后续研究；历史报告与快照已保留。");
      } else {
        await selectProject(project);
        toast(state.health?.live_configured || isDemoProject(project) ? "项目已创建，可以开始建立研究基线。" : "项目已创建。配置服务端模型后可开始真实调研。");
      }
    } catch (error) { if (connectionEpoch === state.connectionEpoch && formEpoch === state.projectFormEpoch) { errorBox.textContent = error.message; errorBox.hidden = false; } }
    finally { state.actionBusy = false; renderMode(); }
  }

  async function startRun() {
    if (!state.project || state.actionBusy || projectActive() || state.connectionBlocked || state.disconnected) return;
    const project = state.project; const epoch = state.epoch; const connectionEpoch = state.connectionEpoch;
    const mode = $("research-mode").value;
    if (mode === "demo" && !isDemoProject(project)) { showError("演示模式仅支持 Northstar / Orbit 内置虚构来源。请为真实网址选择「真实调研」，或从首页体验演示项目。"); return; }
    if (mode === "live" && isDemoProject(project)) { showError("本项目使用 .example 演示地址。请先新建项目并填写真实来源，再启动真实调研。"); return; }
    state.actionBusy = true; renderMode(); showError("");
    try {
      const run = await api(`/projects/${encodeURIComponent(project.id)}/runs`, { method: "POST", body: JSON.stringify({ mode, demo_revision: Number($("demo-revision").value) }) });
      if (connectionEpoch !== state.connectionEpoch || state.project?.id !== project.id) return;
      updateRunList(run);
      if (epoch !== state.epoch) { renderRun(true); syncTransport(state.epoch); return; }
      setTab("report"); await selectRun(run.id);
    } catch (error) { if (connectionEpoch === state.connectionEpoch && state.project?.id === project.id) showError(error.message); }
    finally { state.actionBusy = false; renderMode(); }
  }

  async function runAction(action) {
    if (!state.run || state.actionBusy || state.connectionBlocked || state.disconnected) return;
    const runId = state.run.id; const projectId = state.project.id; const epoch = state.epoch; const connectionEpoch = state.connectionEpoch;
    const generation = invalidateRunObservation(runId);
    state.actionBusy = true; renderRun();
    try {
      const run = await api(`/runs/${encodeURIComponent(runId)}/${action}`, { method: "POST" });
      if (connectionEpoch !== state.connectionEpoch || generation !== runGeneration(runId) || state.project?.id !== projectId) return;
      const current = run?.id ? updateRunList(run) : null;
      if (epoch !== state.epoch || state.run?.id !== runId) { renderRun(true); syncTransport(state.epoch); return; }
      if (current) state.run = current;
      syncTransport(epoch, true);
      toast(action === "cancel" ? "已提交取消请求。" : "研究已恢复。");
    } catch (error) { if (connectionEpoch === state.connectionEpoch && epoch === state.epoch) showError(error.message); }
    finally {
      state.actionBusy = false; renderRun();
      if (connectionEpoch === state.connectionEpoch && generation === runGeneration(runId) && epoch === state.epoch && state.run?.id === runId && !state.streamController && !state.streamTimer) syncTransport(epoch);
    }
  }

  async function searchEvidence() {
    if (!state.project || state.connectionBlocked || state.disconnected) return;
    const query = $("evidence-query").value.trim();
    resetSearch();
    const searchEpoch = state.searchEpoch; const projectId = state.project.id; const connectionEpoch = state.connectionEpoch;
    if (!query) { renderEvidence(); return; }
    state.searchQuery = query;
    const controller = new AbortController(); state.searchController = controller;
    $("search-evidence").disabled = true;
    $("search-status").hidden = false;
    $("search-status").textContent = "正在搜索项目的历史证据…";
    $("evidence-content").setAttribute("aria-busy", "true");
    empty($("evidence-content"), "正在查找相关证据", "按关键词检索本项目已保存的来源快照。", "◎", true);
    try {
      const result = await api(`/projects/${encodeURIComponent(projectId)}/search?q=${encodeURIComponent(query)}`, { signal: controller.signal });
      if (controller.signal.aborted || connectionEpoch !== state.connectionEpoch || searchEpoch !== state.searchEpoch || state.project?.id !== projectId) return;
      renderEvidence(collection(result, "evidence"), true);
      $("search-status").hidden = true;
    } catch (error) { if (!controller.signal.aborted && error.name !== "AbortError" && connectionEpoch === state.connectionEpoch && searchEpoch === state.searchEpoch) { empty($("evidence-content"), "暂时无法搜索历史证据", error.message, "!"); } }
    finally {
      if (state.searchController === controller) state.searchController = null;
      if (connectionEpoch === state.connectionEpoch && searchEpoch === state.searchEpoch) { $("search-status").hidden = true; $("evidence-content").removeAttribute("aria-busy"); renderConnectionControls(); }
    }
  }

  async function exportReport(format) {
    if (!state.run?.report || state.run.status !== "completed" || state.exportBusy || state.connectionBlocked || state.disconnected) return;
    const runId = state.run.id; const connectionEpoch = state.connectionEpoch;
    state.exportBusy = true; renderExport();
    try {
      const response = await request(`/runs/${encodeURIComponent(runId)}/export?format=${encodeURIComponent(format)}`, { headers: { Accept: format === "json" ? "application/json" : "text/markdown" } });
      if (!response.ok) throw await responseError(response);
      const blob = await response.blob();
      if (connectionEpoch !== state.connectionEpoch) return;
      const url = URL.createObjectURL(blob);
      const link = node("a"); link.href = url; link.download = `signaldesk-${runId.slice(0, 8)}.${format === "markdown" ? "md" : "json"}`;
      document.body.append(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 30000);
      toast(`${format === "markdown" ? "Markdown" : "JSON"} 报告已导出。`);
    } catch (error) { if (connectionEpoch === state.connectionEpoch) toast(`报告导出失败：${error.message}`, true); }
    finally { state.exportBusy = false; renderExport(); }
  }

  function extensionStorage(projectId) {
    try { return JSON.parse(sessionStorage.getItem(`signaldesk.extensionTasks.${projectId}`) || "{}"); } catch { return {}; }
  }

  function saveExtensionStorage(projectId, values) {
    try { sessionStorage.setItem(`signaldesk.extensionTasks.${projectId}`, JSON.stringify({ ...extensionStorage(projectId), ...values })); } catch { /* Task IDs can still be followed in the current page. */ }
  }

  function isExtensionCurrent(context) { return !state.connectionBlocked && extensions.context === context && state.project?.id === context.projectId; }

  function beginExtensionSubmission(context, kind) {
    const ticket = {};
    extensions.submissions.set(`${context.projectId}:${kind}`, ticket);
    return ticket;
  }

  function currentExtensionSubmission(context, kind, ticket) {
    if (extensions.submissions.get(`${context.projectId}:${kind}`) !== ticket) return null;
    return extensions.context?.projectId === context.projectId && state.project?.id === context.projectId ? extensions.context : null;
  }

  function resumePreviousExtension(context, kind) {
    const operation = context[kind]; const record = operation.record;
    if (shouldObserveExtension(kind, record) && operation.selectedId === record.id) operation.timer = setTimeout(() => observeExtension(context, kind, record.id), 4000);
  }

  function stopExtensionWatch(operation) {
    operation.watchEpoch++; clearTimeout(operation.timer); operation.timer = null;
    operation.controller?.abort(); operation.controller = null; operation.watchId = null;
  }

  function stopExtensionObservers() {
    if (!extensions.context) return;
    for (const name of ["sandbox", "a2a"]) {
      const task = extensions.context[name]; stopExtensionWatch(task);
      task.listEpoch++; task.listController?.abort(); task.listController = null;
    }
  }

  function capabilityState(capability) {
    if (!capability || typeof capability.enabled !== "boolean" || typeof capability.available !== "boolean") return { label: extensions.loading ? "读取中" : "状态未知", kind: "unknown" };
    if (capability.enabled === false) return { label: "未启用", kind: "disabled" };
    if (capability.available === true) return { label: "可用", kind: "available" };
    return { label: "暂不可用", kind: "unavailable" };
  }

  function renderExtensionAvailability() {
    for (const name of ["sandbox", "a2a"]) {
      const capability = extensions.capabilities?.[name]; const status = capabilityState(capability);
      $(name + "-capability-badge").textContent = `${name === "sandbox" ? "Sandbox" : "A2A"} · ${status.label}`;
      $(name + "-capability-badge").className = `capability-badge ${status.kind}`;
      $(name + "-availability").textContent = status.label;
      $(name + "-availability").className = `capability-badge ${status.kind}`;
      $(name + "-reason").textContent = capability?.reason || (extensions.loading ? "正在读取服务端能力状态。" : extensions.capabilityError ? "能力状态读取失败，请刷新后再提交任务。" : status.kind === "available" ? (name === "sandbox" ? "服务端已确认隔离执行环境可用。执行会遵守下方资源限制。" : "A2A 服务已启用。请选择管理员允许的 Agent，并指定远端项目。") : "当前服务未提供此能力的配置状态。");
    }
    const sandbox = extensions.capabilities?.sandbox;
    const limits = sandbox?.limits;
    const limitLabels = [];
    if (typeof limits?.timeout_seconds === "number") limitLabels.push(`最长 ${limits.timeout_seconds} 秒`);
    if (typeof limits?.memory_mb === "number") limitLabels.push(`${limits.memory_mb} MB 内存`);
    if (typeof limits?.cpus === "number") limitLabels.push(`${limits.cpus} CPU`);
    if (typeof limits?.max_output_bytes === "number") limitLabels.push(`输出上限 ${Math.ceil(limits.max_output_bytes / 1024)} KB`);
    $("sandbox-limits").hidden = !limitLabels.length;
    $("sandbox-limits").textContent = limitLabels.join(" · ");
    $("capabilities-error").hidden = !extensions.capabilityError;
    $("capabilities-error").textContent = extensions.capabilityError;
    $("refresh-capabilities").disabled = extensions.loading;
    const remotes = Array.isArray(extensions.capabilities?.a2a?.remotes) ? extensions.capabilities.a2a.remotes.filter((remote) => typeof remote.name === "string" && remote.name) : [];
    const existing = $("a2a-remote").value;
    const previousOptions = Array.from($("a2a-remote").options).map((option) => option.value);
    if (previousOptions.join("\0") !== remotes.map((remote) => remote.name).join("\0")) {
      $("a2a-remote").replaceChildren(...(remotes.length ? remotes.map((remote) => { const option = node("option", "", remote.name); option.value = remote.name; return option; }) : [node("option", "", "暂无允许委托的 Agent")]));
      if (!remotes.length) $("a2a-remote").options[0].value = "";
      if (remotes.some((remote) => remote.name === existing)) $("a2a-remote").value = existing;
    }
    const selected = remotes.find((remote) => remote.name === $("a2a-remote").value);
    if (selected && $("a2a-project").dataset.defaultRemote !== selected.name) {
      $("a2a-project").value = selected.project_id || "";
      $("a2a-project").dataset.defaultRemote = selected.name;
    }
    if (extensions.capabilities?.a2a?.available === true && !remotes.length) $("a2a-reason").textContent += " 尚未配置允许委托的远端 Agent，暂不能提交远端研究。";
    const context = extensions.context;
    const online = !!context && !state.connectionBlocked && !state.disconnected && !extensions.loading && !extensions.capabilityError;
    $("submit-sandbox").disabled = !online || sandbox?.enabled !== true || sandbox?.available !== true || context.sandbox.busy;
    $("submit-a2a").disabled = !online || extensions.capabilities?.a2a?.enabled !== true || extensions.capabilities?.a2a?.available !== true || !selected || context.a2a.busy;
    $("a2a-remote").disabled = !remotes.length || !!context?.a2a.busy;
    $("sandbox-project-context").textContent = state.project ? `当前项目：${state.project.name}` : "先选择一个项目";
    $("sandbox-link-run").disabled = !state.run || state.run.project_id !== state.project?.id;
    for (const name of ["sandbox", "a2a"]) {
      $("cancel-" + name).disabled = !context || context[name].busy || state.connectionBlocked || state.disconnected;
      $("refresh-" + name).disabled = !context || context[name].busy || context[name].listLoading || state.disconnected;
      $(name + "-history").disabled = !context || context[name].busy;
      $("load-more-" + name).disabled = !context || context[name].busy || context[name].listLoading || state.disconnected;
    }
  }

  async function loadCapabilities() {
    const ticket = ++extensions.capabilityEpoch;
    extensions.loading = true; extensions.capabilityError = ""; renderExtensionAvailability();
    try {
      const response = await api("/capabilities");
      if (ticket !== extensions.capabilityEpoch) return;
      if (!response?.extensions || typeof response.extensions !== "object") throw new Error("当前服务尚未提供扩展能力信息，请更新服务后刷新。");
      extensions.capabilities = response.extensions;
    } catch (error) {
      if (ticket === extensions.capabilityEpoch) { extensions.capabilities = null; extensions.capabilityError = `暂时无法读取扩展能力：${error.message}`; }
    } finally { if (ticket === extensions.capabilityEpoch) { extensions.loading = false; renderExtensionAvailability(); } }
  }

  function taskLabel(status, kind) {
    const names = kind === "a2a" ? { PENDING: "登记尚未确认", TASK_STATE_SUBMITTED: "已提交", TASK_STATE_WORKING: "远端研究中", TASK_STATE_COMPLETED: "远端已完成", TASK_STATE_FAILED: "远端执行失败", TASK_STATE_CANCELED: "委托已取消", TASK_STATE_REJECTED: "远端已拒绝", TASK_STATE_INPUT_REQUIRED: "远端需要补充输入", TASK_STATE_AUTH_REQUIRED: "远端需要验证" } : { queued: "等待执行", pending: "等待执行", running: "沙箱执行中", completed: "执行已完成", failed: "执行失败", cancelled: "已取消", canceled: "已取消", cancelling: "正在取消", canceling: "正在取消", timed_out: "执行超时", timeout: "执行超时", output_limited: "输出达到上限", interrupted: "执行已中断" };
    return names[status] || status || "状态未提供";
  }

  function publicOutput(value) {
    if (typeof value === "string") return value;
    if (Array.isArray(value) && value.every((item) => typeof item === "string")) return value.join("\n");
    return null;
  }

  function boundedText(value, limit = 100000) {
    const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
    return text.length > limit ? text.slice(0, limit) + `\n\n…前端仅展示前 ${limit} 个字符。` : text;
  }

  function outputBlock(title, value, className = "") {
    const block = node("section", `execution-output ${className}`);
    block.append(node("h4", "", title));
    if (value === null) block.append(node("p", "extension-field-help", "服务端暂未提供该项输出。"));
    else if (!value) block.append(node("p", "extension-field-help", "（空）"));
    else block.append(node("pre", "", boundedText(value)));
    return block;
  }

  function extensionSubmittedAt(record, compact = true) {
    return record?.created_at && Number.isFinite(Date.parse(record.created_at)) ? dateTime(record.created_at, compact, !compact) : "提交时间未知";
  }

  function renderExtensionHistory(context, kind) {
    const operation = context[kind]; const select = $(kind + "-history");
    const options = operation.records.map((record) => {
      const label = `${extensionSubmittedAt(record)} · ${kind === "a2a" ? (record.remote || "Agent") + " · " : ""}${taskLabel(record.status, kind)} · ${String(record.id).slice(0, 8)}`;
      const option = node("option", "", label); option.value = record.id; option.selected = record.id === operation.selectedId; return option;
    });
    if (operation.selectedId && !operation.records.some((record) => record.id === operation.selectedId)) {
      const option = node("option", "", `先前选择 · ${operation.selectedId.slice(0, 8)}（详情未加载）`);
      option.value = operation.selectedId; option.selected = true; options.unshift(option);
    }
    select.hidden = !options.length; select.replaceChildren(...options);
    select.disabled = operation.busy; select.setAttribute("aria-busy", String(operation.listLoading));
    const status = operation.listLoading ? (operation.listMode === "more" ? "正在加载更多历史…" : "正在读取历史…") : !operation.historyLoaded ? (operation.listError ? "历史读取失败，可点击刷新重试。" : "等待读取历史。") : `已加载 ${operation.historyIds.length} 条历史${operation.hasMore ? " · 可继续加载" : " · 已到末尾"}${operation.selectedId && !operation.historyIds.includes(operation.selectedId) ? " · 另保留当前选择" : ""}`;
    if ($(kind + "-history-status").textContent !== status) $(kind + "-history-status").textContent = status;
    $(kind + "-history-error").textContent = operation.listError; $(kind + "-history-error").hidden = !operation.listError;
    const more = $("load-more-" + kind); more.hidden = !operation.hasMore;
    more.textContent = operation.listLoading && operation.listMode === "more" ? "加载中…" : operation.listError && operation.listMode === "more" ? "重试加载更多" : "加载更多";
  }

  function shouldObserveExtension(kind, record) {
    if (!record?.id) return false;
    return kind === "sandbox" ? sandboxActive.has(String(record.status).toLowerCase()) : !!record.task?.id && delegationActive.has(record.status);
  }

  function renderExtension(context, kind) { if (kind === "sandbox") renderSandbox(context); else renderDelegation(context); }

  function ensureExtensionObservation(context, kind) {
    const operation = context[kind]; const record = operation.record;
    if (!shouldObserveExtension(kind, record)) { if (operation.watchId) stopExtensionWatch(operation); return; }
    if (operation.watchId !== record.id) observeExtension(context, kind, record.id);
  }

  function keepExtensionRecords(operation, records) {
    const known = new Map(operation.records.map((record) => [record.id, record]));
    for (const record of records) known.set(record.id, record);
    if (operation.record) known.set(operation.record.id, operation.record);
    operation.records = operation.historyIds.map((id) => known.get(id)).filter(Boolean);
    if (operation.record && !operation.historyIds.includes(operation.record.id)) operation.records.unshift(operation.record);
  }

  function selectExtensionRecord(context, kind, record, persist = true) {
    if (!isExtensionCurrent(context)) return;
    const operation = context[kind];
    if (operation.selectedId !== record.id) stopExtensionWatch(operation);
    operation.selectionEpoch++; operation.selectedId = record.id; operation.record = record; operation.error = "";
    keepExtensionRecords(operation, [record]);
    if (persist) saveExtensionStorage(context.projectId, { [kind === "sandbox" ? "sandboxId" : "a2aId"]: record.id });
    renderExtension(context, kind); ensureExtensionObservation(context, kind);
  }

  function renderSandbox(context) {
    if (!isExtensionCurrent(context)) return;
    const task = context.sandbox; const record = task.record;
    $("sandbox-task-error").textContent = task.error;
    $("sandbox-task-error").hidden = !task.error;
    renderExtensionHistory(context, "sandbox");
    const container = $("sandbox-result"); container.replaceChildren();
    $("cancel-sandbox").hidden = !record || !sandboxActive.has(String(record.status).toLowerCase());
    $("cancel-sandbox").disabled = task.busy || state.disconnected;
    $("refresh-sandbox").disabled = task.busy;
    if (!record) { container.append(node("p", "extension-empty", task.selectedId ? "先前选择的执行详情尚未读取，可刷新恢复或选择其他记录。" : task.listLoading || !task.historyLoaded ? "正在读取沙箱执行历史。" : "尚未提交沙箱任务。只有实际执行后，这里才会出现结果。")); renderExtensionAvailability(); return; }
    const heading = node("div", "extension-result-heading");
    const statusClass = ["failed", "timed_out", "timeout"].includes(record.status) ? "failed" : ["output_limited", "interrupted"].includes(record.status) ? "interrupted" : ["cancelled", "canceled"].includes(record.status) ? "cancelled" : sandboxActive.has(String(record.status).toLowerCase()) ? "running" : record.status === "completed" ? "completed" : "unknown";
    heading.append(node("span", `status-badge ${statusClass}`, taskLabel(record.status, "sandbox")), node("span", "extension-task-id", `执行 ${String(record.id).slice(0, 8)}`)); container.append(heading);
    const result = record.result && typeof record.result === "object" ? record.result : {};
    const stdout = publicOutput(Object.hasOwn(record, "stdout") ? record.stdout : result.stdout);
    const stderr = publicOutput(Object.hasOwn(record, "stderr") ? record.stderr : result.stderr);
    const exitCode = Object.hasOwn(record, "exit_code") ? record.exit_code : result.exit_code;
    if (Number.isInteger(exitCode)) container.append(node("p", "extension-result-meta", `进程退出码：${exitCode}`));
    if (typeof result.duration_seconds === "number") container.append(node("p", "extension-result-meta", `实际执行耗时：${result.duration_seconds.toFixed(2)} 秒`));
    if (record.created_at) container.append(node("p", "extension-result-meta", `提交时间：${dateTime(record.created_at, false, true)}`));
    const error = typeof record.error === "string" ? record.error : typeof result.error === "string" ? result.error : record.error?.message || result.error?.message;
    if (typeof error === "string" && error) container.append(node("p", "extension-error", error));
    if (result.cleanup_confirmed === false && !sandboxActive.has(String(record.status).toLowerCase())) container.append(node("p", "extension-error", "隔离环境的清理尚未确认，请检查服务端任务状态。"));
    container.append(outputBlock("标准输出 · stdout", stdout), outputBlock("标准错误 · stderr", stderr, "stderr-output"));
    if (result.stdout_truncated === true || result.stderr_truncated === true) container.append(node("p", "extension-field-help", "服务端已按输出上限截断部分内容。"));
    if (Object.hasOwn(result, "result") && result.result !== null) container.append(outputBlock("结构化结果 · result", boundedText(result.result)));
    renderExtensionAvailability();
  }

  function renderDelegation(context) {
    if (!isExtensionCurrent(context)) return;
    const operation = context.a2a; const record = operation.record;
    $("a2a-task-error").textContent = operation.error;
    $("a2a-task-error").hidden = !operation.error;
    renderExtensionHistory(context, "a2a");
    $("cancel-a2a").hidden = !record?.task?.id || !delegationActive.has(record.status);
    $("cancel-a2a").disabled = operation.busy || state.disconnected;
    $("refresh-a2a").disabled = operation.busy;
    const container = $("a2a-result"); container.replaceChildren();
    if (!record) { container.append(node("p", "extension-empty", operation.selectedId ? "先前选择的委托详情尚未读取，可刷新恢复或选择其他记录。" : operation.listLoading || !operation.historyLoaded ? "正在读取本地委托历史。" : "尚未提交远端委托。这里仅展示服务端返回的实际任务状态和结果。")); renderExtensionAvailability(); return; }
    const heading = node("div", "extension-result-heading");
    const statusClass = ["TASK_STATE_FAILED", "TASK_STATE_REJECTED"].includes(record.status) ? "failed" : record.status === "TASK_STATE_CANCELED" ? "cancelled" : delegationActive.has(record.status) ? "running" : record.status === "TASK_STATE_COMPLETED" ? "completed" : ["TASK_STATE_AUTH_REQUIRED", "TASK_STATE_INPUT_REQUIRED"].includes(record.status) ? "interrupted" : "unknown";
    heading.append(node("span", `status-badge ${statusClass}`, taskLabel(record.status, "a2a")), node("span", "extension-task-id", `委托 ${String(record.id).slice(0, 8)}`)); container.append(heading);
    container.append(node("p", "extension-result-meta", record.created_at && Number.isFinite(Date.parse(record.created_at)) ? `提交时间：${extensionSubmittedAt(record, false)}` : "提交时间未知（历史记录）"));
    container.append(node("p", "extension-result-meta", `Agent：${record.remote || "未提供"} · 远端项目：${record.remote_project_id || "未提供"}`));
    const task = record.task && typeof record.task === "object" ? record.task : {};
    if (task.id) container.append(node("p", "extension-result-meta", `远端 Task：${task.id}`));
    if (record.status === "PENDING" || !task.id) {
      container.append(node("p", "extension-error", "这条委托已在本地登记，但尚未获得远端 Task ID，无法查询或取消远端任务。请使用原请求重试；如果本会话仍保留原请求 ID，相同参数提交会沿用该 ID。刷新只会重新读取本地登记，不会重复创建任务。"));
      renderExtensionAvailability(); return;
    }
    const statusMessage = task.status?.message;
    (statusMessage?.parts || []).forEach((part) => { if (typeof part.text === "string") container.append(node("p", "extension-remote-message", part.text)); });
    let outputs = 0;
    (Array.isArray(task.artifacts) ? task.artifacts : []).forEach((artifact) => {
      (Array.isArray(artifact.parts) ? artifact.parts : []).forEach((part) => {
        if (typeof part.text === "string") { container.append(outputBlock(artifact.name || "远端文本结果", part.text)); outputs++; }
        else if (Object.hasOwn(part, "data")) { container.append(outputBlock(artifact.name || "远端结构化结果", boundedText(part.data))); outputs++; }
      });
    });
    if (!outputs) container.append(node("p", "extension-field-help", delegationActive.has(record.status) ? "等待远端 Agent 返回结果。" : "远端未返回可展示的文本或数据产物。"));
    if (record.run_id) {
      const button = node("button", "text-button", "查看关联的本地研究 ↗"); button.type = "button";
      button.addEventListener("click", async () => {
        try { const run = await api(`/runs/${encodeURIComponent(record.run_id)}`); if (isExtensionCurrent(context) && run.project_id === context.projectId) { updateRunList(run); setTab("report"); await selectRun(run.id); } }
        catch (error) { if (isExtensionCurrent(context)) { operation.error = error.message; renderDelegation(context); } }
      }); container.append(button);
    }
    renderExtensionAvailability();
  }

  async function observeExtension(context, kind, id) {
    if (!isExtensionCurrent(context)) return;
    const operation = context[kind]; if (operation.selectedId !== id) return;
    const ticket = ++operation.watchEpoch;
    clearTimeout(operation.timer); operation.timer = null; operation.controller?.abort();
    if (!shouldObserveExtension(kind, operation.record)) { operation.watchId = null; return; }
    operation.watchId = id;
    const controller = new AbortController(); operation.controller = controller;
    const path = kind === "sandbox" ? `/projects/${encodeURIComponent(context.projectId)}/sandbox/executions/${encodeURIComponent(id)}` : `/a2a/delegations/${encodeURIComponent(id)}`;
    try {
      const record = await api(path, { signal: controller.signal });
      if (!isExtensionCurrent(context) || ticket !== operation.watchEpoch || operation.selectedId !== id) return;
      if (!record || record.id !== id || record.project_id !== context.projectId) throw Object.assign(new Error("服务返回的任务标识不匹配，已停止观察。"), { stopWatching: true });
      operation.record = record; operation.error = "";
      keepExtensionRecords(operation, [record]); renderExtension(context, kind);
      if (shouldObserveExtension(kind, record)) operation.timer = setTimeout(() => observeExtension(context, kind, id), 2000);
      else operation.watchId = null;
    } catch (error) {
      if (!isExtensionCurrent(context) || ticket !== operation.watchEpoch || operation.selectedId !== id || error.name === "AbortError") return;
      const retry = !error.stopWatching && ![400, 401, 403, 404, 409, 422].includes(error.status);
      operation.error = `任务状态暂时无法更新：${error.message}${retry ? " 4 秒后重试，不会自动取消任务。" : ""}`;
      renderExtension(context, kind);
      if (retry) operation.timer = setTimeout(() => observeExtension(context, kind, id), 4000);
      else operation.watchId = null;
    } finally {
      if (ticket === operation.watchEpoch) operation.controller = null;
    }
  }

  function extensionLocalPath(context, kind) {
    return `/projects/${encodeURIComponent(context.projectId)}/${kind === "sandbox" ? "sandbox/executions" : "a2a/delegations"}`;
  }

  async function loadExtensionHistory(context, kind, more = false) {
    if (!isExtensionCurrent(context)) return;
    const operation = context[kind];
    if (more && (operation.listLoading || !operation.hasMore || !operation.nextCursor)) return;
    const ticket = ++operation.listEpoch; operation.listController?.abort();
    const controller = new AbortController(); operation.listController = controller;
    const requestedCursor = more ? operation.nextCursor : null;
    const selectedAtStart = operation.record; const selectionAtStart = operation.selectionEpoch;
    operation.listLoading = true; operation.listMode = more ? "more" : "refresh"; operation.listError = "";
    renderExtension(context, kind);
    const current = () => isExtensionCurrent(context) && ticket === operation.listEpoch;
    try {
      const response = await api(`${extensionLocalPath(context, kind)}/page?limit=20${requestedCursor ? "&cursor=" + encodeURIComponent(requestedCursor) : ""}`, { signal: controller.signal });
      if (!current()) return;
      if (!Array.isArray(response?.items) || typeof response.has_more !== "boolean" || (response.has_more && (typeof response.next_cursor !== "string" || !response.next_cursor || response.next_cursor === requestedCursor))) throw new Error("服务返回的历史分页格式或游标无效，已保留当前记录。");
      const items = response.items;
      if (items.some((record) => !record || typeof record.id !== "string" || record.project_id !== context.projectId)) throw new Error("历史记录的项目标识不匹配，已拒绝载入。");
      if (operation.record === selectedAtStart && operation.selectionEpoch === selectionAtStart) {
        const updated = items.find((record) => record.id === operation.selectedId);
        if (updated) operation.record = updated;
      }
      operation.historyIds = [...new Set([...(more ? operation.historyIds : []), ...items.map((record) => record.id)])];
      operation.nextCursor = response.has_more ? response.next_cursor : null; operation.hasMore = response.has_more; operation.historyLoaded = true;
      keepExtensionRecords(operation, items); renderExtension(context, kind);
      if (operation.record) { ensureExtensionObservation(context, kind); return; }
      const selectedId = operation.selectedId;
      const loaded = operation.records.find((record) => record.id === selectedId);
      if (loaded) { selectExtensionRecord(context, kind, loaded); return; }
      if (!selectedId) { if (operation.records[0]) selectExtensionRecord(context, kind, operation.records[0]); return; }
      const selectionTicket = operation.selectionEpoch;
      try {
        const record = await api(`${extensionLocalPath(context, kind)}/${encodeURIComponent(selectedId)}`, { signal: controller.signal });
        if (!current() || operation.selectionEpoch !== selectionTicket || operation.selectedId !== selectedId) return;
        if (!record || record.id !== selectedId || record.project_id !== context.projectId) throw new Error("先前选择的任务不属于当前项目，已拒绝载入。");
        selectExtensionRecord(context, kind, record);
      } catch (error) {
        if (!current() || operation.selectionEpoch !== selectionTicket || operation.selectedId !== selectedId || error.name === "AbortError") return;
        operation.listError = `暂时无法恢复先前选择：${error.message} 已加载的历史仍可查看。`;
        if (error.status === 404) {
          operation.selectedId = null;
          saveExtensionStorage(context.projectId, { [kind === "sandbox" ? "sandboxId" : "a2aId"]: null });
          if (operation.records[0]) selectExtensionRecord(context, kind, operation.records[0]);
        }
      }
    } catch (error) {
      if (current() && error.name !== "AbortError") operation.listError = `暂时无法${more ? "加载更多" : "读取"}历史：${[404, 405].includes(error.status) ? "当前服务尚未提供历史分页接口，请更新服务后刷新。" : error.message} 已加载记录与当前任务观察保持不变。`;
    } finally {
      if (current()) { operation.listLoading = false; operation.listController = null; renderExtension(context, kind); }
    }
  }

  function loadSandboxExecutions(context, more = false) { return loadExtensionHistory(context, "sandbox", more); }
  function loadDelegations(context, more = false) { return loadExtensionHistory(context, "a2a", more); }

  function loadExtensionProject(projectId) {
    const saved = extensionStorage(projectId);
    const operation = (selectedId) => ({ record: null, selectedId: typeof selectedId === "string" ? selectedId : null, records: [], historyIds: [], busy: false, timer: null, controller: null, watchEpoch: 0, watchId: null, selectionEpoch: 0, error: "", listController: null, listEpoch: 0, listLoading: false, listMode: "refresh", listError: "", historyLoaded: false, nextCursor: null, hasMore: false });
    const context = { projectId, sandbox: operation(saved.sandboxId), a2a: operation(saved.a2aId) };
    extensions.context = context;
    $("sandbox-form").reset(); $("a2a-form").reset(); $("a2a-project").value = ""; $("a2a-project").dataset.defaultRemote = "";
    $("a2a-revision-wrap").hidden = false;
    renderSandbox(context); renderDelegation(context); renderExtensionAvailability();
    loadSandboxExecutions(context);
    loadDelegations(context);
  }

  async function submitSandbox(event) {
    event.preventDefault(); const context = extensions.context;
    if (!context || context.sandbox.busy || $("submit-sandbox").disabled) return;
    const operation = context.sandbox; operation.error = "";
    let input;
    try { input = JSON.parse($("sandbox-input").value.trim() || "null"); }
    catch { operation.error = "输入数据不是有效 JSON，请修正后再执行。"; renderSandbox(context); return; }
    const payload = { code: $("sandbox-code").value, input_data: input };
    if (!payload.code.trim()) { operation.error = "请先填写 Python 代码。"; renderSandbox(context); return; }
    if ($("sandbox-link-run").checked && state.run?.project_id === context.projectId) payload.run_id = state.run.id;
    const submission = beginExtensionSubmission(context, "sandbox");
    stopExtensionWatch(operation);
    operation.busy = true; renderSandbox(context);
    try {
      const record = await api(`/projects/${encodeURIComponent(context.projectId)}/sandbox/executions`, { method: "POST", body: JSON.stringify(payload) });
      if (!record?.id || record.project_id !== context.projectId) throw new Error("服务端未返回匹配的执行 ID，无法观察执行结果。");
      if (extensions.submissions.get(`${context.projectId}:sandbox`) !== submission) return;
      saveExtensionStorage(context.projectId, { sandboxId: record.id });
      const current = currentExtensionSubmission(context, "sandbox", submission); if (!current) return;
      selectExtensionRecord(current, "sandbox", record);
    } catch (error) {
      const current = currentExtensionSubmission(context, "sandbox", submission);
      if (current) { current.sandbox.error = `执行请求未完成：${error.message}`; renderSandbox(current); if (current === context) resumePreviousExtension(current, "sandbox"); }
    }
    finally { operation.busy = false; if (isExtensionCurrent(context)) renderSandbox(context); }
  }

  async function submitDelegation(event) {
    event.preventDefault(); const context = extensions.context;
    if (!context || context.a2a.busy || $("submit-a2a").disabled) return;
    const operation = context.a2a; operation.error = "";
    const payload = { remote: $("a2a-remote").value, remote_project_id: $("a2a-project").value.trim(), mode: $("a2a-mode").value, demo_revision: Number($("a2a-revision").value) };
    if (!payload.remote || !payload.remote_project_id) { operation.error = "请选择允许委托的 Agent，并填写远端项目 ID。"; renderDelegation(context); return; }
    const fingerprint = JSON.stringify(payload); const saved = extensionStorage(context.projectId);
    payload.message_id = saved.a2aPending?.fingerprint === fingerprint ? saved.a2aPending.messageId : crypto.randomUUID();
    saveExtensionStorage(context.projectId, { a2aPending: { fingerprint, messageId: payload.message_id } });
    const submission = beginExtensionSubmission(context, "a2a");
    stopExtensionWatch(operation);
    operation.busy = true; renderDelegation(context);
    try {
      const record = await api(`/projects/${encodeURIComponent(context.projectId)}/a2a/delegations`, { method: "POST", body: JSON.stringify(payload) });
      if (!record?.id || record.project_id !== context.projectId) throw new Error("服务端未返回匹配的委托 ID，无法观察远端任务。");
      if (extensions.submissions.get(`${context.projectId}:a2a`) !== submission) return;
      saveExtensionStorage(context.projectId, { a2aId: record.id, ...(record.task?.id && record.status !== "PENDING" ? { a2aPending: null } : {}) });
      const current = currentExtensionSubmission(context, "a2a", submission); if (!current) return;
      selectExtensionRecord(current, "a2a", record);
    } catch (error) {
      const current = currentExtensionSubmission(context, "a2a", submission);
      if (current) { current.a2a.error = `委托请求未完成：${error.message} 相同参数重试将沿用本次请求 ID。`; renderDelegation(current); if (current === context) resumePreviousExtension(current, "a2a"); }
    }
    finally { operation.busy = false; if (isExtensionCurrent(context)) renderDelegation(context); }
  }

  async function cancelExtension(kind) {
    const context = extensions.context; if (!context) return;
    const operation = context[kind]; const id = operation.record?.id;
    if (!id || operation.busy) return;
    stopExtensionWatch(operation);
    operation.busy = true; operation.error = "";
    if (kind === "sandbox") renderSandbox(context); else renderDelegation(context);
    const path = kind === "sandbox" ? `/projects/${encodeURIComponent(context.projectId)}/sandbox/executions/${encodeURIComponent(id)}/cancel` : `/a2a/delegations/${encodeURIComponent(id)}/cancel`;
    try {
      const record = await api(path, { method: "POST" });
      if (isExtensionCurrent(context) && operation.selectedId === id) {
        if (!record || record.id !== id || record.project_id !== context.projectId) throw new Error("取消响应的任务标识不匹配，请刷新确认。");
        operation.record = record; keepExtensionRecords(operation, [record]); renderExtension(context, kind); ensureExtensionObservation(context, kind);
      }
    } catch (error) {
      if (isExtensionCurrent(context) && operation.selectedId === id) {
        operation.error = `取消请求未完成：${error.message}`; toast(operation.error, true);
        if (kind === "sandbox") renderSandbox(context); else renderDelegation(context);
        if (operation.selectedId === id) operation.timer = setTimeout(() => observeExtension(context, kind, id), 2000);
      }
    }
    finally { operation.busy = false; if (isExtensionCurrent(context)) { if (kind === "sandbox") renderSandbox(context); else renderDelegation(context); } }
  }

  function memoryText(value) {
    if (typeof value?.text === "string") return value.text;
    return Object.entries(value || {}).map(([key, item]) => `${key}：${typeof item === "object" ? JSON.stringify(item) : item}`).join("\n");
  }

  async function loadMemory(projectId = state.project?.id) {
    if (!projectId || state.connectionBlocked) return;
    const ticket = ++memoryEpoch;
    const connectionEpoch = state.connectionEpoch;
    memoryController?.abort(); const controller = new AbortController(); memoryController = controller;
    $("memory-status").textContent = "正在读取项目记忆…";
    $("memory-items").replaceChildren(); $("memory-runs").replaceChildren();
    try {
      const context = await api(`/projects/${encodeURIComponent(projectId)}/memory`, { signal: controller.signal });
      if (controller.signal.aborted || connectionEpoch !== state.connectionEpoch || ticket !== memoryEpoch || state.project?.id !== projectId) return;
      const preferences = collection(context, "preferences");
      if (!preferences.length) $("memory-items").append(node("p", "memory-note", "尚未设置明确偏好。"));
      preferences.forEach((item) => {
        const article = node("article", "memory-item");
        const heading = node("div", "memory-item-heading");
        const edit = node("button", "text-button", "编辑"); edit.type = "button";
        edit.addEventListener("click", () => {
          memoryFormEpoch++;
          $("memory-key").value = item.key; $("memory-value").value = memoryText(item.value);
          $("memory-value").focus();
        });
        const remove = node("button", "text-button", "移除"); remove.type = "button";
        remove.dataset.writeAction = "memory-delete";
        remove.disabled = memoryBusy || state.connectionBlocked || state.disconnected;
        remove.setAttribute("aria-label", `移除偏好${item.key}`);
        remove.addEventListener("click", async () => {
          if (memoryBusy || state.connectionBlocked || state.disconnected || state.project?.id !== projectId) return;
          const actionConnectionEpoch = state.connectionEpoch; const actionFormEpoch = memoryFormEpoch;
          memoryBusy = true; remove.disabled = true; $("save-memory").disabled = true;
          try {
            await api(`/projects/${encodeURIComponent(projectId)}/memory/${encodeURIComponent(item.key)}`, { method: "DELETE" });
            if (actionConnectionEpoch === state.connectionEpoch && state.project?.id === projectId) {
              if (actionFormEpoch === memoryFormEpoch && $("memory-key").value === item.key) { memoryFormEpoch++; $("memory-form").reset(); }
              await loadMemory(projectId);
              if (actionConnectionEpoch === state.connectionEpoch && state.project?.id === projectId) toast("该明确偏好已移除。" );
            }
          } catch (error) {
            if (actionConnectionEpoch === state.connectionEpoch && state.project?.id === projectId) $("memory-status").textContent = `偏好未移除：${error.message}`;
          } finally { memoryBusy = false; renderConnectionControls(); }
        });
        const actions = node("div", "memory-item-actions"); actions.append(edit, remove);
        heading.append(node("strong", "", item.key), actions);
        article.append(heading, node("p", "memory-value", memoryText(item.value))); $("memory-items").append(article);
      });
      const runs = collection(context, "recent_runs");
      if (!runs.length) $("memory-runs").append(node("p", "memory-note", "完成研究后，摘要会保存在当前项目。"));
      runs.slice(0, 3).forEach((item) => {
        const content = item.value || {};
        const article = node("article", "memory-item");
        article.append(node("strong", "", content.title || `研究 ${item.key.slice(0, 8)}`), node("p", "memory-value", content.summary || memoryText(content)));
        $("memory-runs").append(article);
      });
      $("memory-status").textContent = "偏好仅在你保存时写入；网页内容不会自动成为偏好。";
    } catch (error) {
      if (!controller.signal.aborted && error.name !== "AbortError" && connectionEpoch === state.connectionEpoch && ticket === memoryEpoch && state.project?.id === projectId) $("memory-status").textContent = `暂时无法读取项目记忆：${error.message}`;
    } finally { if (memoryController === controller) memoryController = null; }
  }

  async function saveMemory(event) {
    event.preventDefault();
    if (!state.project || memoryBusy || state.connectionBlocked || state.disconnected) return;
    const projectId = state.project.id; const connectionEpoch = state.connectionEpoch; const formEpoch = memoryFormEpoch;
    const key = $("memory-key").value.trim(); const text = $("memory-value").value.trim();
    if (!key || !text) return;
    memoryBusy = true; $("save-memory").disabled = true;
    $("memory-status").textContent = "正在保存你明确设置的偏好…";
    try {
      await api(`/projects/${encodeURIComponent(projectId)}/memory/${encodeURIComponent(key)}`, {
        method: "PUT", body: JSON.stringify({ value: { text } }),
      });
      if (connectionEpoch !== state.connectionEpoch || state.project?.id !== projectId) return;
      if (formEpoch === memoryFormEpoch) { memoryFormEpoch++; $("memory-form").reset(); }
      await loadMemory(projectId);
      if (connectionEpoch === state.connectionEpoch && state.project?.id === projectId) toast("明确偏好已保存，下次真实研究会读取。" );
    } catch (error) {
      if (connectionEpoch === state.connectionEpoch && state.project?.id === projectId) $("memory-status").textContent = `偏好未保存：${error.message}`;
    } finally { memoryBusy = false; renderConnectionControls(); }
  }

  ["add-project", "add-project-small", "welcome-new", "topbar-add"].forEach((id) => $(id).addEventListener("click", openProjectDialog));
  $("edit-project").addEventListener("click", () => openProjectDialog(true));
  $("export-markdown").addEventListener("click", () => exportReport("markdown"));
  $("export-json").addEventListener("click", () => exportReport("json"));
  $("retry-connection").addEventListener("click", connect);
  $("retry-poll").addEventListener("click", () => { state.pollingError = ""; renderPollingError(); syncTransport(state.epoch, true); });
  $("retry-stream").addEventListener("click", () => syncTransport(state.epoch, true));
  $("refresh-memory").addEventListener("click", () => loadMemory());
  $("memory-form").addEventListener("submit", saveMemory);
  $("refresh-capabilities").addEventListener("click", loadCapabilities);
  $("sandbox-form").addEventListener("submit", submitSandbox);
  $("a2a-form").addEventListener("submit", submitDelegation);
  $("cancel-sandbox").addEventListener("click", () => cancelExtension("sandbox"));
  $("cancel-a2a").addEventListener("click", () => cancelExtension("a2a"));
  $("refresh-sandbox").addEventListener("click", () => { if (extensions.context) loadSandboxExecutions(extensions.context); });
  $("refresh-a2a").addEventListener("click", () => { if (extensions.context) loadDelegations(extensions.context); });
  $("load-more-sandbox").addEventListener("click", () => { if (extensions.context) loadSandboxExecutions(extensions.context, true); });
  $("load-more-a2a").addEventListener("click", () => { if (extensions.context) loadDelegations(extensions.context, true); });
  $("sandbox-history").addEventListener("change", (event) => {
    const context = extensions.context; if (!context) return;
    if (context.sandbox.busy) return;
    const selected = context.sandbox.records.find((record) => record.id === event.target.value);
    if (selected) selectExtensionRecord(context, "sandbox", selected);
  });
  $("a2a-history").addEventListener("change", (event) => {
    const context = extensions.context; if (!context) return;
    if (context.a2a.busy) return;
    const selected = context.a2a.records.find((record) => record.id === event.target.value);
    if (selected) selectExtensionRecord(context, "a2a", selected);
  });
  $("a2a-remote").addEventListener("change", () => {
    const selected = extensions.capabilities?.a2a?.remotes?.find((remote) => remote.name === $("a2a-remote").value);
    $("a2a-project").value = selected?.project_id || ""; $("a2a-project").dataset.defaultRemote = selected?.name || ""; renderExtensionAvailability();
  });
  $("a2a-mode").addEventListener("change", () => { $("a2a-revision-wrap").hidden = $("a2a-mode").value !== "demo"; });
  $("form-demo").addEventListener("click", () => { state.projectFormEpoch++; $("project-dialog").close(); createDemo(); });
  $("welcome-demo").addEventListener("click", createDemo);
  $("project-form").addEventListener("submit", createProject);
  $("project-form").addEventListener("input", () => { state.projectFormEpoch++; });
  $("project-dialog").addEventListener("cancel", () => { state.projectFormEpoch++; });
  $("memory-form").addEventListener("input", () => { memoryFormEpoch++; });
  $("start-run").addEventListener("click", startRun);
  $("research-mode").addEventListener("change", () => { showError(""); renderMode(); });
  $("run-selector").addEventListener("change", (event) => selectRun(event.target.value));
  $("resume-run").addEventListener("click", () => runAction("resume"));
  $("cancel-run").addEventListener("click", () => runAction("cancel"));
  $("view-sources").addEventListener("click", () => { $("sources-panel").scrollIntoView({ behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth", block: "center" }); $("sources-heading").tabIndex = -1; $("sources-heading").focus({ preventScroll: true }); });
  ["report", "evidence", "history"].forEach((tab) => {
    $("tab-" + tab).addEventListener("click", () => setTab(tab));
    $("tab-" + tab).addEventListener("keydown", (event) => {
      const tabs = ["report", "evidence", "history"]; const index = tabs.indexOf(tab);
      const next = event.key === "ArrowRight" ? tabs[(index + 1) % 3] : event.key === "ArrowLeft" ? tabs[(index + 2) % 3] : event.key === "Home" ? tabs[0] : event.key === "End" ? tabs[2] : null;
      if (next) { event.preventDefault(); setTab(next, true); }
    });
  });
  $("search-evidence").addEventListener("click", searchEvidence);
  $("evidence-query").addEventListener("keydown", (event) => { if (event.key === "Enter") { event.preventDefault(); searchEvidence(); } });
  $("evidence-query").addEventListener("input", () => {
    const query = $("evidence-query").value.trim();
    if (query && query === state.searchQuery) return;
    resetSearch();
    if (!query) { renderEvidence(); return; }
    $("search-status").hidden = false; $("search-status").textContent = "关键词已修改，点击「搜索」或按 Enter 查看匹配证据。";
    empty($("evidence-content"), "等待搜索新关键词", "点击「搜索」或按 Enter，不会继续显示上一个关键词的结果。", "◎");
  });
  $("open-settings").addEventListener("click", () => { $("api-token").value = token(); $("settings-dialog").showModal(); });
  $("settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try { const value = $("api-token").value.trim(); if (value) sessionStorage.setItem("signaldesk.apiToken", value); else sessionStorage.removeItem("signaldesk.apiToken"); }
    catch { toast("当前浏览器禁用了会话存储，无法保存 Token。", true); return; }
    $("settings-dialog").close(); await connect();
  });
  document.querySelectorAll(".close-dialog").forEach((button) => button.addEventListener("click", () => { const dialog = button.closest("dialog"); if (dialog.id === "project-dialog") state.projectFormEpoch++; dialog.close(); }));
  window.addEventListener("pagehide", stopTransport);
  window.addEventListener("pagehide", stopExtensionObservers);
  window.addEventListener("pageshow", (event) => { if (event.persisted && state.run) syncTransport(state.epoch); });
  window.addEventListener("pageshow", (event) => { if (event.persisted && state.project) loadExtensionProject(state.project.id); });
  window.addEventListener("offline", () => connectionState(true, "浏览器当前离线。已有研究结果仍保留，恢复网络后可重新连接。"));
  window.addEventListener("online", () => { if (state.run && !state.connectionBlocked) syncTransport(state.epoch, true); else connect(); });
  setInterval(() => { if (isActive()) updateElapsed(); }, 1000);
  connect();
})();
