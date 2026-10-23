// Run with: node --test tests/frontend_connection.mjs
// Executes the complete production script and its actual form/button handlers.
// Only browser primitives, HTTP responses, storage, and the clock are fixtures.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../src/signaldesk/static/app.js", import.meta.url), "utf8");
const startup = "  connect();\n})();";
assert.equal(source.split(startup).length, 2, "Production startup marker is unique");
const instrumented = source.replace(startup, "  globalThis.production = {state, extensions, connect, selectProject, pollRun, loadMemory};\n})();");
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};
const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const project = { id: "p", name: "Fixture project", sources: [{ name: "Fixture", url: "https://fixture.example/pricing" }], focus: ["pricing"], description: "" };
const run = (status) => ({ id: "r", project_id: "p", mode: "live", status, stage: status === "completed" ? "completed" : "collect", created_at: "2026-10-23T00:00:00Z", updated_at: status === "completed" ? "2026-10-23T00:01:00Z" : "2026-10-23T00:00:01Z", report: status === "completed" ? { title: "Fixture report", summary: "Saved report", findings: [], recommendations: [], limitations: [], mode: "live", evidence: [{ id: "current", source_name: "Fixture", url: "https://fixture.example/pricing", title: "Current report evidence", text: "CURRENT_REPORT_BODY", change_type: "baseline" }] } : null });

class Element {
  constructor(tag = "div", id = "") {
    this.tag = tag; this.id = id; this.children = []; this.dataset = {};
    this.listeners = new Map(); this.attributes = {}; this.hidden = false; this.disabled = false;
    this._text = ""; this.className = ""; this.open = false; this.checked = false;
    this.scrollTop = 0; this.scrollHeight = 0; this.clientHeight = 0;
    this.classList = { add() {}, remove() {}, toggle() {} };
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map((child) => child.textContent || "").join(""); }
  set innerHTML(_) { throw new Error("Dynamic HTML must not be injected"); }
  get childElementCount() { return this.children.length; }
  get options() { return this.children; }
  set value(value) { this._value = String(value); }
  get value() { return this._value ?? (this.tag === "select" ? (this.children.find((child) => child.selected) || this.children[0])?.value : "") ?? ""; }
  append(...items) { this.children.push(...items); }
  prepend(...items) { this.children.unshift(...items); }
  replaceChildren(...items) { this.children = items; this._text = ""; if (this.tag === "select") this._value = undefined; }
  setAttribute(key, value) { this.attributes[key] = String(value); }
  removeAttribute(key) { delete this.attributes[key]; }
  addEventListener(name, callback) { if (!this.listeners.has(name)) this.listeners.set(name, []); this.listeners.get(name).push(callback); }
  async emit(name, values = {}) { for (const handler of this.listeners.get(name) || []) await handler({ target: this, preventDefault() {}, ...values }); }
  showModal() { this.open = true; }
  close() { this.open = false; }
  closest() { return this.dialog; }
  focus() {}
  scrollIntoView() {}
  remove() {}
  reset() { this.onReset?.(); }
}

function harness(route) {
  const elements = new Map(); const all = []; const calls = []; const storage = new Map(); const timers = new Map(); let timerId = 0;
  const $ = (id) => {
    if (!elements.has(id)) { const item = new Element(["research-mode", "demo-revision", "run-selector", "a2a-remote", "sandbox-history", "a2a-history"].includes(id) ? "select" : "div", id); elements.set(id, item); all.push(item); }
    return elements.get(id);
  };
  const focus = ["pricing", "features", "positioning"].map((value) => { const item = new Element("input"); item.value = value; item.checked = value === "pricing"; return item; });
  const projectClose = new Element("button"); projectClose.dialog = $("project-dialog");
  const settingsClose = new Element("button"); settingsClose.dialog = $("settings-dialog");
  $("project-form").onReset = () => { for (const id of ["project-name", "project-desc", "project-sources"]) $(id).value = ""; focus.forEach((item) => { item.checked = item.value === "pricing"; }); };
  $("memory-form").onReset = () => { $("memory-key").value = ""; $("memory-value").value = ""; };
  $("research-mode").value = "demo"; $("demo-revision").value = "1";
  const document = {
    getElementById: $, body: new Element("body"),
    createElement(tag) { const item = new Element(tag); all.push(item); return item; },
    querySelectorAll(selector) {
      if (selector === ".close-dialog") return [projectClose, settingsClose];
      if (selector.startsWith("#project-form input")) return selector.includes(":checked") ? focus.filter((item) => item.checked) : focus;
      if (selector === '[data-write-action="memory-delete"]') return all.filter((item) => item.dataset.writeAction === "memory-delete");
      throw new Error(`Unexpected DOM selector: ${selector}`);
    },
  };
  const scope = vm.createContext({
    document, window: { addEventListener() {}, matchMedia: () => ({ matches: false }) },
    console, Headers, Response, URL, AbortController, TextDecoder, Intl, Date,
    sessionStorage: { getItem: (key) => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, String(value)), removeItem: (key) => storage.delete(key) },
    setInterval() {}, setTimeout(callback) { const id = ++timerId; timers.set(id, callback); return id; }, clearTimeout(id) { timers.delete(id); },
    fetch: async (url, options) => { const call = { url, ...options }; calls.push(call); return route(call); },
  });
  vm.runInContext(instrumented, scope, { filename: "production-app.js" });
  const production = scope.production;
  Object.assign(production.state, { connectionBlocked: false, connectionStatus: "ready", health: { status: "ok", live_configured: true, version: "0.4.2" }, project, projects: [project] });
  return { $, calls, storage, timers, production, projectClose,
    async settings(value) { $("api-token").value = value; await $("settings-form").emit("submit"); },
    async openProject(name) { await $("add-project").emit("click"); $("project-name").value = name; $("project-sources").value = "Fixture | https://fixture.example/pricing"; await $("project-form").emit("input"); },
  };
}

test("wrong Token never appears connected; connection changes abort old search/memory and block writes", async () => {
  const search = deferred(); const memory = deferred();
  const ui = harness(({ url }) => {
    if (url.includes("/search?")) return search.promise;
    if (url.endsWith("/memory")) return memory.promise;
    if (url === "/api/health") return json({ status: "ok", version: "0.4.2" });
    if (url === "/api/projects") return json({ detail: "Incorrect token" }, 401);
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  ui.$("evidence-query").value = "alpha";
  const pendingSearch = ui.$("search-evidence").emit("click");
  const pendingMemory = ui.production.loadMemory();
  await ui.settings("incorrect-token");
  const { state } = ui.production;
  assert.equal(state.connectionStatus, "auth_error"); assert.equal(state.connectionBlocked, true);
  assert.match(ui.$("health-label").textContent, /访问验证失败/);
  assert.match(ui.$("settings-health").textContent, /API 访问验证失败/);
  for (const id of ["start-run", "edit-project", "submit-project", "save-memory", "cancel-run", "resume-run", "submit-sandbox", "submit-a2a"]) assert.equal(ui.$(id).disabled, true, id);
  for (const call of ui.calls.filter(({ url }) => /search|memory/.test(url))) assert.equal(call.signal.aborted, true);
  // Deliberately ignore the abort at the HTTP fixture: even a late response must not paint.
  search.resolve(json([{ evidence_id: "old", title: "OLD_SEARCH_RESULT", text: "Old response" }]));
  memory.resolve(json({ preferences: [{ key: "OLD_MEMORY_RESULT", value: { text: "Old response" } }], recent_runs: [] }));
  await Promise.all([pendingSearch, pendingMemory]);
  assert.doesNotMatch(ui.$("evidence-content").textContent, /OLD_SEARCH_RESULT/);
  assert.doesNotMatch(ui.$("memory-items").textContent, /OLD_MEMORY_RESULT/);
  const before = ui.calls.length;
  await ui.$("start-run").emit("click"); await ui.$("project-form").emit("submit"); await ui.$("memory-form").emit("submit");
  assert.equal(ui.calls.length, before, "write handlers do not submit while access is unverified");
});

test("a newer connection survives an older unauthorized result or network failure", async () => {
  for (const oldFailure of ["unauthorized", "network"]) {
    const health = deferred(); const projects = deferred();
    const ui = harness(({ url, headers }) => {
      if (headers.get("Authorization") === "Bearer old") return url === "/api/health" ? health.promise : projects.promise;
      if (url === "/api/health") return json({ status: "ok", version: "new-version" });
      if (url === "/api/projects") return json([]);
      if (url === "/api/capabilities") return json({ extensions: {} });
      throw new Error(`Unexpected HTTP request: ${url}`);
    });
    const old = ui.settings("old"); await ui.settings("new");
    assert.equal(ui.production.state.connectionStatus, "ready");
    health.resolve(json({ status: "ok", version: "old-version" }));
    if (oldFailure === "unauthorized") projects.resolve(json({ detail: "Old invalid token" }, 401));
    else projects.reject(new TypeError("Old network failure"));
    await old;
    assert.equal(ui.production.state.health.version, "new-version");
    assert.equal(ui.production.state.connectionBlocked, false); assert.equal(ui.production.state.disconnected, false);
    assert.equal(ui.$("health-label").textContent, "服务已连接"); assert.equal(ui.$("global-error").textContent, "");
    assert.equal(ui.calls[0].signal.aborted, true);
  }
});

test("successful access verification restores writes and subscribes to the selected run", async () => {
  const completed = run("completed");
  const ui = harness(({ url, headers }) => {
    const authorized = headers.get("Authorization") === "Bearer corrected-token";
    if (url === "/api/health") return json({ status: "ok", live_configured: true });
    if (url === "/api/projects") return authorized ? json([project]) : json({ detail: "Incorrect token" }, 401);
    if (url === "/api/capabilities") return json({ extensions: {} });
    if (url.includes("/executions/page") || url.includes("/delegations/page")) return json({ items: [], next_cursor: null, has_more: false });
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    if (url === "/api/projects/p/runs/page?limit=20") return json({ items: [run("running")], next_cursor: null, has_more: false, active_run: run("running") });
    if (url === "/api/runs/r") return json(run("running"));
    if (url.startsWith("/api/runs/r/ag-ui?")) {
      const events = [{ type: "RUN_STARTED", runId: "r", threadId: "p" }, { type: "STATE_SNAPSHOT", snapshot: { run: completed } }, { type: "RUN_FINISHED", runId: "r", threadId: "p" }];
      return new Response(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""), { headers: { "Content-Type": "text/event-stream" } });
    }
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  Object.assign(ui.production.state, { projects: [], project: null });
  await ui.settings("incorrect-token"); assert.equal(ui.production.state.connectionBlocked, true);
  assert.match(ui.$("project-list").textContent, /访问未验证.*Token/);
  assert.doesNotMatch(ui.$("project-list").textContent, /正在连接|正在验证/);
  await ui.settings("corrected-token");
  for (let attempt = 0; attempt < 10 && ui.production.state.streamStatus !== "ended"; attempt++) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(ui.production.state.connectionStatus, "ready"); assert.equal(ui.production.state.connectionBlocked, false);
  assert.equal(ui.$("health-label").textContent, "服务已连接"); assert.equal(ui.$("submit-project").disabled, false);
  assert.match(ui.$("project-list").textContent, /Fixture project/);
  assert.doesNotMatch(ui.$("project-list").textContent, /访问未验证|正在验证/);
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.production.state.streamStatus, "ended");
  assert.equal(ui.calls.filter(({ url }) => url.startsWith("/api/runs/r/ag-ui?")).length, 1);
});

test("an old successful project submission updates history without closing or selecting over a new draft", async () => {
  const response = deferred(); const ui = harness(() => response.promise);
  await ui.openProject("First project");
  const submission = ui.$("project-form").emit("submit");
  await ui.projectClose.emit("click"); await ui.openProject("New unsaved draft");
  response.resolve(json({ ...project, id: "created-first", name: "First project" })); await submission;
  assert.equal(ui.$("project-dialog").open, true); assert.equal(ui.$("project-name").value, "New unsaved draft");
  assert.equal(ui.production.state.project.id, "p");
  assert.equal(ui.production.state.projects.filter((item) => item.id === "created-first").length, 1);
  assert.equal(ui.$("submit-project").disabled, false);
});

test("an old project error cannot mark a reopened form, and Escape also invalidates its submission", async () => {
  const response = deferred(); const ui = harness(() => response.promise);
  await ui.openProject("First project"); const submission = ui.$("project-form").emit("submit");
  await ui.$("project-dialog").emit("cancel"); ui.$("project-dialog").close(); await ui.openProject("New draft");
  response.resolve(json({ detail: "OLD_FORM_ERROR" }, 422)); await submission;
  assert.equal(ui.$("project-dialog").open, true); assert.equal(ui.$("project-form-error").hidden, true);
  assert.doesNotMatch(ui.$("project-form-error").textContent, /OLD_FORM_ERROR/);
});

test("changing connection while a project POST is in flight preserves its new draft and does not abort the write", async () => {
  const response = deferred();
  const ui = harness(({ url, method }) => {
    if (url === "/api/projects" && method === "POST") return response.promise;
    if (url === "/api/projects") return json([]);
    if (url === "/api/health") return json({ status: "ok" });
    if (url === "/api/capabilities") return json({ extensions: {} });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  await ui.openProject("First project"); const submission = ui.$("project-form").emit("submit");
  await ui.settings("new-token"); ui.$("project-name").value = "Draft for new connection";
  await ui.$("project-form").emit("input");
  response.resolve(json({ ...project, id: "old-connection-created" })); await submission;
  assert.equal(ui.$("project-dialog").open, true); assert.equal(ui.$("project-name").value, "Draft for new connection");
  assert.equal(ui.production.state.project, null); assert.equal(ui.production.state.projects.length, 0);
  const write = ui.calls.find(({ method }) => method === "POST");
  assert.equal(write.signal, undefined, "already submitted work is not cancelled by connection replacement");
});

test("memory save/delete do not toast into a later connection or project after their refresh settles", async () => {
  for (const action of ["save", "delete"]) for (const change of ["connection", "project"]) {
    const refresh = deferred(); let memoryReads = 0;
    const ui = harness(({ url, method }) => {
      if (method === "PUT" || method === "DELETE") return json({});
      if (url === "/api/projects/p/memory") {
        if (action === "delete" && ++memoryReads === 1) return json({ preferences: [{ key: "preference", value: { text: "Old value" } }], recent_runs: [] });
        return refresh.promise;
      }
      if (url === "/api/health") return json({ status: "ok" });
      if (url === "/api/projects") return json([]);
      if (url === "/api/capabilities") return json({ extensions: {} });
      if (url === "/api/projects/q/memory") return json({ preferences: [], recent_runs: [] });
      if (url === "/api/projects/q/runs/page?limit=20") return json({ items: [], next_cursor: null, has_more: false, active_run: null });
      if (url.includes("/executions/page") || url.includes("/delegations/page")) return json({ items: [], next_cursor: null, has_more: false });
      throw new Error(`Unexpected HTTP request: ${url}`);
    });
    let submission;
    if (action === "save") {
      ui.$("memory-key").value = "preference"; ui.$("memory-value").value = "Saved value";
      submission = ui.$("memory-form").emit("submit");
    } else {
      await ui.production.loadMemory();
      const descendants = (element) => [element, ...element.children.flatMap(descendants)];
      const remove = descendants(ui.$("memory-items")).find((element) => element.dataset.writeAction === "memory-delete");
      assert.ok(remove, "Production preference delete button was rendered"); submission = remove.emit("click");
    }
    const expectedReads = action === "delete" ? 2 : 1;
    for (let attempt = 0; attempt < 10 && ui.calls.filter(({ url, method }) => url === "/api/projects/p/memory" && !method).length < expectedReads; attempt++) await new Promise((resolve) => setImmediate(resolve));
    const oldRefresh = ui.calls.filter(({ url, method }) => url === "/api/projects/p/memory" && !method).at(-1);
    assert.equal(ui.$("save-memory").disabled, true);
    if (change === "connection") await ui.settings("new-token");
    else await ui.production.selectProject({ ...project, id: "q", name: "New project" });
    assert.equal(oldRefresh.signal.aborted, true); assert.equal(ui.$("save-memory").disabled, true, "Old mutation still owns its busy state until it settles");
    refresh.resolve(json({ preferences: [], recent_runs: [] })); await submission;
    assert.equal(ui.$("toast").textContent, "", `${action}/${change}: old success must not toast into the new context`);
    assert.equal(ui.$("save-memory").disabled, false, "Settled mutation releases busy state using current connection permissions");
  }
});

test("JSON fallback completion preserves successful evidence search and its query", async () => {
  const ui = harness(({ url }) => {
    if (url.includes("/search?")) return json([{ evidence_id: "hit", source_name: "History", url: "https://fixture.example/history", title: "ALPHA_SEARCH_RESULT", text: "Matched historical evidence" }]);
    if (url === "/api/runs/r") return json(run("completed"));
    if (url.includes("/runs/r/events?")) return json([]);
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  const active = run("running"); Object.assign(ui.production.state, { run: active, runs: [active], streamSupported: false });
  ui.$("evidence-query").value = "alpha"; await ui.$("search-evidence").emit("click");
  assert.match(ui.$("evidence-content").textContent, /ALPHA_SEARCH_RESULT/);
  await ui.production.pollRun(ui.production.state.epoch);
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.$("evidence-query").value, "alpha");
  assert.match(ui.$("evidence-content").textContent, /ALPHA_SEARCH_RESULT/);
  assert.doesNotMatch(ui.$("evidence-content").textContent, /CURRENT_REPORT_BODY/);
});
