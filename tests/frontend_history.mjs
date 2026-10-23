// Run with: node --test tests/frontend_history.mjs
// Executes the production extension controllers and their real event handlers.
// Only the DOM, storage, HTTP boundary, and clock are controlled fixtures.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../src/signaldesk/static/app.js", import.meta.url), "utf8");
const section = (start, end) => {
  const first = source.indexOf(start);
  const last = source.indexOf(end, first);
  assert.ok(first >= 0 && last > first, `Production section exists: ${start}`);
  return source.slice(first, last);
};
const controllers = section("  function extensionStorage(", "  function memoryText(");
const events = section('  $("refresh-sandbox").addEventListener', '  $("a2a-remote").addEventListener');
const nodeFactory = section("  function node(", "  function token(");

class Element {
  constructor(tag = "div") {
    this.tag = tag;
    this.children = [];
    this.dataset = {};
    this.listeners = new Map();
    this.hidden = false;
    this.disabled = false;
    this._text = "";
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map((child) => child.textContent || "").join(""); }
  set innerHTML(_) { throw new Error("Dynamic HTML must not be injected"); }
  get options() { return this.children; }
  set value(value) {
    this._value = String(value);
    if (this.tag === "select") this.children.forEach((option) => { option.selected = option.value === this._value; });
  }
  get value() {
    return this._value ?? (this.tag === "select" ? (this.children.find((option) => option.selected) || this.children[0])?.value : "") ?? "";
  }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; this._text = ""; if (this.tag === "select") this._value = undefined; }
  setAttribute(name, value) { this[name] = value; }
  addEventListener(name, callback) { this.listeners.set(name, callback); }
  emit(name) { return this.listeners.get(name)?.({ target: this, preventDefault() {} }); }
  reset() {}
}

const deferred = () => {
  let resolve, reject;
  const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};
const flush = () => new Promise((resolve) => setImmediate(resolve));
const page = (items, cursor = null) => ({ items, next_cursor: cursor, has_more: cursor !== null });
const sandbox = (id, project_id = "p", status = "completed") => ({ id, project_id, status, created_at: "2026-10-23T10:00:00Z", result: null });
const delegation = (id, project_id = "p", status = "TASK_STATE_COMPLETED") => ({ id, project_id, status, remote: "fixture", remote_project_id: "remote-p", created_at: null, task: status === "PENDING" ? null : { id: `task-${id}`, artifacts: [] } });

function harness(route, initialStorage = {}) {
  const elements = new Map();
  const element = (id) => {
    if (!elements.has(id)) elements.set(id, new Element(id.endsWith("-history") || id === "a2a-remote" ? "select" : "div"));
    return elements.get(id);
  };
  const storage = new Map(Object.entries(initialStorage));
  const calls = [];
  const timers = new Map();
  let nextTimer = 0;
  const state = { project: null, run: null, disconnected: false };
  const extensions = { capabilities: { sandbox: { enabled: true, available: true }, a2a: { enabled: true, available: true, remotes: [{ name: "fixture", project_id: "remote-p" }] } }, capabilityError: "", loading: false, context: null, submissions: new Map() };
  const scope = vm.createContext({
    $, state, extensions, AbortController, console,
    document: { createElement: (tag) => new Element(tag) },
    sessionStorage: { getItem: (key) => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
    dateTime: (value) => value,
    setTimeout: (callback) => { const id = ++nextTimer; timers.set(id, callback); return id; },
    clearTimeout: (id) => timers.delete(id),
    api: async (path, options = {}) => { calls.push({ path, ...options }); return route(path, options); },
    toast() {},
    updateRunList() {}, setTab() {}, selectRun() {},
  });
  function $(id) { return element(id); }
  vm.runInContext(
    `${source.match(/^  const sandboxActive = .*$/m)[0]}\n${source.match(/^  const delegationActive = .*$/m)[0]}\n${nodeFactory}\n${controllers}\n${events}\n` +
    "this.production = {loadExtensionProject, loadExtensionHistory, selectExtensionRecord, observeExtension, cancelExtension, stopExtensionObservers};",
    scope,
    { filename: "production-extension-history.js" },
  );
  return {
    state, extensions, calls, timers, element, storage,
    production: scope.production,
    async open(id) {
      scope.production.stopExtensionObservers();
      state.project = { id, name: id };
      scope.production.loadExtensionProject(id);
      await flush();
      return extensions.context;
    },
    async runTimer(id) { const callback = timers.get(id); timers.delete(id); assert.ok(callback); await callback(); await flush(); },
  };
}

test("restore a page-excluded selection locally, preserve its cursor, and deduplicate later pages", async () => {
  const first = Array.from({ length: 20 }, (_, index) => delegation(`new-${index}`));
  const saved = delegation("old-selection");
  const cursor = "next/page+token==";
  const ui = harness((path) => {
    if (path === "/projects/p/sandbox/executions/page?limit=20") return page([]);
    if (path === "/projects/p/a2a/delegations/page?limit=20") return page(first, cursor);
    if (path === "/projects/p/a2a/delegations/old-selection") return saved;
    if (path.endsWith("&cursor=" + encodeURIComponent(cursor))) return page([saved, ...Array.from({ length: 5 }, (_, index) => delegation(`older-${index}`))]);
    throw new Error(`Unexpected request: ${path}`);
  }, { "signaldesk.extensionTasks.p": JSON.stringify({ a2aId: saved.id }) });
  const context = await ui.open("p");
  assert.equal(context.a2a.selectedId, saved.id);
  assert.equal(context.a2a.historyIds.length, 20);
  assert.equal(context.a2a.records.length, 21);
  assert.equal(context.a2a.nextCursor, cursor);
  assert.match(ui.element("a2a-history").textContent, /提交时间未知/);
  assert.match(ui.element("a2a-history-status").textContent, /已加载 20 条历史.*另保留当前选择/);
  assert.equal(ui.calls.filter(({ path }) => path.startsWith("/a2a/")).length, 0);

  await ui.element("load-more-a2a").emit("click");
  await flush();
  assert.equal(context.a2a.selectedId, saved.id);
  assert.equal(context.a2a.historyIds.length, 26);
  assert.equal(context.a2a.records.length, 26);
  assert.equal(context.a2a.nextCursor, null);
  assert.equal(ui.element("load-more-a2a").hidden, true);
});

test("a failed refresh or next page keeps the selected task's live observation running", async () => {
  let pageRequests = 0, observations = 0;
  const running = sandbox("active", "p", "running");
  const ui = harness((path) => {
    if (path.startsWith("/projects/p/sandbox/executions/page")) {
      if (++pageRequests === 1) return page([running], "older-cursor");
      throw new Error("Fixture transport unavailable");
    }
    if (path === "/projects/p/a2a/delegations/page?limit=20") return page([]);
    if (path === "/projects/p/sandbox/executions/active") return sandbox("active", "p", ++observations === 1 ? "running" : "completed");
    throw new Error(`Unexpected request: ${path}`);
  });
  const context = await ui.open("p");
  const watchEpoch = context.sandbox.watchEpoch;
  const timer = context.sandbox.timer;
  await ui.production.loadExtensionHistory(context, "sandbox");
  assert.equal(context.sandbox.watchEpoch, watchEpoch);
  assert.equal(context.sandbox.timer, timer);
  assert.equal(context.sandbox.watchId, "active");
  assert.equal(context.sandbox.selectedId, "active");
  assert.equal(context.sandbox.nextCursor, "older-cursor");
  assert.match(ui.element("sandbox-history-error").textContent, /Fixture transport unavailable/);
  await ui.production.loadExtensionHistory(context, "sandbox", true);
  assert.equal(context.sandbox.timer, timer);
  assert.equal(context.sandbox.historyIds.length, 1);
  assert.equal(ui.element("load-more-sandbox").textContent, "重试加载更多");
  await ui.runTimer(timer);
  assert.equal(context.sandbox.record.status, "completed");
  assert.equal(context.sandbox.watchId, null);
  assert.equal(ui.element("sandbox-history-error").hidden, false);
});

test("a delayed local restore cannot override a newer explicit selection, and PENDING never polls remotely", async () => {
  const restore = deferred();
  const known = delegation("known");
  const pending = delegation("pending", "p", "PENDING");
  const ui = harness((path) => {
    if (path === "/projects/p/sandbox/executions/page?limit=20") return page([]);
    if (path === "/projects/p/a2a/delegations/page?limit=20") return page([known, pending]);
    if (path === "/projects/p/a2a/delegations/old") return restore.promise;
    throw new Error(`Unexpected request: ${path}`);
  }, { "signaldesk.extensionTasks.p": JSON.stringify({ a2aId: "old" }) });
  const context = await ui.open("p");
  ui.element("a2a-history").value = known.id;
  ui.element("a2a-history").emit("change");
  restore.resolve(delegation("old"));
  await flush();
  assert.equal(context.a2a.selectedId, known.id);
  ui.element("a2a-history").value = pending.id;
  ui.element("a2a-history").emit("change");
  await flush();
  assert.equal(context.a2a.selectedId, pending.id);
  assert.match(ui.element("a2a-result").textContent, /尚未获得远端 Task ID/);
  assert.equal(ui.calls.filter(({ path }) => path.startsWith("/a2a/")).length, 0);
});

test("old project pages and late cancellation responses cannot steal the current selection", async () => {
  const oldPage = deferred();
  const cancellation = deferred();
  const ui = harness((path, options) => {
    if (path === "/projects/p/sandbox/executions/page?limit=20") return oldPage.promise;
    if (path.endsWith("/a2a/delegations/page?limit=20")) return page([]);
    if (path === "/projects/q/sandbox/executions/page?limit=20") return page([sandbox("a", "q", "running"), sandbox("b", "q")]);
    if (path === "/projects/q/sandbox/executions/a/cancel" && options.method === "POST") return cancellation.promise;
    if (path === "/projects/q/sandbox/executions/a") return sandbox("a", "q", "running");
    throw new Error(`Unexpected request: ${path}`);
  });
  await ui.open("p");
  const context = await ui.open("q");
  oldPage.resolve(page([sandbox("wrong-project", "p")]));
  await flush();
  assert.equal(ui.extensions.context.projectId, "q");
  assert.equal(context.sandbox.records.some(({ id }) => id === "wrong-project"), false);

  const cancelling = ui.production.cancelExtension("sandbox");
  assert.equal(ui.element("sandbox-history").disabled, true);
  ui.element("sandbox-history").value = "b";
  ui.element("sandbox-history").emit("change");
  assert.equal(context.sandbox.selectedId, "a", "busy history handler ignores changes");
  // Also exercise the selected-ID guard against an independent selection intent.
  ui.production.selectExtensionRecord(context, "sandbox", sandbox("b", "q"));
  cancellation.resolve(sandbox("a", "q", "cancelled"));
  await cancelling;
  assert.equal(context.sandbox.selectedId, "b");
  assert.equal(context.sandbox.record.id, "b");
  const count = ui.calls.length;
  await ui.production.observeExtension(context, "sandbox", "a");
  assert.equal(ui.calls.length, count, "late observer does not query or abort the new selection");
});
