// Run with: node --test tests/frontend_run_races.mjs
// Reuse the existing browser fixture, then execute the complete production app.
// No production controller or transition logic is reproduced in this test.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../src/signaldesk/static/app.js", import.meta.url), "utf8");
const fixture = await readFile(new URL("frontend_connection.mjs", import.meta.url), "utf8");
const begin = fixture.indexOf("const startup ="); const end = fixture.indexOf('test("wrong Token');
assert.ok(begin >= 0 && end > begin, "Existing controlled browser fixture is available");
const { harness, deferred, json, run } = new Function("source", "assert", "vm", "Response", fixture.slice(begin, end) + "return {harness, deferred, json, run};")(source, assert, vm, Response);
const flush = () => new Promise((resolve) => setImmediate(resolve));
async function until(predicate) { for (let i = 0; i < 20 && !predicate(); i++) await flush(); assert.ok(predicate(), "Expected production async work settled"); }
const at = (status, time) => ({ ...run(status), updated_at: `2026-10-23T00:00:01.${time}Z` });
const event = (value) => `data: ${JSON.stringify(value)}\n\n`;
function sse(records) { return new Response(records.map(event).join(""), { headers: { "Content-Type": "text/event-stream" } }); }
function terminal(record) { return [{ type: "RUN_STARTED", runId: "r", threadId: "p" }, { type: "STATE_SNAPSHOT", snapshot: { run: record } }, record.status === "completed" ? { type: "RUN_FINISHED", runId: "r", threadId: "p" } : { type: "RUN_ERROR", message: "Fixture terminal state" }]; }

test("a delayed detail response cannot undo a resumed run's completed state", async () => {
  const detail = deferred(); const failed = at("failed", "123100"); const resumed = at("running", "123200"); const completed = at("completed", "123300");
  const ui = harness(({ url }) => {
    if (url === "/api/runs/r") return detail.promise;
    if (url === "/api/runs/r/resume") return json(resumed);
    if (url.includes("/ag-ui?")) return sse(terminal(completed));
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  ui.production.state.runs = [failed]; ui.$("run-selector").value = "r";
  const selecting = ui.$("run-selector").emit("change");
  await ui.$("resume-run").emit("click"); await until(() => ui.production.state.streamStatus === "ended");
  detail.resolve(json(failed)); await selecting;
  assert.equal(ui.production.state.run.status, "completed");
  assert.equal(ui.production.state.runs[0].status, "completed");
  assert.match(ui.$("report-content").textContent, /Fixture report/);
  assert.equal(ui.$("resume-run").hidden, true);
});

test("an old JSON poll cannot undo fresh observation after a cancellation request fails", async () => {
  const oldPoll = deferred(); let reads = 0;
  const ui = harness(({ url }) => {
    if (url === "/api/runs/r") return ++reads === 1 ? oldPoll.promise : json(at("completed", "123300"));
    if (url === "/api/runs/r/cancel") return json({ detail: "Task already completed" }, 409);
    if (url.includes("/events/page?")) return json({ items: [], next_after: 0, has_more: false, producer_active: false });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  const active = at("running", "123100"); Object.assign(ui.production.state, { run: active, runs: [active], streamSupported: false });
  const pending = ui.production.pollRun(ui.production.state.epoch);
  await ui.$("cancel-run").emit("click"); await until(() => ui.production.state.run.status === "completed");
  oldPoll.resolve(json(active)); await pending;
  assert.equal(reads, 2, "Failed action restores a fresh observation");
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.production.state.runs[0].status, "completed");
  assert.equal(ui.$("cancel-run").hidden, true);
});

test("SSE preserves sub-millisecond ordering for both selected state and history", async () => {
  const initial = at("running", "123000"); const completed = at("completed", "123900");
  const ui = harness(({ url }) => {
    if (url === "/api/runs/r") return json(initial);
    if (url.includes("/ag-ui?")) return sse([
      { type: "RUN_STARTED", runId: "r", threadId: "p" },
      { type: "STATE_SNAPSHOT", snapshot: { run: completed } },
      { type: "STATE_SNAPSHOT", snapshot: { run: at("running", "123100") } },
      { type: "RUN_FINISHED", runId: "r", threadId: "p" },
    ]);
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  assert.equal(Date.parse(initial.updated_at), Date.parse(completed.updated_at), "Fixture is inside one millisecond");
  ui.production.state.runs = [initial]; ui.$("run-selector").value = "r"; await ui.$("run-selector").emit("change");
  await until(() => ui.production.state.streamStatus === "ended");
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.production.state.run.updated_at, completed.updated_at);
  assert.equal(ui.production.state.runs[0].updated_at, completed.updated_at);
});

test("omitted report hydration keeps a newer SSE state and retries until full details are available", async () => {
  const active = at("running", "123100"); const completed = at("completed", "123900"); let reads = 0, streams = 0;
  const ui = harness(({ url }) => {
    if (url === "/api/runs/r") return json(++reads < 3 ? active : completed);
    if (url.includes("/ag-ui?")) {
      streams++;
      return sse([
        { type: "RUN_STARTED", runId: "r", threadId: "p" },
        { type: "STATE_SNAPSHOT", snapshot: { run: { ...completed, report: null }, reportAvailable: true, reportOmitted: true } },
        { type: "RUN_FINISHED", runId: "r", threadId: "p" },
      ]);
    }
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  ui.production.state.runs = [active]; ui.$("run-selector").value = "r"; await ui.$("run-selector").emit("change");
  await until(() => ui.production.state.streamStatus === "reconnecting");
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.production.state.run.updated_at, completed.updated_at);
  assert.equal(ui.production.state.runs[0].updated_at, completed.updated_at); assert.equal(ui.production.state.run.report, null);
  assert.match(ui.$("poll-error-message").textContent, /报告详情尚未同步/);
  assert.doesNotMatch(ui.$("toast").textContent, /报告和原始证据已就绪/);
  await ui.$("retry-stream").emit("click"); await until(() => ui.production.state.streamStatus === "ended");
  assert.equal(streams, 2); assert.equal(reads, 3); assert.equal(ui.production.state.run.status, "completed");
  assert.equal(ui.production.state.run.report.title, completed.report.title);
  assert.match(ui.$("report-content").textContent, /Fixture report/);
  assert.match(ui.$("toast").textContent, /报告和原始证据已就绪/);
});

test("resume accepts failed to running while rejecting a late event from the previous subscription", async () => {
  const failed = at("failed", "123100"); const resumed = at("running", "123500");
  let oldController, newController; let streams = 0;
  const ui = harness(({ url }) => {
    if (url === "/api/runs/r") return json(failed);
    if (url === "/api/runs/r/resume") return json(resumed);
    if (url.includes("/ag-ui?")) {
      const current = ++streams;
      return new Response(new ReadableStream({ start(controller) {
        if (current === 1) oldController = controller; else newController = controller;
        const records = [{ type: "RUN_STARTED", runId: "r", threadId: "p" }, { type: "STATE_SNAPSHOT", snapshot: { run: current === 1 ? failed : resumed } }];
        controller.enqueue(new TextEncoder().encode(records.map(event).join("")));
      } }), { headers: { "Content-Type": "text/event-stream" } });
    }
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  ui.production.state.runs = [failed]; ui.$("run-selector").value = "r"; await ui.$("run-selector").emit("change");
  await until(() => ui.production.state.streamStatus === "connected");
  await ui.$("resume-run").emit("click"); await until(() => streams === 2 && ui.production.state.streamStatus === "connected");
  const subscriptions = ui.calls.filter(({ url }) => url.includes("/ag-ui?"));
  assert.equal(subscriptions[0].signal.aborted, true); assert.equal(subscriptions[1].signal.aborted, false);
  // Intentionally ignore HTTP abort and deliver an old event with a later timestamp.
  // Subscription generation must reject it independently of status or wall-clock order.
  oldController.enqueue(new TextEncoder().encode(event({ type: "STATE_SNAPSHOT", snapshot: { run: at("failed", "123999") } }))); oldController.close();
  await flush();
  assert.equal(ui.production.state.run.status, "running"); assert.equal(ui.production.state.runs[0].status, "running");
  assert.equal(ui.production.state.streamStatus, "connected");
  const completed = at("completed", "124000");
  newController.enqueue(new TextEncoder().encode(event({ type: "STATE_SNAPSHOT", snapshot: { run: completed } }) + event({ type: "RUN_FINISHED", runId: "r", threadId: "p" }))); newController.close();
  await until(() => ui.production.state.streamStatus === "ended");
  assert.equal(ui.production.state.run.status, "completed");
});

test("editing a pending or completed query invalidates old results and requires explicit search", async () => {
  const oldSearch = deferred();
  const ui = harness(({ url }) => {
    if (url.endsWith("q=alpha")) return oldSearch.promise;
    if (url.endsWith("q=beta")) return json([{ evidence_id: "beta", title: "BETA_ONLY_RESULT" }]);
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  ui.$("evidence-query").value = "alpha"; const searching = ui.$("search-evidence").emit("click");
  ui.$("evidence-query").value = "beta"; await ui.$("evidence-query").emit("input");
  assert.equal(ui.calls[0].signal.aborted, true); assert.equal(ui.$("search-evidence").disabled, false);
  assert.match(ui.$("search-status").textContent, /关键词已修改/); assert.equal(ui.$("search-status").hidden, false);
  assert.equal(ui.$("evidence-content").attributes["aria-busy"], undefined);
  oldSearch.resolve(json([{ evidence_id: "alpha", title: "ALPHA_ONLY_RESULT" }])); await searching;
  assert.doesNotMatch(ui.$("evidence-content").textContent, /ALPHA_ONLY_RESULT/);
  await ui.$("search-evidence").emit("click"); assert.match(ui.$("evidence-content").textContent, /BETA_ONLY_RESULT/);
  ui.$("evidence-query").value = "gamma"; await ui.$("evidence-query").emit("input");
  assert.doesNotMatch(ui.$("evidence-content").textContent, /BETA_ONLY_RESULT/);
  assert.match(ui.$("evidence-content").textContent, /等待搜索新关键词/);
  ui.$("evidence-query").value = ""; await ui.$("evidence-query").emit("input");
  assert.equal(ui.$("search-status").hidden, true);
  assert.doesNotMatch(ui.$("evidence-content").textContent, /等待搜索新关键词/);
});

test("late selected-run details preserve a search started while those details were loading", async () => {
  const detail = deferred(); const completed = at("completed", "123900");
  const ui = harness(({ url }) => {
    if (url === "/api/runs/r") return detail.promise;
    if (url.includes("/search?")) return json([{ evidence_id: "alpha", title: "ALPHA_ONLY_RESULT" }]);
    if (url.includes("/ag-ui?")) return sse(terminal(completed));
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
  ui.production.state.runs = [completed]; ui.$("run-selector").value = "r";
  const selecting = ui.$("run-selector").emit("change");
  ui.$("evidence-query").value = "alpha"; await ui.$("search-evidence").emit("click");
  detail.resolve(json(completed)); await selecting; await until(() => ui.production.state.streamStatus === "ended");
  assert.equal(ui.$("evidence-query").value, "alpha"); assert.match(ui.$("evidence-content").textContent, /ALPHA_ONLY_RESULT/);
  assert.doesNotMatch(ui.$("evidence-content").textContent, /CURRENT_REPORT_BODY/);
});
