// Run with: node --test tests/frontend_run_history.mjs
// Reuses the controlled browser fixture and executes production controllers/events.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../src/signaldesk/static/app.js", import.meta.url), "utf8");
const fixture = await readFile(new URL("frontend_connection.mjs", import.meta.url), "utf8");
const begin = fixture.indexOf("const startup ="); const end = fixture.indexOf('test("wrong Token');
assert.ok(begin >= 0 && end > begin);
const { harness, deferred, json, run, project } = new Function("source", "assert", "vm", "Response", fixture.slice(begin, end) + "return {harness,deferred,json,run,project};")(source, assert, vm, Response);
const flush = () => new Promise((resolve) => setImmediate(resolve));
async function until(check) { for (let i = 0; i < 25 && !check(); i++) await flush(); assert.ok(check(), "Expected production async work settled"); }
const summary = ({ report, ...record }) => record;
const record = (number, status = "completed", pid = "p") => ({ ...run(status), id: `run-${number}`, project_id: pid, created_at: new Date(Date.UTC(2026, 0, 1, 0, number)).toISOString(), updated_at: new Date(Date.UTC(2026, 0, 1, 1, number)).toISOString(), report: status === "completed" ? { ...run("completed").report, title: `Report ${number}`, summary: `REPORT_BODY_${number}` } : null });
const rows = Array.from({ length: 27 }, (_, index) => record(27 - index));
const page = (items, cursor = null, active = null) => ({ items: items.map(summary), next_cursor: cursor, has_more: cursor !== null, active_run: active ? summary(active) : null });
const encoded = (value) => new TextEncoder().encode(`data: ${JSON.stringify(value)}\n\n`);
const finish = (item) => new Response([{ type: "RUN_STARTED", runId: item.id, threadId: item.project_id }, { type: "STATE_SNAPSHOT", snapshot: { run: item } }, { type: "RUN_FINISHED", runId: item.id, threadId: item.project_id }].map((item) => new TextDecoder().decode(encoded(item))).join(""), { headers: { "Content-Type": "text/event-stream" } });
function browser(custom, records = rows) {
  return harness((call) => {
    const response = custom(call); if (response !== undefined) return response;
    const { url } = call;
    if (url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    if (url.includes("/executions/page") || url.includes("/delegations/page")) return json({ items: [], next_cursor: null, has_more: false });
    if (url.includes("/events?")) return json([]);
    const id = url.match(/^\/api\/runs\/([^/?]+)/)?.[1]; const item = records.find((entry) => entry.id === id);
    if (url.includes("/ag-ui?") && item) return finish(item);
    if (/^\/api\/runs\/[^/?]+$/.test(url) && item) return json(item);
    throw new Error(`Unexpected HTTP request: ${url}`);
  });
}
const details = (ui) => ui.calls.filter(({ url }) => /^\/api\/runs\/[^/?]+$/.test(url));
const noLegacy = (ui) => assert.equal(ui.calls.some(({ url }) => url.startsWith("/api/runs?")), false);

test("20-summary pages load to 27, pin an old choice across refresh/reload, and never count pins", async () => {
  const cursor = "opaque/page+cursor==";
  const route = ({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), cursor));
    if (url.endsWith("&cursor=" + encodeURIComponent(cursor))) return json(page(rows.slice(20)));
  };
  const ui = browser(route); await ui.production.selectProject(project);
  assert.equal(ui.production.state.history.ids.length, 20); assert.equal(details(ui).length, 1);
  await ui.$("load-more-runs").emit("click"); assert.equal(ui.production.state.history.ids.length, 27); assert.equal(details(ui).length, 1, "Pagination never prefetches reports");
  ui.$("run-selector").value = "run-1"; await ui.$("run-selector").emit("change");
  await ui.$("refresh-runs").emit("click");
  assert.equal(ui.production.state.run.id, "run-1"); assert.equal(ui.production.state.history.ids.length, 20);
  assert.equal(ui.production.state.history.nextCursor, cursor); assert.equal(ui.$("metric-runs").textContent, "20");
  assert.match(ui.$("history-content").textContent, /页外当前选择/); assert.match(ui.$("metric-runs-note").textContent, /已加载/);
  const reloaded = browser(route); reloaded.storage.set("signaldesk.runId.p", "run-1"); await reloaded.production.selectProject(project);
  assert.equal(reloaded.production.state.run.id, "run-1"); assert.equal(reloaded.production.state.history.ids.length, 20);
  assert.equal(reloaded.production.state.history.nextCursor, cursor); assert.deepEqual(details(reloaded).map(({ url }) => url), ["/api/runs/run-1"]);
  noLegacy(ui); noLegacy(reloaded);
});

test("pagination failure preserves its cursor and selection; a later page cannot steal newer selection", async () => {
  const cursor = "older"; const pending = deferred(); let attempts = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), cursor));
    if (url.endsWith("&cursor=older")) return ++attempts === 1 ? json({ detail: "Fixture page unavailable" }, 503) : pending.promise;
  });
  await ui.production.selectProject(project); await ui.$("load-more-runs").emit("click");
  assert.equal(ui.production.state.history.ids.length, 20); assert.equal(ui.production.state.history.nextCursor, cursor);
  assert.match(ui.$("run-history-error").textContent, /Fixture page unavailable/); assert.equal(ui.production.state.run.id, "run-27");
  const loading = ui.$("load-more-runs").emit("click"); ui.$("run-selector").value = "run-10"; await ui.$("run-selector").emit("change");
  pending.resolve(json(page(rows.slice(20)))); await loading;
  assert.equal(ui.production.state.run.id, "run-10"); assert.equal(ui.production.state.history.ids.length, 27);
  assert.equal(ui.production.state.history.nextCursor, null); assert.equal(ui.$("load-more-runs").hidden, true);
});

test("off-page activity locks writes, stays observed after a history error, and never hydrates background reports", async () => {
  const active = record(2, "running"); let stream; let lists = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return ++lists === 1 ? json(page(rows.slice(0, 20), "older", active)) : json({ detail: "History temporarily unavailable" }, 503);
    if (url.startsWith("/api/runs/run-2/ag-ui?")) return new Response(new ReadableStream({ start(controller) { stream = controller; controller.enqueue(encoded({ type: "RUN_STARTED", runId: active.id, threadId: "p" })); controller.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: active } })); } }), { headers: { "Content-Type": "text/event-stream" } });
    if (url === "/api/runs/run-2") throw new Error("Background full-report GET is forbidden");
  }, [...rows.filter((item) => item.id !== active.id), active]);
  await ui.production.selectProject(project); await until(() => !!stream);
  const subscription = ui.calls.find(({ url }) => url.startsWith("/api/runs/run-2/ag-ui?"));
  assert.equal(ui.production.state.history.ids.length, 20); assert.equal(ui.production.state.history.activeId, active.id);
  assert.equal(ui.$("edit-project").disabled, true); assert.equal(ui.$("start-run").disabled, true);
  await ui.$("refresh-runs").emit("click"); assert.equal(subscription.signal.aborted, false, "List failure does not stop independent observation");
  stream.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: { ...summary(record(2)), updated_at: "2026-01-01T02:02:00Z", report: null }, reportAvailable: true, reportOmitted: true } }));
  stream.enqueue(encoded({ type: "RUN_FINISHED", runId: active.id, threadId: "p" })); stream.close();
  await until(() => ui.production.state.history.activeId === null);
  assert.equal(ui.$("edit-project").disabled, false); assert.equal(ui.$("start-run").disabled, false);
  assert.equal(details(ui).some(({ url }) => url === "/api/runs/run-2"), false);
  assert.equal(ui.production.state.history.ids.length, 20); noLegacy(ui);
});

test("unknown or failed activity refresh conservatively blocks edit/start and a fresh page restores them", async () => {
  let fail = true;
  const ui = browser(({ url }) => { if (url === "/api/projects/p/runs/page?limit=20") return fail ? json({ detail: "Fixture API unavailable" }, 503) : json(page(rows.slice(0, 20), "older")); });
  await ui.production.selectProject(project);
  assert.equal(ui.production.state.history.activityKnown, false); assert.equal(ui.$("start-run").disabled, true); assert.equal(ui.$("edit-project").disabled, true);
  assert.match(ui.$("run-activity-error").textContent, /尚未确认/);
  fail = false; await ui.$("refresh-runs").emit("click"); await until(() => ui.production.state.run?.id === "run-27");
  const report = ui.production.state.run.report;
  assert.equal(ui.$("edit-project").disabled, false);
  fail = true; await ui.$("refresh-runs").emit("click");
  assert.equal(ui.$("edit-project").disabled, true); assert.deepEqual(ui.production.state.run.report, report);
  fail = false; await ui.$("refresh-runs").emit("click");
  assert.equal(ui.$("edit-project").disabled, false); assert.deepEqual(ui.production.state.runs.find((item) => item.id === "run-27").report, report, "A report-free summary does not erase loaded details");
});

test("temporary detail failure preserves a saved off-page choice and the real retry button restores it", async () => {
  let reads = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "older"));
    if (url === "/api/runs/run-1" && ++reads === 1) return json({ detail: "Details temporarily unavailable" }, 503);
  });
  ui.storage.set("signaldesk.runId.p", "run-1"); await ui.production.selectProject(project);
  assert.equal(ui.production.state.selectedRunId, "run-1"); assert.equal(ui.storage.get("signaldesk.runId.p"), "run-1");
  assert.equal(ui.$("run-selector").value, "run-1"); assert.match(ui.$("poll-error-message").textContent, /Details temporarily unavailable/);
  await ui.$("retry-poll").emit("click");
  assert.equal(ui.production.state.run.id, "run-1"); assert.equal(reads, 2);
  assert.equal(ui.production.state.history.ids.length, 20); assert.equal(ui.production.state.history.nextCursor, "older");
  assert.equal(details(ui).some(({ url }) => url === "/api/runs/run-27"), false);
});

test("missing or foreign saved detail is discarded without displaying it, then a valid page choice is used", async () => {
  for (const foreign of [false, true]) {
    const ui = browser(({ url }) => {
      if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "older"));
      if (url === "/api/runs/invalid") return foreign ? json({ ...record(1), id: "invalid", project_id: "q", report: { title: "PRIVATE_FOREIGN_REPORT" } }) : json({ detail: "Missing run" }, 404);
    });
    ui.storage.set("signaldesk.runId.p", "invalid"); await ui.production.selectProject(project);
    assert.equal(ui.production.state.run.id, "run-27"); assert.equal(ui.storage.get("signaldesk.runId.p"), "run-27");
    assert.equal(ui.production.state.runs.some((item) => item.project_id === "q"), false); assert.doesNotMatch(ui.$("report-content").textContent, /PRIVATE_FOREIGN_REPORT/);
    assert.equal(ui.production.state.history.ids.length, 20);
  }
});

test("a delayed old-project page cannot populate a newer project's history", async () => {
  const old = deferred(); const q = { ...project, id: "q", name: "New project" }; const qRun = record(100, "completed", "q");
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return old.promise;
    if (url === "/api/projects/q/runs/page?limit=20") return json(page([qRun]));
  }, [...rows, qRun]);
  const pending = ui.production.selectProject(project); await ui.production.selectProject(q);
  old.resolve(json(page(rows.slice(0, 20), "old-cursor"))); await pending;
  assert.equal(ui.production.state.project.id, "q"); assert.equal(ui.production.state.run.id, qRun.id);
  assert.deepEqual(Array.from(ui.production.state.history.ids), [qRun.id]); assert.equal(ui.calls.find(({ url }) => url === "/api/projects/p/runs/page?limit=20").signal.aborted, true);
});

test("JSON fallback refreshes bounded activity metadata without fetching historical reports or resetting pages", async () => {
  const active = record(2, "running"); let activity = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "page-cursor", active));
    if (url === "/api/projects/p/runs/page?limit=1") return json(page([rows[0]], "IGNORED_CURSOR", ++activity === 1 ? active : null));
    if (url.includes("/ag-ui?")) return json({ detail: "Not supported" }, 404);
  }, [...rows.filter((item) => item.id !== active.id), active]);
  await ui.production.selectProject(project); await until(() => activity === 1 && !!ui.production.state.timer);
  assert.equal(ui.production.state.history.nextCursor, "page-cursor"); assert.equal(ui.production.state.history.ids.length, 20);
  await ui.production.pollRun(ui.production.state.epoch);
  assert.equal(activity, 2); assert.equal(ui.production.state.history.activeId, null); assert.equal(ui.$("edit-project").disabled, false);
  assert.equal(details(ui).length, 1, "A selected completed report is not fetched on every activity poll");
  assert.equal(ui.production.state.history.nextCursor, "page-cursor"); assert.equal(ui.production.state.history.ids.length, 20); noLegacy(ui);
});

test("failed activity polling locks writes visibly and the real retry restores bounded observation", async () => {
  const active = record(2, "running"); let activity = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "page-cursor", active));
    if (url === "/api/projects/p/runs/page?limit=1") return ++activity === 1 ? json({ detail: "Activity fixture unavailable" }, 503) : json(page([rows[0]], "IGNORED_CURSOR", null));
    if (url.includes("/ag-ui?")) return json({ detail: "Not supported" }, 404);
  }, [...rows.filter((item) => item.id !== active.id), active]);
  await ui.production.selectProject(project); await until(() => !!ui.production.state.history.activityError && !!ui.production.state.timer);
  assert.match(ui.$("run-activity-error").textContent, /Activity fixture unavailable/);
  assert.equal(ui.$("edit-project").disabled, true); assert.equal(ui.$("start-run").disabled, true);
  await ui.$("retry-poll").emit("click"); await until(() => activity === 2 && !ui.production.state.pollingError);
  assert.equal(ui.production.state.history.activityError, ""); assert.equal(ui.$("edit-project").disabled, false);
  assert.equal(ui.production.state.history.nextCursor, "page-cursor"); assert.equal(ui.production.state.history.ids.length, 20);
  assert.equal(details(ui).length, 1); noLegacy(ui);
});

test("a stale page cannot clear activity created after its request began", async () => {
  const refresh = deferred(); const created = record(100, "running"); let requests = 0, controller;
  const ui = browser(({ url, method }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return ++requests === 1 ? json(page(rows.slice(0, 20), "older")) : refresh.promise;
    if (url === "/api/projects/p/runs" && method === "POST") return json(created, 202);
    if (url.startsWith("/api/runs/run-100/ag-ui?")) return new Response(new ReadableStream({ start(value) { controller = value; value.enqueue(encoded({ type: "RUN_STARTED", runId: created.id, threadId: "p" })); value.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: created } })); } }), { headers: { "Content-Type": "text/event-stream" } });
  }, [...rows, created]);
  await ui.production.selectProject(project); const refreshing = ui.$("refresh-runs").emit("click"); await ui.$("start-run").emit("click");
  await until(() => !!controller); refresh.resolve(json(page(rows.slice(0, 20), "older", null))); await refreshing;
  assert.equal(ui.production.state.run.id, created.id); assert.equal(ui.production.state.history.activeId, created.id);
  assert.equal(ui.production.state.history.ids.length, 20); assert.equal(ui.$("metric-runs").textContent, "20");
  assert.equal(ui.$("edit-project").disabled, true); assert.equal(ui.$("start-run").disabled, true);
  const completed = { ...record(100), updated_at: "2026-01-01T04:00:00Z" };
  controller.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: completed } })); controller.enqueue(encoded({ type: "RUN_FINISHED", runId: completed.id, threadId: "p" })); controller.close();
  await until(() => ui.production.state.streamStatus === "ended");
});

test("changing selection to a failed detail keeps independent off-page SSE activity and its later completion unlocks writes", async () => {
  const active = record(2, "running"); const controllers = []; let failures = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "older", active));
    if (url === "/api/runs/run-26" && ++failures === 1) return json({ detail: "Selected details unavailable" }, 503);
    if (url.startsWith("/api/runs/run-2/ag-ui?")) return new Response(new ReadableStream({ start(controller) { controllers.push(controller); controller.enqueue(encoded({ type: "RUN_STARTED", runId: active.id, threadId: "p" })); controller.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: active } })); } }), { headers: { "Content-Type": "text/event-stream" } });
    if (url === "/api/runs/run-2") throw new Error("Background report hydration is forbidden");
  }, [...rows.filter((item) => item.id !== active.id), active]);
  await ui.production.selectProject(project); await until(() => ui.production.state.streamStatus === "ended" && controllers.length === 1);
  ui.$("run-selector").value = "run-26"; await ui.$("run-selector").emit("change");
  await until(() => controllers.length === 2 && ui.production.state.backgroundStreams.get(active.id)?.status === "connected");
  assert.equal(ui.production.state.selectedRunId, "run-26"); assert.match(ui.production.state.detailError, /Selected details unavailable/);
  assert.equal(ui.$("edit-project").disabled, true); controllers[0].close();
  controllers[1].enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: { ...summary(record(2)), updated_at: "2026-01-01T04:00:00Z", report: null }, reportAvailable: true, reportOmitted: true } }));
  controllers[1].enqueue(encoded({ type: "RUN_FINISHED", runId: active.id, threadId: "p" })); controllers[1].close();
  await until(() => ui.production.state.history.activeId === null && ui.production.state.backgroundStreams.size === 0);
  assert.equal(ui.$("edit-project").disabled, false); assert.equal(ui.$("start-run").disabled, false);
  assert.match(ui.$("poll-error-message").textContent, /Selected details unavailable/); assert.equal(ui.$("poll-error").hidden, false);
  assert.equal(ui.storage.get("signaldesk.runId.p"), "run-26"); assert.equal(ui.production.state.history.ids.length, 20);
  assert.equal(ui.production.state.history.nextCursor, "older"); assert.equal(details(ui).some(({ url }) => url === "/api/runs/run-2"), false);
  await ui.$("retry-poll").emit("click"); assert.equal(ui.production.state.run.id, "run-26"); assert.equal(ui.production.state.detailError, "");
});

test("selected active task is observed before its slow detail GET settles and remains observed after failure", async () => {
  const active = record(2, "running"); const detail = deferred(); let controller;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "older", active));
    if (url === "/api/runs/run-2") return detail.promise;
    if (url.startsWith("/api/runs/run-2/ag-ui?")) return new Response(new ReadableStream({ start(value) { controller = value; value.enqueue(encoded({ type: "RUN_STARTED", runId: active.id, threadId: "p" })); value.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: active } })); } }), { headers: { "Content-Type": "text/event-stream" } });
  }, [...rows.filter((item) => item.id !== active.id), active]);
  ui.storage.set("signaldesk.runId.p", active.id); const opening = ui.production.selectProject(project);
  await until(() => !!ui.production.state.history?.detailObservationTimer);
  const timer = ui.production.state.history.detailObservationTimer; ui.timers.get(timer)();
  await until(() => !!controller && ui.production.state.backgroundStreams.get(active.id)?.status === "connected");
  assert.equal(ui.production.state.detailLoading, true, "Activity observation does not wait for detail success");
  detail.resolve(json({ detail: "Active details unavailable" }, 503)); await opening;
  controller.enqueue(encoded({ type: "STATE_SNAPSHOT", snapshot: { run: { ...summary(record(2)), updated_at: "2026-01-01T04:00:00Z", report: null }, reportAvailable: true, reportOmitted: true } }));
  controller.enqueue(encoded({ type: "RUN_FINISHED", runId: active.id, threadId: "p" })); controller.close();
  await until(() => ui.production.state.history.activeId === null);
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.$("edit-project").disabled, false);
  assert.match(ui.production.state.detailError, /Active details unavailable/); assert.equal(details(ui).length, 1);
});

test("detail failure in JSON fallback keeps lightweight project activity polling without retrying the failed report", async () => {
  const active = record(2, "running"); let metadata = 0, failedDetails = 0;
  const ui = browser(({ url }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json(page(rows.slice(0, 20), "older", active));
    if (url === "/api/projects/p/runs/page?limit=1") return json(page([rows[0]], "IGNORED_CURSOR", ++metadata === 1 ? active : null));
    if (url.includes("/ag-ui?")) return json({ detail: "Unsupported stream" }, 404);
    if (url === "/api/runs/run-26") { failedDetails++; return json({ detail: "Fallback details unavailable" }, 503); }
  }, [...rows.filter((item) => item.id !== active.id), active]);
  await ui.production.selectProject(project); await until(() => metadata === 1 && !!ui.production.state.timer);
  ui.$("run-selector").value = "run-26"; await ui.$("run-selector").emit("change");
  await until(() => metadata === 2 && ui.production.state.history.activeId === null);
  assert.equal(failedDetails, 1); assert.equal(ui.production.state.selectedRunId, "run-26");
  assert.match(ui.production.state.detailError, /Fallback details unavailable/); assert.equal(ui.$("edit-project").disabled, false);
  assert.equal(ui.production.state.streamStatus, "fallback"); assert.equal(ui.production.state.history.nextCursor, "older");
  assert.equal(ui.production.state.history.ids.length, 20); noLegacy(ui);
});
