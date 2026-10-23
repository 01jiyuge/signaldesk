// Run with: node --test tests/frontend_event_replay.mjs
// Full production handlers with controlled HTTP/SSE, DOM primitives, and timers.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../src/signaldesk/static/app.js", import.meta.url), "utf8");
const fixture = await readFile(new URL("frontend_connection.mjs", import.meta.url), "utf8");
const begin = fixture.indexOf("const startup ="); const end = fixture.indexOf('test("wrong Token');
assert.ok(begin >= 0 && end > begin);
// Supply the native DOM operation used when an expired event leaves the window.
const setup = fixture.slice(begin, end).replace("  get childElementCount()", "  removeChild(item) { const index = this.children.indexOf(item); if (index < 0) throw new Error('Missing DOM child'); this.children.splice(index, 1); return item; }\n  get childElementCount()");
const { harness, deferred, json, run, project } = new Function("source", "assert", "vm", "Response", setup + "return {harness,deferred,json,run,project};")(source, assert, vm, Response);
const flush = () => new Promise((resolve) => setImmediate(resolve));
async function until(check) { for (let i = 0; i < 40 && !check(); i++) await flush(); assert.ok(check(), "Expected production async work settled"); }
const businessEvent = (id, runId = "r") => ({ id, run_id: runId, kind: "progress", message: `EVENT_${id}`, data: {}, created_at: "2026-10-23T00:00:01Z" });
const events = (start, end, runId = "r") => Array.from({ length: end - start + 1 }, (_, offset) => businessEvent(start + offset, runId));
const eventPage = (items, more = false, after = 0, producerActive = false) => ({ items, next_after: items.at(-1)?.id ?? after, has_more: more, producer_active: producerActive });
const encode = (item, cursor = null) => `${cursor === null ? "" : `id: ${cursor}\n`}data: ${JSON.stringify(item)}\n\n`;
const custom = (item, cursor = item.id) => encode({ type: "CUSTOM", name: "signaldesk.event", value: item }, cursor);
const sse = (frames) => new Response(frames.join(""), { headers: { "Content-Type": "text/event-stream" } });
const snapshot = (record) => encode({ type: "STATE_SNAPSHOT", snapshot: { run: record } });
const done = (id = "r") => encode({ type: "RUN_FINISHED", runId: id, threadId: "p" });
function browser(route) {
  return harness((call) => {
    const answer = route(call); if (answer !== undefined) return answer;
    if (call.url.endsWith("/memory")) return json({ preferences: [], recent_runs: [] });
    if (call.url.includes("/executions/page") || call.url.includes("/delegations/page")) return json({ items: [], next_cursor: null, has_more: false });
    if (call.url === "/api/runs/r") return json(run("completed"));
    throw new Error(`Unexpected HTTP request: ${call.url}`);
  });
}
async function select(ui, record = run("completed")) {
  ui.production.state.runs = [record]; ui.$("run-selector").value = record.id;
  await ui.$("run-selector").emit("change");
}
async function nextPoll(ui) {
  const timer = ui.production.state.timer; assert.ok(timer, "A continuation is scheduled");
  const callback = ui.timers.get(timer); assert.ok(callback); ui.timers.delete(timer); await callback();
}
const pageCalls = (ui) => ui.calls.filter(({ url }) => url.includes("/events/page?"));
const pageAfter = ({ url }) => Number(new URL(url, "https://fixture.example").searchParams.get("after"));
const visibleIds = (ui) => ui.$("event-list").children.map((item) => Number(item.dataset.eventId));

test("completed fallback drains only three pages per turn and bounds both state and DOM to the newest 200", async () => {
  const ui = browser(({ url }) => {
    if (url.includes("/ag-ui?")) return json({ detail: "Unavailable" }, 404);
    if (url.includes("/events/page?")) {
      const parsed = new URL(url, "https://fixture.example"); const after = Number(parsed.searchParams.get("after"));
      assert.equal(parsed.searchParams.get("limit"), "100");
      const end = Math.min(after + 100, 650); return json(eventPage(events(after + 1, end), end < 650));
    }
  });
  await select(ui); await until(() => ui.production.state.eventAfter === 300 && !!ui.production.state.timer);
  assert.equal(pageCalls(ui).length, 3); assert.equal(ui.production.state.eventHasMore, true);
  assert.deepEqual(visibleIds(ui), events(101, 300).map((item) => item.id));
  assert.equal(ui.production.state.events.length, 200); assert.match(ui.$("event-replay-status").textContent, /分批/);
  await nextPoll(ui); assert.equal(ui.production.state.eventAfter, 600); assert.equal(pageCalls(ui).length, 6);
  await nextPoll(ui); assert.equal(ui.production.state.eventAfter, 650); assert.equal(pageCalls(ui).length, 7);
  assert.equal(ui.production.state.eventHasMore, false); assert.equal(ui.production.state.timer, null);
  assert.equal(ui.$("event-replay-status").textContent, "");
  assert.deepEqual(visibleIds(ui), events(451, 650).map((item) => item.id));
  assert.equal(ui.calls.some(({ url }) => /\/events\?/.test(url)), false, "The unbounded endpoint is never used");
});

test("page failure keeps confirmed events and retries from the last valid cursor through the real retry handler", async () => {
  let failed = false;
  const ui = browser(({ url }) => {
    if (url.includes("/ag-ui?")) return json({ detail: "Unavailable" }, 404);
    if (url.includes("/events/page?")) {
      const after = pageAfter({ url });
      if (!after) return json(eventPage(events(1, 100), true));
      if (!failed) { failed = true; return json({ detail: "Event fixture unavailable" }, 503); }
      return json(eventPage(events(101, 125)));
    }
  });
  await select(ui); await until(() => !!ui.production.state.pollingError && !!ui.production.state.timer);
  assert.equal(ui.production.state.eventAfter, 100); assert.equal(visibleIds(ui).length, 100);
  assert.match(ui.$("poll-error-message").textContent, /Event fixture unavailable/);
  await ui.$("retry-poll").emit("click"); await until(() => ui.production.state.eventAfter === 125 && !ui.production.state.eventLoad);
  assert.deepEqual(pageCalls(ui).map(pageAfter), [0, 100, 100]);
  assert.equal(visibleIds(ui).length, 125); assert.equal(ui.production.state.pollingError, "");
});

test("a completed report keeps polling an active producer until its final events are committed and the producer exits", async () => {
  let committed = []; let producing = true;
  const ui = browser(({ url }) => {
    if (url.includes("/ag-ui?")) return json({ detail: "Unavailable" }, 404);
    if (url.includes("/events/page?")) return json(eventPage(committed.filter((item) => item.id > pageAfter({ url })), false, pageAfter({ url }), producing));
  });
  await select(ui); await until(() => pageCalls(ui).length === 1 && !!ui.production.state.timer);
  assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.production.state.eventAfter, 0);
  assert.equal(ui.production.state.eventHasMore, false); assert.equal(ui.production.state.eventProducerActive, true);
  assert.match(ui.$("event-replay-status").textContent, /最后执行事件/);
  committed = [businessEvent(1)]; await nextPoll(ui);
  assert.equal(ui.production.state.eventAfter, 1); assert.equal(ui.production.state.eventProducerActive, true);
  assert.ok(ui.production.state.timer, "An exhausted page is not final while its producer is active");
  committed = events(1, 2); producing = false; await nextPoll(ui);
  assert.deepEqual(pageCalls(ui).map(pageAfter), [0, 0, 1]); assert.deepEqual(visibleIds(ui), [1, 2]);
  assert.equal(ui.production.state.eventProducerActive, false); assert.equal(ui.production.state.timer, null);
  assert.equal(ui.$("event-replay-status").textContent, "");
  assert.equal(ui.calls.filter(({ url }) => url === "/api/runs/r").length, 1, "Waiting for tail events never reloads the completed report");
});

test("invalid page shapes, ordering, foreign events, and cursor jumps never commit any part of the page", async () => {
  const variants = [
    { items: {}, next_after: 200, has_more: false },
    { items: events(101, 200), next_after: 999, has_more: false },
    { items: [businessEvent(102), businessEvent(101)], next_after: 101, has_more: false },
    { items: [businessEvent(101, "another-run")], next_after: 101, has_more: false },
    { items: [], next_after: 100, has_more: true },
    { items: [businessEvent(101)], next_after: "101", has_more: false },
    { items: [businessEvent(101)], next_after: 101, has_more: "false" },
    { items: [businessEvent(101)], next_after: 101, has_more: false, producer_active: "false" },
    { items: events(101, 201), next_after: 201, has_more: false },
  ];
  for (const invalid of variants) {
    const ui = browser(({ url }) => {
      if (url.includes("/ag-ui?")) return json({ detail: "Unavailable" }, 404);
      if (url.includes("/events/page?")) return json(pageAfter({ url }) === 0 ? eventPage(events(1, 100), true) : { producer_active: false, ...invalid });
    });
    await select(ui); await until(() => !!ui.production.state.pollingError);
    assert.equal(ui.production.state.eventAfter, 100); assert.equal(ui.production.state.events.length, 100);
    assert.deepEqual(visibleIds(ui), events(1, 100).map((item) => item.id));
    assert.ok(ui.production.state.timer, "Invalid data remains retryable even for a completed run");
  }
});

test("a late event page cannot paint or advance the newly selected run even when HTTP ignores abort", async () => {
  const pending = deferred(); const other = { ...run("completed"), id: "other" };
  const ui = browser(({ url }) => {
    if (url === "/api/runs/other") return json(other);
    if (url.includes("/runs/r/ag-ui?")) return json({ detail: "Unavailable" }, 404);
    if (url.includes("/events/page?")) return pending.promise;
    if (url.includes("/runs/other/ag-ui?")) return sse([snapshot(other), custom(businessEvent(500, "other")), done("other")]);
  });
  await select(ui); await until(() => pageCalls(ui).length === 1);
  const old = pageCalls(ui)[0]; ui.production.state.runs.push(other); ui.$("run-selector").value = "other";
  await ui.$("run-selector").emit("change");
  // A server upgrade can make SSE available again for the next selection.
  await ui.$("retry-stream").emit("click"); await until(() => ui.production.state.eventAfter === 500);
  pending.resolve(json(eventPage(events(1, 100), true, 0, true))); await flush(); await flush();
  assert.equal(old.signal.aborted, true); assert.equal(ui.production.state.eventAfter, 500);
  assert.deepEqual(visibleIds(ui), [500]); assert.equal(ui.production.state.run.id, "other"); assert.equal(ui.production.state.eventProducerActive, false);
});

test("resume invalidates an old generation's pending page without losing the new stream cursor", async () => {
  const pending = deferred(); let streams = 0; const failed = run("failed"); const completed = run("completed");
  const ui = browser(({ url }) => {
    if (url === "/api/runs/r") return json(failed);
    if (url === "/api/runs/r/resume") return json(run("running"));
    if (url.includes("/events/page?")) return pending.promise;
    if (url.includes("/ag-ui?")) return ++streams === 1 ? json({ detail: "Unavailable" }, 404) : sse([snapshot(completed), custom(businessEvent(20)), done()]);
  });
  await select(ui, failed); await until(() => pageCalls(ui).length === 1);
  await ui.$("resume-run").emit("click"); await until(() => ui.production.state.streamStatus === "ended");
  pending.resolve(json(eventPage(events(1, 10), false, 0, true))); await flush(); await flush();
  assert.equal(pageCalls(ui)[0].signal.aborted, true); assert.equal(ui.production.state.eventAfter, 20);
  assert.deepEqual(visibleIds(ui), [20]); assert.equal(ui.production.state.run.status, "completed"); assert.equal(ui.production.state.eventProducerActive, false);
});

test("SSE reconnect deduplicates replay, keeps the newest DOM rows, and resumes a transport error from its confirmed cursor", async () => {
  let subscriptions = 0; const active = run("running");
  const ui = browser(({ url, headers }) => {
    if (url === "/api/runs/r") return json(active);
    if (url.includes("/ag-ui?")) {
      if (++subscriptions === 1) return sse([snapshot(active), ...events(1, 250).map((item) => custom(item)), encode({ type: "RUN_ERROR", code: "stream_unavailable", message: "Subscription interrupted" })]);
      assert.match(url, /after=250$/); assert.equal(headers.get("Last-Event-ID"), "250");
      return sse([...events(240, 455).map((item) => custom(item)), snapshot(run("completed")), done()]);
    }
  });
  await select(ui, active); await until(() => ui.production.state.streamStatus === "reconnecting");
  assert.equal(ui.production.state.run.status, "running"); assert.ok(ui.production.state.streamTimer);
  assert.equal(ui.production.state.eventAfter, 250); assert.deepEqual(visibleIds(ui), events(51, 250).map((item) => item.id));
  await ui.$("retry-stream").emit("click"); await until(() => ui.production.state.streamStatus === "ended");
  assert.equal(ui.production.state.eventAfter, 455); assert.equal(ui.production.state.events.length, 200);
  assert.deepEqual(visibleIds(ui), events(256, 455).map((item) => item.id));
  assert.equal(subscriptions, 2); assert.equal(ui.production.state.pollingError, "");
});

test("a mismatched SSE cursor or foreign business event reconnects without advancing the confirmed cursor", async () => {
  for (const invalid of [custom(businessEvent(2), 999), custom(businessEvent(2, "foreign"))]) {
    const ui = browser(({ url }) => {
      if (url.includes("/ag-ui?")) return sse([snapshot(run("running")), custom(businessEvent(1)), invalid, done()]);
    });
    await select(ui); await until(() => ui.production.state.streamStatus === "reconnecting");
    assert.equal(ui.production.state.eventAfter, 1); assert.deepEqual(visibleIds(ui), [1]);
    assert.match(ui.production.state.pollingError, /游标无效/);
  }
});

test("background transport errors retain their subscription and cursor, then completion releases project locks", async () => {
  const active = { ...run("running"), id: "background" }; let reads = 0;
  const ui = browser(({ url, headers }) => {
    if (url === "/api/projects/p/runs/page?limit=20") return json({ items: [run("completed")], next_cursor: null, has_more: false, active_run: active });
    if (url.includes("/runs/r/ag-ui?")) return sse([snapshot(run("completed")), done()]);
    if (url.includes("/runs/background/ag-ui?")) {
      if (++reads === 1) return sse([snapshot(active), custom(businessEvent(7, active.id)), encode({ type: "RUN_ERROR", code: "stream_unavailable" })]);
      assert.match(url, /after=7$/); assert.equal(headers.get("Last-Event-ID"), "7");
      return sse([snapshot({ ...run("completed"), id: active.id, report: null }), done(active.id)]);
    }
    if (url === "/api/runs/background") throw new Error("Background full report read forbidden");
  });
  await ui.production.selectProject(project);
  await until(() => ui.production.state.backgroundStreams.get(active.id)?.status === "reconnecting");
  const stream = ui.production.state.backgroundStreams.get(active.id);
  assert.equal(stream.cursor, 7); assert.equal(ui.$("edit-project").disabled, true); assert.ok(stream.timer);
  await ui.timers.get(stream.timer)(); await until(() => ui.production.state.history.activeId === null);
  assert.equal(ui.production.state.backgroundStreams.size, 0); assert.equal(ui.$("edit-project").disabled, false);
  assert.equal(ui.$("start-run").disabled, false); assert.equal(reads, 2);
});

test("overlapping fallback polls share the same pending event-page request", async () => {
  const pending = deferred();
  const ui = browser(({ url }) => { if (url.includes("/events/page?")) return pending.promise; });
  Object.assign(ui.production.state, { run: run("completed"), runs: [run("completed")], streamSupported: false });
  const first = ui.production.pollRun(ui.production.state.epoch); await until(() => pageCalls(ui).length === 1);
  const second = ui.production.pollRun(ui.production.state.epoch); await flush(); assert.equal(pageCalls(ui).length, 1);
  pending.resolve(json(eventPage(events(1, 5)))); await Promise.all([first, second]);
  assert.equal(ui.production.state.eventAfter, 5); assert.deepEqual(visibleIds(ui), [1, 2, 3, 4, 5]);
});
