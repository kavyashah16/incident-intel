// k6 load test for POST /v1/events.
// Usage (from the project root):
//   k6 run -e API_KEY=ms_xxx -e RATE=2000 -e DURATION=60s load/ingest.js
// RATE is events per second; each request carries BATCH events.
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const URL = __ENV.URL || "http://localhost:8080/v1/events";
const API_KEY = __ENV.API_KEY;
const RATE = Number(__ENV.RATE || 1000); // events per second
const BATCH = Number(__ENV.BATCH || 50); // events per request
const DURATION = __ENV.DURATION || "30s";
const REQ_RATE = Math.max(1, Math.round(RATE / BATCH));

const accepted = new Counter("events_accepted");

export const options = {
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: REQ_RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 400,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<250"],
  },
};

const hex = (n) => {
  let s = "";
  for (let i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16);
  return s;
};
// A valid v4 UUID (the API validates the version and variant digits).
const uuid = () =>
  `${hex(8)}-${hex(4)}-4${hex(3)}-${"89ab"[Math.floor(Math.random() * 4)]}${hex(3)}-${hex(12)}`;

// About 20 distinct errors, so thousands of events collapse into a realistic handful of issues.
const KINDS = 20;
function makeEvent() {
  const k = Math.floor(Math.random() * KINDS);
  return {
    event_id: uuid(),
    level: k % 5 === 0 ? "fatal" : "error",
    timestamp: new Date().toISOString(),
    environment: "load-test",
    release: "1.0.0",
    exception: {
      type: `LoadTestError${k}`,
      value: `Failed to process order ${Math.floor(Math.random() * 1e6)} in handler ${k}`,
      frames: [
        {
          filename: `/app/src/handlers/h${k}.ts`,
          function: `handle${k}`,
          lineno: 10 + k,
          in_app: true,
        },
        {
          filename: "/app/node_modules/express/lib/router.js",
          function: "next",
          lineno: 200,
          in_app: false,
        },
      ],
    },
  };
}

export default function () {
  const events = [];
  for (let i = 0; i < BATCH; i++) events.push(makeEvent());
  const res = http.post(URL, JSON.stringify({ events }), {
    headers: { "content-type": "application/json", "x-api-key": API_KEY },
  });
  const ok = check(res, { "status is 202": (r) => r.status === 202 });
  if (ok) accepted.add(JSON.parse(res.body).accepted.length);
}

// Written next to where you ran k6; scripts/verify.ts reads it to check for loss.
export function handleSummary(data) {
  const count = (m) => (data.metrics[m] ? data.metrics[m].values.count : 0);
  const dur = data.metrics.http_req_duration
    ? data.metrics.http_req_duration.values
    : {};
  const elapsedMs = data.state ? data.state.testRunDurationMs : 0;
  const result = {
    // Lets verify.ts count only this run's events, so you can reuse one project for many runs.
    started_at: new Date(Date.now() - elapsedMs - 5000).toISOString(),
    accepted: count("events_accepted"),
    requests: count("http_reqs"),
    failed_rate: data.metrics.http_req_failed
      ? data.metrics.http_req_failed.values.rate
      : 0,
    avg_ms: dur.avg ?? null,
    p95_ms: dur["p(95)"] ?? null,
    max_ms: dur.max ?? null,
    target_events_per_sec: RATE,
  };
  const text =
    `\nevents accepted: ${result.accepted}  (target ${RATE}/s, achieved ${(result.accepted / (elapsedMs / 1000)).toFixed(0)}/s)\n` +
    `requests: ${result.requests}   failed: ${(result.failed_rate * 100).toFixed(2)}%\n` +
    `latency ms: avg ${result.avg_ms?.toFixed(1)}  p95 ${result.p95_ms?.toFixed(1)}  max ${result.max_ms?.toFixed(1)}\n`;
  return {
    "load-result.json": JSON.stringify(result, null, 2) + "\n",
    stdout: text,
  };
}
