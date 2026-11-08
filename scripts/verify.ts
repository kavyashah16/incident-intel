/**
 * After a k6 run: waits for the consumer to drain, then checks nothing was lost or double counted.
 * Usage: pnpm exec tsx scripts/verify.ts <project_id>
 * Expects load-result.json (written by k6) in the current directory.
 */
import { readFileSync } from "node:fs";
import pg from "pg";

const databaseUrl =
  process.env.DATABASE_URL ??
  "postgres://postgres:postgres@localhost:5432/incidents";
const projectId = process.argv[2];
if (!projectId) {
  console.error("usage: pnpm exec tsx scripts/verify.ts <project_id>");
  process.exit(1);
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const result = JSON.parse(readFileSync("load-result.json", "utf8")) as {
    accepted: number;
    started_at: string;
  };
  const expected = result.accepted;
  const since = result.started_at;
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });
  const num = async (sql: string, ...extra: unknown[]): Promise<number> =>
    Number((await pool.query(sql, [projectId, ...extra])).rows[0].n);

  const started = Date.now();
  let count = 0;
  let last = -1;
  let lastChange = Date.now();
  // Done when every accepted event is stored, or when the count has stopped moving for 20 s.
  while (true) {
    // Only events received since this run started, so one project can be reused across runs.
    count = await num(
      "SELECT count(*) AS n FROM events WHERE project_id = $1 AND received_at >= $2",
      since,
    );
    if (count >= expected) break;
    if (count !== last) {
      last = count;
      lastChange = Date.now();
    } else if (Date.now() - lastChange > 20_000) break;
    process.stdout.write(`\rstored ${count} / ${expected}   `);
    await sleep(1000);
  }
  const drainSeconds = (Date.now() - started) / 1000;
  console.log(`\rstored ${count} / ${expected}   `);

  // Counters accumulate across runs, so they are compared with ALL events in the project.
  const totalEvents = await num(
    "SELECT count(*) AS n FROM events WHERE project_id = $1",
  );
  const issueTotal = await num(
    "SELECT coalesce(sum(event_count), 0) AS n FROM issues WHERE project_id = $1",
  );
  const statsTotal = await num(
    "SELECT coalesce(sum(count), 0) AS n FROM issue_stats_5m WHERE project_id = $1",
  );
  const issues = await num(
    "SELECT count(*) AS n FROM issues WHERE project_id = $1",
  );
  const dupes = await num(
    "SELECT count(*) AS n FROM (SELECT event_id FROM events WHERE project_id = $1 GROUP BY event_id HAVING count(*) > 1) d",
  );
  await pool.end();

  const checks: [string, boolean, string][] = [
    [
      "no events lost",
      count === expected,
      `${count} stored of ${expected} accepted in this run`,
    ],
    ["no duplicate event ids", dupes === 0, `${dupes} duplicated`],
    [
      "issue counters match events",
      issueTotal === totalEvents,
      `sum(event_count) ${issueTotal} vs ${totalEvents} events in project`,
    ],
    [
      "5-minute stats match events",
      statsTotal === totalEvents,
      `sum(stats) ${statsTotal} vs ${totalEvents} events in project`,
    ],
  ];
  for (const [name, ok, detail] of checks)
    console.log(`${ok ? "PASS" : "FAIL"}  ${name}  (${detail})`);
  console.log(
    `\n${issues} issues, drained in ${drainSeconds.toFixed(1)} s after the load stopped`,
  );
  process.exit(checks.every((c) => c[1]) ? 0 : 1);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
