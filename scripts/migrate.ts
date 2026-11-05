/**
 * Minimal forward-only migration runner: applies migrations/*.sql in filename order, each in
 * its own transaction, and records them in schema_migrations. No down migrations on purpose.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import pg from "pg";

const databaseUrl =
  process.env.DATABASE_URL ??
  "postgres://postgres:postgres@localhost:5432/incidents";
const dir = join(import.meta.dirname, "..", "migrations");

export async function migrate(
  url: string = databaseUrl,
  migrationsDir: string = dir,
): Promise<string[]> {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  const applied: string[] = [];
  try {
    await client.query(
      "CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())",
    );
    // Serialize concurrent runners (e.g. two containers booting together).
    await client.query("SELECT pg_advisory_lock(727001)");
    const done = new Set(
      (
        await client.query<{ name: string }>(
          "SELECT name FROM schema_migrations",
        )
      ).rows.map((r) => r.name),
    );
    const files = readdirSync(migrationsDir)
      .filter((f) => f.endsWith(".sql"))
      .sort();
    for (const file of files) {
      if (done.has(file)) continue;
      const sql = readFileSync(join(migrationsDir, file), "utf8");
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query("INSERT INTO schema_migrations (name) VALUES ($1)", [
          file,
        ]);
        await client.query("COMMIT");
        applied.push(file);
      } catch (err) {
        await client.query("ROLLBACK");
        throw new Error(`migration ${file} failed: ${(err as Error).message}`);
      }
    }
    await client.query("SELECT pg_advisory_unlock(727001)");
  } finally {
    await client.end();
  }
  return applied;
}

// pathToFileURL makes this check work on Windows too (a C:\ path vs a file:/// URL).
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  migrate()
    .then((applied) =>
      console.log(
        applied.length ? `applied: ${applied.join(", ")}` : "up to date",
      ),
    )
    .catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
}
