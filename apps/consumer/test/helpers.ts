import { randomBytes } from "node:crypto";
import pg from "pg";
import { uuidv7, type Envelope } from "@ii/shared";
import { migrate } from "../../../scripts/migrate";

/** Connection used only to create and drop throwaway test databases. */
const adminUrl =
  process.env.TEST_ADMIN_DATABASE_URL ??
  "postgres://postgres:postgres@localhost:5432/postgres";

export interface TestDb {
  pool: pg.Pool;
  projectId: string;
  addProject(): Promise<string>;
  close(): Promise<void>;
}

/** A fresh, fully migrated database with one tenant and one project. Never touches `incidents`. */
export async function createTestDb(): Promise<TestDb> {
  const name = `ii_test_${randomBytes(4).toString("hex")}`;
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.end();

  const url = new URL(adminUrl);
  url.pathname = `/${name}`;
  await migrate(url.toString());

  const pool = new pg.Pool({ connectionString: url.toString(), max: 4 });
  const tenantId = uuidv7();
  await pool.query("INSERT INTO tenants (id, name) VALUES ($1, 'test')", [
    tenantId,
  ]);
  const addProject = async (): Promise<string> => {
    const id = uuidv7();
    await pool.query(
      "INSERT INTO projects (id, tenant_id, name) VALUES ($1, $2, 'p')",
      [id, tenantId],
    );
    return id;
  };
  const projectId = await addProject();

  return {
    pool,
    projectId,
    addProject,
    async close() {
      await pool.end();
      const a = new pg.Client({ connectionString: adminUrl });
      await a.connect();
      await a.query(`DROP DATABASE ${name} WITH (FORCE)`);
      await a.end();
    },
  };
}

export const DAY = "2026-11-01";

export function makeEnvelope(
  projectId: string,
  opts: {
    message?: string;
    occurredAt?: string;
    eventId?: string;
    level?: Envelope["payload"]["level"];
  } = {},
): Envelope {
  const occurredAt = opts.occurredAt ?? `${DAY}T10:00:00.000Z`;
  return {
    schema_version: 1,
    event_id: opts.eventId ?? uuidv7(),
    project_id: projectId,
    occurred_at: occurredAt,
    received_at: occurredAt,
    payload: { level: opts.level ?? "error", message: opts.message ?? "boom" },
  };
}
