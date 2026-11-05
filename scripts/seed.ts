/**
 * Creates one tenant, one project and one API key, and prints the key once.
 * Run it again any time you want another key: it creates a fresh tenant each run.
 */
import pg from "pg";
import { generateApiKey, uuidv7 } from "../packages/shared/src/index";

const databaseUrl =
  process.env.DATABASE_URL ??
  "postgres://postgres:postgres@localhost:5432/incidents";

async function main(): Promise<void> {
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    const tenantId = uuidv7();
    const projectId = uuidv7();
    const keyId = uuidv7();
    const apiKey = generateApiKey();

    await client.query("BEGIN");
    await client.query("INSERT INTO tenants (id, name) VALUES ($1, $2)", [
      tenantId,
      "Demo Tenant",
    ]);
    await client.query(
      "INSERT INTO projects (id, tenant_id, name) VALUES ($1, $2, $3)",
      [projectId, tenantId, "Demo Project"],
    );
    await client.query(
      "INSERT INTO api_keys (id, project_id, prefix, key_hash) VALUES ($1, $2, $3, $4)",
      [keyId, projectId, apiKey.prefix, apiKey.hash],
    );
    await client.query("COMMIT");

    console.log("Created demo tenant and project.");
    console.log(`project_id: ${projectId}`);
    console.log(`api key (shown once, save it): ${apiKey.key}`);
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
