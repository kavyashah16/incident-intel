/**
 * Sends a few requests to a running ingest API and prints what came back, so you can see
 * each behaviour: accepted, unauthorized, rejected and partially accepted.
 * Usage: pnpm exec tsx scripts/send-event.ts <api-key>
 */
const arg = process.argv[2];
if (!arg) {
  console.error("usage: pnpm exec tsx scripts/send-event.ts <api-key>");
  process.exit(1);
}
const apiKey: string = arg;
const url = process.env.INGEST_URL ?? "http://localhost:8080/v1/events";

const goodEvent = () => ({
  level: "error",
  message: "Cannot read properties of undefined (reading 'id')",
  exception: {
    type: "TypeError",
    value: "Cannot read properties of undefined (reading 'id')",
    frames: [
      {
        filename: "src/orders.ts",
        function: "getOrder",
        lineno: 42,
        in_app: true,
      },
    ],
  },
  environment: "development",
  release: "1.0.0",
  timestamp: new Date().toISOString(),
});

async function post(
  label: string,
  headers: Record<string, string>,
  body: unknown,
): Promise<void> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  console.log(`\n${label}\n  -> HTTP ${res.status} ${await res.text()}`);
}

async function main(): Promise<void> {
  await post("1. valid event", { "x-api-key": apiKey }, goodEvent());
  await post("2. no API key", {}, goodEvent());
  await post(
    "3. wrong API key",
    { "x-api-key": "ms_00000000_" + "0".repeat(32) },
    goodEvent(),
  );
  await post(
    "4. invalid event (no message or exception)",
    { "x-api-key": apiKey },
    { level: "error" },
  );
  await post(
    "5. batch: one good, one bad level",
    { "x-api-key": apiKey },
    {
      events: [goodEvent(), { message: "x", level: "loud" }],
    },
  );
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
