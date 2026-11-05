import { normalizeMessage, computeFingerprint } from "./src";

console.log(normalizeMessage("User 4812 not found"));
console.log(
  normalizeMessage("ENOENT: no such file '/var/app/data/u123/file.txt'"),
);

const make = (id: number, path: string) => ({
  level: "error" as const,
  exception: {
    type: "TypeError",
    value: `Cannot read properties of undefined (reading 'id') for user ${id}`,
    frames: [{ filename: path, function: "getUser", lineno: id, in_app: true }],
  },
});
const a = computeFingerprint(make(4812, "/app/src/handlers/user.js"));
const b = computeFingerprint(
  make(9930, "/srv/releases/77/src/handlers/user.js"),
);
console.log(a.fingerprint, a.title);
console.log(b.fingerprint, "same issue?", a.fingerprint === b.fingerprint);
