import { defineConfig } from "vitest/config";

// These tests create a throwaway database, so give slow machines room.
export default defineConfig({
  test: { testTimeout: 30_000, hookTimeout: 30_000 },
});
