import path from "node:path";

import { defineConfig } from "drizzle-kit";

const defaultDbPath = path.join(
  process.cwd(),
  "data",
  "bid-investigation-console.sqlite",
);

export default defineConfig({
  schema: "./lib/db/schema/*.ts",
  out: "./drizzle",
  dialect: "sqlite",
  strict: true,
  verbose: true,
  dbCredentials: {
    url: process.env.BID_CONSOLE_DB_PATH?.trim()
      ? path.resolve(process.env.BID_CONSOLE_DB_PATH)
      : defaultDbPath,
  },
});
