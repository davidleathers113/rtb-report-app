import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { migrate } from "drizzle-orm/better-sqlite3/migrator";

const databasePath = process.env.BID_CONSOLE_DB_PATH?.trim()
  ? path.resolve(process.env.BID_CONSOLE_DB_PATH)
  : path.join(process.cwd(), "data", "bid-investigation-console.sqlite");

fs.mkdirSync(path.dirname(databasePath), { recursive: true });

const sqlite = new Database(databasePath);
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("foreign_keys = ON");
sqlite.pragma("synchronous = NORMAL");
sqlite.pragma("busy_timeout = 5000");

const db = drizzle(sqlite);

migrate(db, {
  migrationsFolder: path.join(process.cwd(), "drizzle"),
});

process.stdout.write(`Applied migrations to ${databasePath}\n`);
