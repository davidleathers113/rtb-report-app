import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";

import * as schema from "@/lib/db/schema";

const DEFAULT_DB_PATH = path.join(
  process.cwd(),
  "data",
  "bid-investigation-console.sqlite",
);

let sqlite: Database.Database | null = null;
let dbInstance: ReturnType<typeof drizzle<typeof schema>> | null = null;

function ensureDataDirectory(filePath: string) {
  const directoryPath = path.dirname(filePath);
  fs.mkdirSync(directoryPath, { recursive: true });
}

function getDatabasePath() {
  const configuredPath = process.env.BID_CONSOLE_DB_PATH?.trim();
  return configuredPath ? path.resolve(configuredPath) : DEFAULT_DB_PATH;
}

function configureDatabase(database: Database.Database) {
  database.pragma("journal_mode = WAL");
  database.pragma("foreign_keys = ON");
  database.pragma("synchronous = NORMAL");
  database.pragma("busy_timeout = 5000");
}

export function getSqlite() {
  if (sqlite) {
    return sqlite;
  }

  const filePath = getDatabasePath();
  ensureDataDirectory(filePath);
  sqlite = new Database(filePath);
  configureDatabase(sqlite);
  return sqlite;
}

export function getDb() {
  if (dbInstance) {
    return dbInstance;
  }

  dbInstance = drizzle(getSqlite(), {
    schema,
  });
  return dbInstance;
}

export function getDatabaseFilePath() {
  return getDatabasePath();
}

export function resetDbClientForTests() {
  if (sqlite) {
    sqlite.close();
  }

  sqlite = null;
  dbInstance = null;
}
