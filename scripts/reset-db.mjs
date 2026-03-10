import fs from "node:fs";
import path from "node:path";

const databasePath = process.env.BID_CONSOLE_DB_PATH?.trim()
  ? path.resolve(process.env.BID_CONSOLE_DB_PATH)
  : path.join(process.cwd(), "data", "bid-investigation-console.sqlite");

for (const filePath of [databasePath, `${databasePath}-wal`, `${databasePath}-shm`]) {
  if (fs.existsSync(filePath)) {
    fs.rmSync(filePath);
    process.stdout.write(`Removed ${filePath}\n`);
  }
}
