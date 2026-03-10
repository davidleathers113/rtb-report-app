import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { afterEach, describe, expect, it } from "vitest";

import { getDb, resetDbClientForTests } from "@/lib/db/client";
import { createImportSourceFile, insertImportSourceRows, listImportSourceRows } from "@/lib/db/import-sources";
import { importRuns } from "@/lib/db/schema";
import { nowIso } from "@/lib/db/utils";
import { GET } from "@/app/api/import-source-rows/route";

async function setupTestDatabase() {
  const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "bid-console-"));
  const databasePath = path.join(tempDirectory, "test.sqlite");
  process.env.BID_CONSOLE_DB_PATH = databasePath;
  resetDbClientForTests();
  migrate(getDb(), {
    migrationsFolder: path.join(process.cwd(), "drizzle"),
  });

  return { databasePath };
}

afterEach(() => {
  const databasePath = process.env.BID_CONSOLE_DB_PATH;
  resetDbClientForTests();
  if (databasePath) {
    for (const filePath of [databasePath, `${databasePath}-wal`, `${databasePath}-shm`]) {
      if (fs.existsSync(filePath)) {
        fs.rmSync(filePath);
      }
    }

    const directoryPath = path.dirname(databasePath);
    if (fs.existsSync(directoryPath)) {
      fs.rmSync(directoryPath, { recursive: true, force: true });
    }
  }

  delete process.env.BID_CONSOLE_DB_PATH;
});

async function seedSourceRows() {
  await setupTestDatabase();
  const db = getDb();
  const now = nowIso();
  const runId = "run-1";
  db.insert(importRuns)
    .values({
      id: runId,
      sourceType: "csv_direct_import",
      status: "queued",
      sourceStage: "queued",
      forceRefresh: false,
      totalFound: 0,
      totalProcessed: 0,
      exportRowCount: 0,
      sourceMetadata: {},
      createdAt: now,
      updatedAt: now,
    })
    .run();

  const file = await createImportSourceFile({
    importRunId: runId,
    sourceType: "csv_direct_import",
    fileName: "3-5-26.csv",
    rowCount: 2,
    headerJson: ["Bid ID"],
  });

  await insertImportSourceRows({
    importRunId: runId,
    importSourceFileId: file.id,
    rows: [
      {
        rowNumber: 2,
        bidId: "RTB123",
        bidDt: "2026-03-05T01:00:00.000Z",
        campaignName: "Campaign A",
        campaignId: "campaign-a",
        publisherName: "Publisher A",
        publisherId: "publisher-a",
        bidAmount: 10,
        winningBid: 12,
        bidRejected: true,
        reasonForReject: "Final capacity check",
        bidDid: null,
        bidExpireDate: null,
        expirationSeconds: null,
        winningBidCallAccepted: null,
        winningBidCallRejected: null,
        bidElapsedMs: null,
        rowJson: { "Bid ID": "RTB123" },
      },
      {
        rowNumber: 3,
        bidId: "RTB456",
        bidDt: "2026-03-05T02:00:00.000Z",
        campaignName: "Campaign B",
        campaignId: "campaign-b",
        publisherName: "Publisher B",
        publisherId: "publisher-b",
        bidAmount: 5,
        winningBid: 0,
        bidRejected: true,
        reasonForReject: "Zero bid",
        bidDid: null,
        bidExpireDate: null,
        expirationSeconds: null,
        winningBidCallAccepted: null,
        winningBidCallRejected: null,
        bidElapsedMs: null,
        rowJson: { "Bid ID": "RTB456" },
      },
    ],
  });
}

describe("import source rows queries", () => {
  it("filters rows by bid id and date range", async () => {
    await seedSourceRows();
    const result = await listImportSourceRows({
      bidId: "RTB123",
      startBidDt: "2026-03-05T00:00:00.000Z",
      endBidDt: "2026-03-05T23:59:59.999Z",
      limit: 10,
      offset: 0,
    });

    expect(result.total).toBe(1);
    expect(result.items[0]?.bidId).toBe("RTB123");
  });

  it("returns API payload with files and rows", async () => {
    await seedSourceRows();
    const request = new Request(
      "http://localhost/api/import-source-rows?limit=10&offset=0&fileName=3-5-26.csv",
    );
    const response = await GET(request);
    const payload = (await response.json()) as {
      items: Array<{ bidId: string }>;
      files: Array<{ fileName: string }>;
      total: number;
      limit: number;
      offset: number;
    };

    expect(response.status).toBe(200);
    expect(payload.items.length).toBe(2);
    expect(payload.files[0]?.fileName).toBe("3-5-26.csv");
    expect(payload.total).toBe(2);
    expect(payload.limit).toBe(10);
  });
});
