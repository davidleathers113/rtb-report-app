import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { afterEach, describe, expect, it, vi } from "vitest";

import { getDb, resetDbClientForTests } from "@/lib/db/client";
import {
  createImportSourceFile,
  getImportSourceRowForBidId,
  insertImportSourceRows,
} from "@/lib/db/import-sources";
import { importRunItems, importRuns, importSourceRows } from "@/lib/db/schema";
import { nowIso } from "@/lib/db/utils";
import {
  buildCsvDirectPreview,
  createImportRunFromCsvDirectUpload,
  previewCsvDirectUpload,
} from "@/lib/import-runs/csv-direct";
import { buildCsvDiagnosis } from "@/lib/diagnostics/csv-direct";

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

describe("csv direct import parsing", () => {
  it("summarizes rows and normalizes dates", () => {
    const csvText = [
      "Campaign,Publisher,Bid Date,Bid ID,Bid,Winning Bid,Reason for Rejection,Bid Rejected",
      "Alpha,Publisher A,03/05/2026 01:08:34 PM,RTB123,10.5,12.0,,False",
      "Beta,Publisher B,03/05/2026 01:09:34 PM,RTB456,0,0,Zero bid,True",
    ].join("\n");

    const preview = buildCsvDirectPreview({ csvText, fileName: "sample.csv" });

    expect(preview.totalRows).toBe(2);
    expect(preview.validBidIdCount).toBe(2);
    expect(preview.duplicateBidIdCount).toBe(0);
    expect(preview.missingBidIdCount).toBe(0);
    expect(preview.earliestBidDt?.endsWith("Z")).toBe(true);
    expect(preview.latestBidDt?.endsWith("Z")).toBe(true);
    expect(preview.sampleRows[0]?.bidId).toBe("RTB123");
  });

  it("flags invalid bid ids", () => {
    const csvText = [
      "Bid ID,Bid Date",
      "RTB123,03/05/2026 01:08:34 PM",
      "bad;value,03/05/2026 01:08:34 PM",
      ",03/05/2026 01:08:34 PM",
    ].join("\n");

    const preview = buildCsvDirectPreview({ csvText, fileName: "sample.csv" });

    expect(preview.invalidBidIdCount).toBe(1);
    expect(preview.missingBidIdCount).toBe(1);
    expect(preview.invalidRows[0]?.value).toBe("bad;value");
  });
});

describe("csv direct diagnosis", () => {
  it("classifies capacity rejections as no eligible targets", () => {
    const diagnosis = buildCsvDiagnosis({
      normalizedBid: {
        bidId: "RTB123",
        bidDt: "2026-03-05T00:00:00.000Z",
        campaignName: "Campaign",
        campaignId: "campaign-1",
        publisherName: "Publisher",
        publisherId: "publisher-1",
        targetName: null,
        targetId: null,
        buyerName: null,
        buyerId: null,
        bidAmount: 10,
        winningBid: 12,
        bidElapsedMs: null,
        isZeroBid: false,
        reasonForReject: "Final capacity check (Code: 1006)",
        httpStatusCode: null,
        errorMessage: null,
        primaryFailureStage: "target_rejected",
        primaryTargetName: null,
        primaryTargetId: null,
        primaryBuyerName: null,
        primaryBuyerId: null,
        primaryErrorCode: null,
        primaryErrorMessage: "Final capacity check (Code: 1006)",
        requestBody: null,
        responseBody: null,
        rawTraceJson: {},
        relevantEvents: [],
        targetAttempts: [],
        outcome: "rejected",
      },
      sourceRow: {
        id: "row-1",
        importSourceFileId: "file-1",
        importRunId: "run-1",
        rowNumber: 2,
        bidId: "RTB123",
        bidDt: "2026-03-05T00:00:00.000Z",
        campaignName: "Campaign",
        campaignId: "campaign-1",
        publisherName: "Publisher",
        publisherId: "publisher-1",
        bidAmount: 10,
        winningBid: 12,
        bidRejected: true,
        reasonForReject: "Final capacity check (Code: 1006)",
        bidDid: null,
        bidExpireDate: null,
        expirationSeconds: null,
        winningBidCallAccepted: null,
        winningBidCallRejected: null,
        bidElapsedMs: null,
        rowJson: {},
        createdAt: "2026-03-05T00:00:00.000Z",
        updatedAt: "2026-03-05T00:00:00.000Z",
      },
    });

    expect(diagnosis.rootCause).toBe("no_eligible_targets");
    expect(diagnosis.ownerType).toBe("ringba_config");
  });
});

describe("csv direct persistence", () => {
  it("stores and retrieves source rows by bid id", async () => {
    await setupTestDatabase();
    const importRunId = "run-1";
    const db = getDb();
    const now = nowIso();
    db.insert(importRuns)
      .values({
        id: importRunId,
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
    const sourceFile = await createImportSourceFile({
      importRunId,
      sourceType: "csv_direct_import",
      fileName: "file.csv",
      rowCount: 1,
      headerJson: ["Bid ID"],
    });

    await insertImportSourceRows({
      importRunId,
      importSourceFileId: sourceFile.id,
      rows: [
        {
          rowNumber: 2,
          bidId: "RTB123",
          bidDt: "2026-03-05T00:00:00.000Z",
          campaignName: "Campaign",
          campaignId: "campaign-1",
          publisherName: "Publisher",
          publisherId: "publisher-1",
          bidAmount: 10,
          winningBid: 12,
          bidRejected: true,
          reasonForReject: "Rejected",
          bidDid: null,
          bidExpireDate: null,
          expirationSeconds: null,
          winningBidCallAccepted: null,
          winningBidCallRejected: null,
          bidElapsedMs: null,
          rowJson: { "Bid ID": "RTB123" },
        },
      ],
    });

    const row = await getImportSourceRowForBidId({
      importRunId,
      bidId: "RTB123",
    });

    expect(row?.bidId).toBe("RTB123");
    expect(row?.campaignName).toBe("Campaign");
  });
});

describe("csv direct streaming import", () => {
  it("builds preview summaries from streamed files", async () => {
    const file = new File(
      [
        [
          "Campaign,Publisher,Bid Date,Bid ID,Bid,Winning Bid,Reason for Rejection,Bid Rejected",
          "Alpha,Publisher A,03/05/2026 01:08:34 PM,RTB123,10.5,12.0,,False",
          "Beta,Publisher B,03/05/2026 01:09:34 PM,RTB123,0,0,Zero bid,True",
          "Gamma,Publisher C,03/05/2026 01:10:34 PM,,0,0,Missing,True",
        ].join("\n"),
      ],
      "preview.csv",
      { type: "text/csv" },
    );

    const preview = await previewCsvDirectUpload({ file });

    expect(preview.fileName).toBe("preview.csv");
    expect(preview.totalRows).toBe(3);
    expect(preview.validBidIdCount).toBe(2);
    expect(preview.duplicateBidIdCount).toBe(1);
    expect(preview.missingBidIdCount).toBe(1);
    expect(preview.sampleRows.length).toBe(3);
  });

  it("creates run items and source rows from a file", async () => {
    await setupTestDatabase();
    const file = new File(
      [
        [
          "Campaign,Publisher,Bid Date,Bid ID,Bid,Winning Bid,Reason for Rejection,Bid Rejected",
          "Alpha,Publisher A,03/05/2026 01:08:34 PM,RTB123,10.5,12.0,,False",
          "Beta,Publisher B,03/05/2026 01:09:34 PM,RTB456,0,0,Zero bid,True",
        ].join("\n"),
      ],
      "sample.csv",
      { type: "text/csv" },
    );

    const result = await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: false,
    });

    const db = getDb();
    const rows = db.select().from(importSourceRows).all();
    const items = db.select().from(importRunItems).all();

    expect(result.importRun.id).toBeDefined();
    expect(rows.length).toBe(2);
    expect(items.length).toBe(2);
  });

  it("tracks valid rows separately from deduped queued items", async () => {
    await setupTestDatabase();
    const file = new File(
      [
        [
          "Campaign,Publisher,Bid Date,Bid ID,Bid,Winning Bid,Reason for Rejection,Bid Rejected",
          "Alpha,Publisher A,03/05/2026 01:08:34 PM,RTB123,10.5,12.0,,False",
          "Beta,Publisher B,03/05/2026 01:09:34 PM,RTB123,0,0,Zero bid,True",
        ].join("\n"),
      ],
      "dedupe.csv",
      { type: "text/csv" },
    );

    const result = await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: false,
    });

    expect(result.preview.validBidIdCount).toBe(2);
    expect(result.importRun.totalItems).toBe(1);
    expect(result.importRun.sourceMetadata.validBidIdCount).toBe(2);
    expect(result.importRun.sourceMetadata.dedupedBidIdCount).toBe(1);
  });

  it("handles larger files with streaming batches", async () => {
    await setupTestDatabase();
    const rows: string[] = [
      "Campaign,Publisher,Bid Date,Bid ID,Bid,Winning Bid,Reason for Rejection,Bid Rejected",
    ];
    for (let index = 0; index < 2000; index += 1) {
      rows.push(
        `Campaign ${index},Publisher ${index},03/05/2026 01:08:34 PM,RTB${index},10,10,,False`,
      );
    }
    const file = new File([rows.join("\n")], "bulk.csv", { type: "text/csv" });

    await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: false,
    });

    const db = getDb();
    const sourceRows = db.select().from(importSourceRows).all();
    const items = db.select().from(importRunItems).all();

    expect(sourceRows.length).toBe(2000);
    expect(items.length).toBe(2000);
  });

  it("rejects header-only files", async () => {
    await setupTestDatabase();
    const file = new File(["Bid ID,Bid Date\n"], "header-only.csv", {
      type: "text/csv",
    });

    await expect(previewCsvDirectUpload({ file })).rejects.toThrow(
      "The uploaded CSV does not contain any data rows.",
    );
    await expect(
      createImportRunFromCsvDirectUpload({
        file,
        forceRefresh: false,
      }),
    ).rejects.toThrow("The uploaded CSV does not contain any data rows.");
  });

  it("enforces the direct import row limit during streamed preview", async () => {
    vi.resetModules();
    vi.doMock("@/lib/import-runs/csv-direct-constants", () => ({
      MAX_CSV_DIRECT_UPLOAD_BYTES: 25 * 1024 * 1024,
      MAX_CSV_DIRECT_ROWS: 5,
      CSV_DIRECT_CHUNK_SIZE: 1000,
    }));

    try {
      const { previewCsvDirectUpload: previewWithSmallLimit } = await import(
        "@/lib/import-runs/csv-direct"
      );

      const rows = ["Bid ID,Bid Date"];

      for (let index = 0; index < 6; index += 1) {
        rows.push(`RTB${index},03/05/2026 01:08:34 PM`);
      }

      const file = new File([rows.join("\n")], "too-many-rows.csv", {
        type: "text/csv",
      });

      await expect(previewWithSmallLimit({ file })).rejects.toThrow(
        "The uploaded CSV exceeds the 5 row limit for direct import.",
      );
    } finally {
      vi.doUnmock("@/lib/import-runs/csv-direct-constants");
      vi.resetModules();
    }
  });

  it("marks the import run as failed when parsing aborts", async () => {
    await setupTestDatabase();
    const file = new File(
      ["Campaign,Publisher,Bid Date\nAlpha,Publisher A,03/05/2026 01:08:34 PM\n"],
      "missing-bid-id.csv",
      { type: "text/csv" },
    );

    await expect(
      createImportRunFromCsvDirectUpload({
        file,
        forceRefresh: false,
      }),
    ).rejects.toThrow("The uploaded CSV file does not include a Bid ID column.");

    const run = getDb().select().from(importRuns).get() as
      | {
          status: string;
          sourceStage: string;
          lastError: string | null;
          exportDownloadStatus: string | null;
        }
      | undefined;

    expect(run?.status).toBe("failed");
    expect(run?.sourceStage).toBe("failed");
    expect(run?.exportDownloadStatus).toBe("failed");
    expect(run?.lastError).toBe("The uploaded CSV file does not include a Bid ID column.");
  });
});
