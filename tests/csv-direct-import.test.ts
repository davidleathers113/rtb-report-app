import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { afterEach, describe, expect, it, vi } from "vitest";

import { POST as createDirectImportRoute } from "@/app/api/import-runs/csv-direct/route";
import { POST as previewDirectImportRoute } from "@/app/api/import-runs/csv-direct/preview/route";
import { getDb, resetDbClientForTests } from "@/lib/db/client";
import {
  createImportSourceFile,
  getImportSourceRowForBidId,
  insertImportSourceRows,
} from "@/lib/db/import-sources";
import { importRunItems, importRuns, importSourceFiles, importSourceRows } from "@/lib/db/schema";
import { nowIso } from "@/lib/db/utils";
import {
  buildCsvDirectPreview,
  createImportRunFromCsvDirectUpload,
  previewCsvDirectUpload,
} from "@/lib/import-runs/csv-direct";
import { buildCsvDiagnosis } from "@/lib/diagnostics/csv-direct";

function buildCsvFormRequest(
  url: string,
  file?: File,
  fields?: Record<string, string>,
) {
  const formData = new FormData();

  if (file) {
    formData.append("file", file);
  }

  if (fields) {
    for (const [key, value] of Object.entries(fields)) {
      formData.append(key, value);
    }
  }

  return new Request(url, {
    method: "POST",
    body: formData,
  });
}

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
    expect(preview.queuedItemCount).toBe(2);
    expect(preview.rejectedRowCount).toBe(0);
    expect(preview.duplicateBidIdCount).toBe(0);
    expect(preview.missingBidIdCount).toBe(0);
    expect(preview.earliestBidDt?.endsWith("Z")).toBe(true);
    expect(preview.latestBidDt?.endsWith("Z")).toBe(true);
    expect(preview.contentHash.length).toBe(64);
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
    expect(preview.rejectedRowCount).toBe(2);
    expect(preview.queuedItemCount).toBe(1);
    expect(preview.invalidRows[0]?.value).toBe("bad;value");
  });

  it("handles BOM-prefixed headers during preview", () => {
    const csvText = [
      "\uFEFFBid ID,Bid Date",
      "RTB123,03/05/2026 01:08:34 PM",
    ].join("\n");

    const preview = buildCsvDirectPreview({ csvText, fileName: "bom.csv" });

    expect(preview.validBidIdCount).toBe(1);
    expect(preview.headers[0]).toBe("Bid ID");
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
        outcomeReasonCategory: "tag_filtered_final",
        outcomeReasonCode: "1006",
        outcomeReasonMessage: "Final capacity check (Code: 1006)",
        classificationSource: "reason_for_reject_text",
        classificationConfidence: 0.72,
        classificationWarnings: [],
        parseStatus: "complete",
        normalizationVersion: "csv-direct-v1",
        schemaVariant: "csv_direct_row",
        normalizationConfidence: 1,
        normalizationWarnings: [],
        missingCriticalFields: [],
        missingOptionalFields: [],
        unknownEventNames: [],
        rawPathsUsed: {},
        primaryErrorCodeSource: null,
        primaryErrorCodeConfidence: null,
        primaryErrorCodeRawMatch: null,
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
        ingestStatus: "queued",
        ingestErrorCode: null,
        ingestErrorMessage: null,
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
      contentHash: "hash-1",
      rowCount: 1,
      headerJson: ["Bid ID"],
      headerMappingJson: [],
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

  it("stores header mapping, file fingerprint, and row-level outcomes", async () => {
    await setupTestDatabase();
    const file = new File(
      [
        [
          "Bid ID,Bid ID,Bid Date",
          "RTB123,ShadowA,03/05/2026 01:08:34 PM",
          "RTB123,ShadowB,03/05/2026 01:09:34 PM",
          ",ShadowC,03/05/2026 01:10:34 PM",
          "bad;value,ShadowD,03/05/2026 01:11:34 PM",
        ].join("\n"),
      ],
      "duplicate-headers.csv",
      { type: "text/csv" },
    );

    const result = await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: false,
    });

    const db = getDb();
    const persistedFileRows = db.select().from(importSourceRows).all();
    const persistedRows = db
      .select()
      .from(importSourceRows)
      .orderBy(importSourceRows.rowNumber)
      .all();
    const fileRow = db.select().from(importSourceFiles).get();

    expect(result.summary.queuedItemCount).toBe(1);
    expect(result.summary.rejectedRowCount).toBe(2);
    expect(result.summary.skippedDuplicateRowCount).toBe(1);
    expect(persistedFileRows.length).toBe(4);
    expect(persistedRows[0]?.rowJson).toEqual({
      "Bid ID": "RTB123",
      "Bid ID (2)": "ShadowA",
      "Bid Date": "03/05/2026 01:08:34 PM",
    });
    expect(persistedRows[1]?.ingestStatus).toBe("skipped_duplicate");
    expect(persistedRows[2]?.ingestStatus).toBe("rejected");
    expect(persistedRows[3]?.ingestErrorCode).toBe("invalid_bid_id");
    expect(fileRow?.contentHash).toBe(result.summary.contentHash);
    expect(fileRow?.headerMappingJson).toEqual([
      {
        columnIndex: 0,
        sourceHeader: "Bid ID",
        normalizedHeader: "bidid",
        storedKey: "Bid ID",
        mappedField: "bidId",
        duplicateIndex: 1,
      },
      {
        columnIndex: 1,
        sourceHeader: "Bid ID",
        normalizedHeader: "bidid",
        storedKey: "Bid ID (2)",
        mappedField: "bidId",
        duplicateIndex: 2,
      },
      {
        columnIndex: 2,
        sourceHeader: "Bid Date",
        normalizedHeader: "biddate",
        storedKey: "Bid Date",
        mappedField: "bidDate",
        duplicateIndex: 1,
      },
    ]);
  });
});

describe("csv direct streaming import", () => {
  it("builds preview summaries from streamed files", async () => {
    await setupTestDatabase();
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
    expect(preview.queuedItemCount).toBe(1);
    expect(preview.rejectedRowCount).toBe(1);
    expect(preview.skippedDuplicateRowCount).toBe(1);
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
    expect(result.summary.queuedItemCount).toBe(2);
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
    expect(result.importRun.sourceMetadata.queuedItemCount).toBe(1);
    expect(result.importRun.sourceMetadata.skippedDuplicateRowCount).toBe(1);
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
    await setupTestDatabase();
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

  it("surfaces duplicate uploads during preview and blocks create by default", async () => {
    await setupTestDatabase();
    const file = new File(
      ["Bid ID,Bid Date\nRTB123,03/05/2026 01:08:34 PM\n"],
      "duplicate.csv",
      { type: "text/csv" },
    );

    const firstResult = await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: false,
    });

    const preview = await previewCsvDirectUpload({ file });

    expect(preview.duplicateImport?.importRunId).toBe(firstResult.importRun.id);
    await expect(
      createImportRunFromCsvDirectUpload({
        file,
        forceRefresh: false,
      }),
    ).rejects.toThrow("This CSV was already imported");
  });
});

describe("csv direct import routes", () => {
  it("returns 422 with an error code for invalid preview requests", async () => {
    await setupTestDatabase();
    const file = new File(["Campaign\nAlpha\n"], "missing-bid-id.csv", {
      type: "text/csv",
    });

    const response = await previewDirectImportRoute(
      buildCsvFormRequest("http://localhost/api/import-runs/csv-direct/preview", file),
    );
    const payload = (await response.json()) as { error: string; code?: string };

    expect(response.status).toBe(422);
    expect(payload.code).toBe("csv_direct_missing_bid_id_column");
  });

  it("returns a structured summary payload for successful create requests", async () => {
    await setupTestDatabase();
    const file = new File(
      ["Bid ID,Bid Date\nRTB123,03/05/2026 01:08:34 PM\n"],
      "route-success.csv",
      { type: "text/csv" },
    );

    const response = await createDirectImportRoute(
      buildCsvFormRequest("http://localhost/api/import-runs/csv-direct", file, {
        forceRefresh: "false",
      }),
    );
    const payload = (await response.json()) as {
      summary: { queuedItemCount: number };
      importRun: { id: string };
    };

    expect(response.status).toBe(202);
    expect(payload.summary.queuedItemCount).toBe(1);
    expect(payload.importRun.id).toBeDefined();
  });

  it("returns 409 for duplicate uploads unless allowDuplicate is set", async () => {
    await setupTestDatabase();
    const file = new File(
      ["Bid ID,Bid Date\nRTB123,03/05/2026 01:08:34 PM\n"],
      "route-duplicate.csv",
      { type: "text/csv" },
    );

    await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: false,
    });

    const conflictResponse = await createDirectImportRoute(
      buildCsvFormRequest("http://localhost/api/import-runs/csv-direct", file, {
        forceRefresh: "false",
      }),
    );
    const conflictPayload = (await conflictResponse.json()) as {
      error: string;
      code?: string;
    };

    expect(conflictResponse.status).toBe(409);
    expect(conflictPayload.code).toBe("csv_direct_duplicate_upload");

    const allowedResponse = await createDirectImportRoute(
      buildCsvFormRequest("http://localhost/api/import-runs/csv-direct", file, {
        forceRefresh: "false",
        allowDuplicate: "true",
      }),
    );

    expect(allowedResponse.status).toBe(202);
  });
});
