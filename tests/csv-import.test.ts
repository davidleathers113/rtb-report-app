import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/import-runs/service", () => ({
  createAsyncImportRun: vi.fn(),
}));

import {
  createImportRunFromCsvUpload,
  extractBidIdsFromCsv,
} from "@/lib/import-runs/csv";
import { createAsyncImportRun } from "@/lib/import-runs/service";
import type { ImportRunDetail } from "@/types/import-run";

function buildRun(overrides: Partial<ImportRunDetail> = {}): ImportRunDetail {
  return {
    id: "run-1",
    sourceType: "csv_upload",
    triggerType: "manual",
    scheduleId: null,
    sourceStage: "queued",
    status: "queued",
    forceRefresh: false,
    notes: null,
    lastError: null,
    sourceWindowStart: null,
    sourceWindowEnd: null,
    exportJobId: null,
    exportRowCount: 0,
    exportDownloadStatus: null,
    sourceMetadata: {},
    startedAt: "2026-03-09T00:00:00.000Z",
    completedAt: null,
    createdAt: "2026-03-09T00:00:00.000Z",
    updatedAt: "2026-03-09T00:00:00.000Z",
    totalItems: 2,
    queuedCount: 2,
    runningCount: 0,
    completedCount: 0,
    reusedCount: 0,
    fetchedCount: 0,
    failedCount: 0,
    percentComplete: 0,
    items: [],
    ...overrides,
  };
}

describe("CSV import parsing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("auto-detects common bid id headers", () => {
    const result = extractBidIdsFromCsv({
      csvText: "Bid ID,Campaign\nbid-1,Alpha\nbid-2,Beta\n",
      fileName: "bids.csv",
    });

    expect(result.headerDetected).toBe(true);
    expect(result.bidIds).toEqual(["bid-1", "bid-2"]);
    expect(result.selectedColumnKey).toBe("column_0");
  });

  it("falls back to first-column parsing when no header is detected", () => {
    const result = extractBidIdsFromCsv({
      csvText: "bid-1,Alpha\nbid-2,Beta\n",
      fileName: "bids.csv",
    });

    expect(result.headerDetected).toBe(false);
    expect(result.bidIds).toEqual(["bid-1", "bid-2"]);
  });

  it("allows selecting a non-standard header column", () => {
    const result = extractBidIdsFromCsv({
      csvText: "Campaign,TraceId,Notes\nAlpha,bid-1,ok\nBeta,bid-2,ok\n",
      fileName: "bids.csv",
      selectedColumnKey: "column_1",
    });

    expect(result.headerDetected).toBe(true);
    expect(result.bidIds).toEqual(["bid-1", "bid-2"]);
    expect(result.columnOptions.map((option) => option.label)).toEqual([
      "Campaign",
      "TraceId",
      "Notes",
    ]);
  });

  it("dedupes values and reports invalid rows", () => {
    const result = extractBidIdsFromCsv({
      csvText: "bid_id\nbid-1\nbid-1\nbad value\nbid-2\n",
      fileName: "bids.csv",
    });

    expect(result.bidIds).toEqual(["bid-1", "bid-2"]);
    expect(result.duplicateCount).toBe(1);
    expect(result.invalidRows).toEqual([
      {
        rowNumber: 4,
        value: "bad value",
        message: "This value does not look like a valid Bid ID.",
      },
    ]);
  });

  it("creates an import run from parsed CSV values", async () => {
    vi.mocked(createAsyncImportRun).mockResolvedValue(buildRun());

    const file = new File(
      ['bid_id\nbid-1\nbid-1\nbid-2\n'],
      "bids.csv",
      {
        type: "text/csv",
      },
    );

    const result = await createImportRunFromCsvUpload({
      file,
      forceRefresh: true,
    });

    expect(createAsyncImportRun).toHaveBeenCalledWith({
      bidIds: ["bid-1", "bid-2"],
      forceRefresh: true,
      sourceType: "csv_upload",
      notes: "CSV upload import from bids.csv.",
    });
    expect(result.importRun.id).toBe("run-1");
  });

  it("keeps the legacy CSV flow scoped to bid id extraction only", async () => {
    vi.mocked(createAsyncImportRun).mockResolvedValue(buildRun());

    const file = new File(
      [
        [
          "Campaign,TraceId,Notes",
          "Alpha,bid-1,first",
          "Beta,bid-2,second",
        ].join("\n"),
      ],
      "trace-export.csv",
      {
        type: "text/csv",
      },
    );

    await createImportRunFromCsvUpload({
      file,
      selectedColumnKey: "column_1",
      forceRefresh: false,
    });

    expect(createAsyncImportRun).toHaveBeenLastCalledWith({
      bidIds: ["bid-1", "bid-2"],
      forceRefresh: false,
      sourceType: "csv_upload",
      notes: "CSV upload import from trace-export.csv.",
    });
  });
});
