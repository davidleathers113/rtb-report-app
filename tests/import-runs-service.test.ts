import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db/import-runs", () => ({
  addImportRunItems: vi.fn(),
  claimImportRunItems: vi.fn(),
  claimImportRunProcessing: vi.fn(),
  completeImportRunItem: vi.fn(),
  createImportRun: vi.fn(),
  failImportRunItem: vi.fn(),
  finalizeImportRun: vi.fn(),
  getImportRunBidIds: vi.fn(),
  getImportRunDetail: vi.fn(),
  getImportSourceCheckpoint: vi.fn(),
  markImportRunFailed: vi.fn(),
  resetFailedImportRunItems: vi.fn(),
  updateImportRunSourceState: vi.fn(),
  upsertImportSourceCheckpoint: vi.fn(),
}));

vi.mock("@/lib/investigations/service", () => ({
  investigateBid: vi.fn(),
}));

import {
  claimImportRunItems,
  claimImportRunProcessing,
  completeImportRunItem,
  createImportRun,
  finalizeImportRun,
  getImportRunBidIds,
  getImportRunDetail,
  resetFailedImportRunItems,
} from "@/lib/db/import-runs";
import {
  createAsyncImportRun,
  processImportRun,
  rerunImportRun,
  retryFailedImportRunItems,
} from "@/lib/import-runs/service";
import { investigateBid } from "@/lib/investigations/service";
import type { ImportRunDetail } from "@/types/import-run";

function buildRun(overrides: Partial<ImportRunDetail> = {}): ImportRunDetail {
  return {
    id: "run-1",
    sourceType: "manual_bulk",
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
    totalItems: 1,
    queuedCount: 1,
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

describe("import runs service", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("dedupes bid ids on run creation", async () => {
    vi.mocked(createImportRun).mockResolvedValue("run-1");
    vi.mocked(getImportRunDetail).mockResolvedValue(buildRun());

    const result = await createAsyncImportRun({
      bidIds: ["abc", "abc", "def"],
      forceRefresh: false,
      sourceType: "manual_bulk",
    });

    expect(createImportRun).toHaveBeenCalledWith({
      bidIds: ["abc", "def"],
      forceRefresh: false,
      sourceType: "manual_bulk",
      notes: undefined,
    });
    expect(result.id).toBe("run-1");
  });

  it("processes claimed items and finalizes the run", async () => {
    vi.mocked(claimImportRunProcessing).mockResolvedValue({
      id: "run-1",
      status: "queued",
      shouldProcess: true,
      forceRefresh: false,
      totalItems: 1,
      totalProcessed: 0,
      lastError: null,
    });
    vi.mocked(claimImportRunItems).mockResolvedValue([
      { id: "item-1", bidId: "bid-1", position: 1 },
    ]);
    vi.mocked(getImportRunDetail).mockResolvedValue(buildRun());
    vi.mocked(investigateBid).mockResolvedValue({
      resolution: "reused",
      investigation: {
        id: "investigation-1",
        bidId: "bid-1",
        fetchStatus: "fetched",
      },
    } as Awaited<ReturnType<typeof investigateBid>>);
    vi.mocked(finalizeImportRun).mockResolvedValue(
      buildRun({
        status: "completed",
        completedCount: 1,
        reusedCount: 1,
        queuedCount: 0,
        percentComplete: 100,
      }),
    );

    const result = await processImportRun({
      importRunId: "run-1",
      batchSize: 10,
      maxBatches: 1,
    });

    expect(completeImportRunItem).toHaveBeenCalledWith({
      itemId: "item-1",
      investigationId: "investigation-1",
      resolution: "reused",
    });
    expect(result?.status).toBe("completed");
  });

  it("retries and reruns through the import run helpers", async () => {
    vi.mocked(resetFailedImportRunItems).mockResolvedValue(
      buildRun({
        status: "queued",
        failedCount: 0,
        queuedCount: 1,
      }),
    );
    vi.mocked(getImportRunBidIds).mockResolvedValue(["bid-1", "bid-2"]);
    vi.mocked(createImportRun).mockResolvedValue("run-2");
    vi.mocked(getImportRunDetail).mockResolvedValue(
      buildRun({
        id: "run-2",
        forceRefresh: true,
        totalItems: 2,
        queuedCount: 2,
      }),
    );

    const retryResult = await retryFailedImportRunItems({
      importRunId: "run-1",
      forceRefresh: true,
    });
    const rerunResult = await rerunImportRun({
      importRunId: "run-1",
      forceRefresh: true,
    });

    expect(retryResult.status).toBe("queued");
    expect(rerunResult.id).toBe("run-2");
    expect(createImportRun).toHaveBeenCalledWith({
      bidIds: ["bid-1", "bid-2"],
      forceRefresh: true,
      sourceType: "import_run_rerun",
      notes: "Rerun of import run run-1.",
    });
  });
});
