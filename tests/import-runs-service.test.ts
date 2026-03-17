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
  listRecoverableCsvDirectImportRunIds: vi.fn(),
  markImportRunFailed: vi.fn(),
  resetFailedImportRunItems: vi.fn(),
  updateImportRunSourceState: vi.fn(),
  upsertImportSourceCheckpoint: vi.fn(),
}));

vi.mock("@/lib/investigations/service", () => ({
  investigateBid: vi.fn(),
}));

vi.mock("@/lib/import-runs/csv-direct", () => ({
  processCsvDirectImportItem: vi.fn(),
}));

vi.mock("@/lib/import-runs/historical-backfill", () => ({
  createHistoricalRingbaBackfillRun: vi.fn(),
}));

vi.mock("@/lib/db/import-ops-events", () => ({
  createImportOpsEvent: vi.fn(),
}));

import {
  claimImportRunItems,
  claimImportRunProcessing,
  completeImportRunItem,
  createImportRun,
  finalizeImportRun,
  getImportRunBidIds,
  getImportRunDetail,
  listRecoverableCsvDirectImportRunIds,
  resetFailedImportRunItems,
  updateImportRunSourceState,
} from "@/lib/db/import-runs";
import {
  createAsyncImportRun,
  processImportRun,
  recoverCsvDirectImportRuns,
  rerunImportRun,
  retryFailedImportRunItems,
} from "@/lib/import-runs/service";
import { processCsvDirectImportItem } from "@/lib/import-runs/csv-direct";
import { createHistoricalRingbaBackfillRun } from "@/lib/import-runs/historical-backfill";
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
    processorLeaseExpiresAt: null,
    processorMode: null,
    isStalled: false,
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

function buildHistoricalMetrics(overrides: Record<string, number | null> = {}) {
  return {
    attemptedCount: 0,
    enrichedCount: 0,
    reusedCount: 0,
    notFoundCount: 0,
    failedCount: 0,
    rateLimitedCount: 0,
    serverErrorCount: 0,
    averageFetchLatencyMs: null,
    latencySampleCount: 0,
    totalFetchLatencyMs: 0,
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

  it("aggregates current-run latency for historical fetched items", async () => {
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
    vi.mocked(getImportRunDetail).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          metrics: buildHistoricalMetrics(),
        },
      }),
    );
    vi.mocked(investigateBid).mockResolvedValue({
      resolution: "fetched",
      fetchTelemetry: {
        latencyMs: 120,
        attemptCount: 1,
        errorKind: "none",
      },
      investigation: {
        id: "investigation-1",
        bidId: "bid-1",
        fetchStatus: "fetched",
        enrichmentState: "enriched",
        rawTraceJson: {},
      },
    } as unknown as Awaited<ReturnType<typeof investigateBid>>);
    vi.mocked(updateImportRunSourceState).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          metrics: buildHistoricalMetrics({
            attemptedCount: 1,
            enrichedCount: 1,
            averageFetchLatencyMs: 120,
            latencySampleCount: 1,
            totalFetchLatencyMs: 120,
          }),
        },
      }),
    );
    vi.mocked(finalizeImportRun).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        status: "completed",
        completedCount: 1,
        fetchedCount: 1,
        queuedCount: 0,
        percentComplete: 100,
      }),
    );

    await processImportRun({
      importRunId: "run-1",
      batchSize: 10,
      maxBatches: 1,
    });

    expect(updateImportRunSourceState).toHaveBeenCalledWith({
      importRunId: "run-1",
      sourceMetadata: {
        metrics: buildHistoricalMetrics({
          attemptedCount: 1,
          enrichedCount: 1,
          averageFetchLatencyMs: 120,
          latencySampleCount: 1,
          totalFetchLatencyMs: 120,
        }),
      },
    });
  });

  it("passes the throttle profile stored on historical backfill runs", async () => {
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
    vi.mocked(getImportRunDetail).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          throttleProfileName: "direct_csv_bulk",
          metrics: buildHistoricalMetrics(),
        },
      }),
    );
    vi.mocked(investigateBid).mockResolvedValue({
      resolution: "reused",
      fetchTelemetry: null,
      investigation: {
        id: "investigation-1",
        bidId: "bid-1",
        fetchStatus: "fetched",
        enrichmentState: "enriched",
        rawTraceJson: {},
      },
    } as unknown as Awaited<ReturnType<typeof investigateBid>>);
    vi.mocked(updateImportRunSourceState).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          throttleProfileName: "direct_csv_bulk",
          metrics: buildHistoricalMetrics({
            attemptedCount: 1,
            reusedCount: 1,
          }),
        },
      }),
    );
    vi.mocked(finalizeImportRun).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        status: "completed",
        queuedCount: 0,
        completedCount: 1,
        reusedCount: 1,
      }),
    );

    await processImportRun({
      importRunId: "run-1",
      batchSize: 10,
      maxBatches: 1,
    });

    expect(investigateBid).toHaveBeenCalledWith("bid-1", {
      importRunId: "run-1",
      forceRefresh: false,
      sourceType: "historical_ringba_backfill",
      ringbaBudgetProfile: "direct_csv_bulk",
    });
  });

  it("does not count stale persisted latency for reused historical items", async () => {
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
    vi.mocked(getImportRunDetail).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          metrics: buildHistoricalMetrics(),
        },
      }),
    );
    vi.mocked(investigateBid).mockResolvedValue({
      resolution: "reused",
      fetchTelemetry: null,
      investigation: {
        id: "investigation-1",
        bidId: "bid-1",
        fetchStatus: "fetched",
        enrichmentState: "enriched",
        rawTraceJson: {
          latencyMs: 999,
          errorKind: "none",
        },
      },
    } as unknown as Awaited<ReturnType<typeof investigateBid>>);
    vi.mocked(updateImportRunSourceState).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          metrics: buildHistoricalMetrics({
            attemptedCount: 1,
            reusedCount: 1,
          }),
        },
      }),
    );
    vi.mocked(finalizeImportRun).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        status: "completed",
        completedCount: 1,
        reusedCount: 1,
        queuedCount: 0,
        percentComplete: 100,
      }),
    );

    await processImportRun({
      importRunId: "run-1",
      batchSize: 10,
      maxBatches: 1,
    });

    expect(updateImportRunSourceState).toHaveBeenCalledWith({
      importRunId: "run-1",
      sourceMetadata: {
        metrics: buildHistoricalMetrics({
          attemptedCount: 1,
          reusedCount: 1,
        }),
      },
    });
  });

  it("records rate-limited historical failures alongside latency", async () => {
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
    vi.mocked(getImportRunDetail).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          metrics: buildHistoricalMetrics(),
        },
      }),
    );
    vi.mocked(investigateBid).mockResolvedValue({
      resolution: "failed",
      fetchTelemetry: {
        latencyMs: 250,
        attemptCount: 2,
        errorKind: "rate_limited",
      },
      investigation: {
        id: "investigation-1",
        bidId: "bid-1",
        fetchStatus: "failed",
        enrichmentState: "failed",
        lastError: "429 from Ringba",
        rawTraceJson: {},
      },
    } as unknown as Awaited<ReturnType<typeof investigateBid>>);
    vi.mocked(updateImportRunSourceState).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          metrics: buildHistoricalMetrics({
            attemptedCount: 1,
            failedCount: 1,
            rateLimitedCount: 1,
            averageFetchLatencyMs: 250,
            latencySampleCount: 1,
            totalFetchLatencyMs: 250,
          }),
        },
      }),
    );
    vi.mocked(finalizeImportRun).mockResolvedValue(
      buildRun({
        sourceType: "historical_ringba_backfill",
        status: "completed_with_errors",
        failedCount: 1,
        queuedCount: 0,
        percentComplete: 100,
      }),
    );

    await processImportRun({
      importRunId: "run-1",
      batchSize: 10,
      maxBatches: 1,
    });

    expect(updateImportRunSourceState).toHaveBeenCalledWith({
      importRunId: "run-1",
      sourceMetadata: {
        metrics: buildHistoricalMetrics({
          attemptedCount: 1,
          failedCount: 1,
          rateLimitedCount: 1,
          averageFetchLatencyMs: 250,
          latencySampleCount: 1,
          totalFetchLatencyMs: 250,
        }),
      },
    });
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

  it("recovers direct CSV runs sequentially and creates filtered backfills", async () => {
    vi.mocked(listRecoverableCsvDirectImportRunIds).mockResolvedValue(["run-1", "run-2"]);
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
    vi.mocked(processCsvDirectImportItem).mockResolvedValue({
      investigationId: "investigation-1",
      resolution: "reused",
    });
    vi.mocked(getImportRunDetail).mockImplementation(async (importRunId: string) => {
      return buildRun({
        id: importRunId,
        sourceType: "csv_direct_import",
        sourceStage: "queued",
        status: "queued",
      });
    });
    vi.mocked(updateImportRunSourceState).mockImplementation(async (input) => {
      return buildRun({
        id: input.importRunId,
        sourceType: "csv_direct_import",
        sourceStage: "processing",
        status: "running",
        sourceMetadata: input.sourceMetadata ?? {},
        processorMode: "background_recovery",
      });
    });
    vi.mocked(finalizeImportRun).mockImplementation(async (importRunId: string) => {
      return buildRun({
        id: importRunId,
        sourceType: "csv_direct_import",
        sourceStage: "completed",
        status: "completed",
        queuedCount: 0,
        completedCount: 1,
        reusedCount: 1,
        percentComplete: 100,
      });
    });
    vi.mocked(createHistoricalRingbaBackfillRun).mockImplementation(async (input) => {
      const sourceImportRunIds = input.sourceImportRunIds ?? [];
      return buildRun({
        id: `backfill-${sourceImportRunIds[0] ?? "unknown"}`,
        sourceType: "historical_ringba_backfill",
        sourceMetadata: {
          selection: {
            sourceImportRunIds,
          },
          throttleProfileName: "direct_csv_bulk",
          metrics: buildHistoricalMetrics(),
        },
      });
    });

    const result = await recoverCsvDirectImportRuns({
      stalledOnly: true,
      maxRuns: 2,
      createHistoricalBackfill: true,
      historicalBackfillLimit: 25,
      historicalBackfillSort: "oldest_first",
    });

    expect(listRecoverableCsvDirectImportRunIds).toHaveBeenCalledWith({
      importRunIds: undefined,
      limit: 2,
      stalledOnly: true,
    });

    expect(result.recoveredRuns.map((run) => run.id)).toEqual(["run-1", "run-2"]);
    expect(result.createdBackfillRuns.map((run) => run.id)).toEqual([
      "backfill-run-1",
      "backfill-run-2",
    ]);
    expect(createHistoricalRingbaBackfillRun).toHaveBeenNthCalledWith(1, {
      limit: 25,
      sort: "oldest_first",
      sourceImportRunIds: ["run-1"],
      forceRefresh: false,
    });
    expect(createHistoricalRingbaBackfillRun).toHaveBeenNthCalledWith(2, {
      limit: 25,
      sort: "oldest_first",
      sourceImportRunIds: ["run-2"],
      forceRefresh: false,
    });
  });
});
