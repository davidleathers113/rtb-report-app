import "server-only";

import {
  claimImportRunItems,
  claimImportRunProcessing,
  completeImportRunItem,
  createImportRun,
  failImportRunItem,
  finalizeImportRun,
  getImportRunBidIds,
  getImportRunDetail,
  markImportRunFailed,
  resetFailedImportRunItems,
  upsertImportSourceCheckpoint,
  updateImportRunSourceState,
} from "@/lib/db/import-runs";
import {
  markImportScheduleRunFailed,
  pauseImportSchedule,
  markImportScheduleRunSucceeded,
} from "@/lib/db/import-schedules";
import { createImportOpsEvent } from "@/lib/db/import-ops-events";
import {
  investigateBid,
  type InvestigationExecutionResult,
} from "@/lib/investigations/service";
import { processCsvDirectImportItem } from "@/lib/import-runs/csv-direct";
import { createHistoricalRingbaBackfillRun } from "@/lib/import-runs/historical-backfill";
import { prepareRingbaRecentImportRun } from "@/lib/import-runs/ringba-recent";
import type {
  HistoricalBackfillMetrics,
  HistoricalBackfillSourceMetadata,
  ImportRunDetail,
} from "@/types/import-run";

function dedupeBidIds(bidIds: string[]) {
  const values: string[] = [];
  const seen = new Set<string>();

  for (const bidId of bidIds) {
    const trimmed = bidId.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }

    seen.add(trimmed);
    values.push(trimmed);
  }

  return values;
}

async function syncScheduledRunStatus(run: ImportRunDetail | null | undefined) {
  if (!run || run.triggerType !== "scheduled" || !run.scheduleId) {
    return;
  }

  if (run.status === "completed") {
    if (run.sourceType === "historical_ringba_backfill") {
      const checkpointSourceKey =
        typeof run.sourceMetadata.checkpointSourceKey === "string"
          ? run.sourceMetadata.checkpointSourceKey
          : `historical_ringba_backfill_schedule:${run.scheduleId}`;
      const selectedCandidates = getHistoricalSelectedCandidates(run.sourceMetadata);
      const lastCandidate = selectedCandidates[selectedCandidates.length - 1] ?? null;

      if (lastCandidate?.bidDt) {
        await upsertImportSourceCheckpoint({
          sourceKey: checkpointSourceKey,
          sourceType: "historical_ringba_backfill",
          lastSuccessfulBidDt: lastCandidate.bidDt,
          sourceMetadata: {
            lastSuccessfulBidId: lastCandidate.bidId,
            importRunId: run.id,
            updatedAt: run.completedAt ?? run.updatedAt,
          },
        });
      }
    }

    await markImportScheduleRunSucceeded({
      scheduleId: run.scheduleId,
      runCreatedAt: run.createdAt,
      occurredAt: run.completedAt ?? run.updatedAt,
    });
    await createImportOpsEvent({
      eventType: "scheduled_run_succeeded",
      severity: "info",
      source: "system",
      scheduleId: run.scheduleId,
      importRunId: run.id,
      message: `Scheduled run ${run.id} completed successfully.`,
    }).catch(() => undefined);
  }

  if (run.status === "completed_with_errors" || run.status === "failed") {
    await markImportScheduleRunFailed({
      scheduleId: run.scheduleId,
      runCreatedAt: run.createdAt,
      occurredAt: run.completedAt ?? run.updatedAt,
      errorMessage: run.lastError ?? `Scheduled run ended with ${run.status}.`,
    });
    await createImportOpsEvent({
      eventType: "scheduled_run_failed",
      severity: "error",
      source: "system",
      scheduleId: run.scheduleId,
      importRunId: run.id,
      message: run.lastError ?? `Scheduled run ${run.id} ended with ${run.status}.`,
      metadataJson: {
        status: run.status,
        sourceStage: run.sourceStage,
      },
    }).catch(() => undefined);

    if (run.sourceType === "historical_ringba_backfill") {
      const metrics = getHistoricalMetrics(run.sourceMetadata);
      await pauseImportSchedule({
        scheduleId: run.scheduleId,
        reason: "Historical backfill auto-paused after a failed or partial run.",
      }).catch(() => undefined);
      await createImportOpsEvent({
        eventType: "schedule_paused",
        severity: "warning",
        source: "system",
        scheduleId: run.scheduleId,
        importRunId: run.id,
        message: `Historical backfill schedule ${run.scheduleId} auto-paused after ${run.status}.`,
        metadataJson: {
          failedCount: metrics.failedCount,
          rateLimitedCount: metrics.rateLimitedCount,
          serverErrorCount: metrics.serverErrorCount,
        },
      }).catch(() => undefined);
    }
  }
}

function getHistoricalMetrics(sourceMetadata: Record<string, unknown>) {
  const historicalMetadata = sourceMetadata as HistoricalBackfillSourceMetadata;
  const metrics =
    historicalMetadata.metrics && typeof historicalMetadata.metrics === "object"
      ? (historicalMetadata.metrics as Record<string, unknown>)
      : {};

  return {
    attemptedCount: typeof metrics.attemptedCount === "number" ? metrics.attemptedCount : 0,
    enrichedCount: typeof metrics.enrichedCount === "number" ? metrics.enrichedCount : 0,
    reusedCount: typeof metrics.reusedCount === "number" ? metrics.reusedCount : 0,
    notFoundCount: typeof metrics.notFoundCount === "number" ? metrics.notFoundCount : 0,
    failedCount: typeof metrics.failedCount === "number" ? metrics.failedCount : 0,
    rateLimitedCount:
      typeof metrics.rateLimitedCount === "number" ? metrics.rateLimitedCount : 0,
    serverErrorCount:
      typeof metrics.serverErrorCount === "number" ? metrics.serverErrorCount : 0,
    averageFetchLatencyMs:
      typeof metrics.averageFetchLatencyMs === "number" ? metrics.averageFetchLatencyMs : null,
    latencySampleCount:
      typeof metrics.latencySampleCount === "number" ? metrics.latencySampleCount : 0,
    totalFetchLatencyMs:
      typeof metrics.totalFetchLatencyMs === "number" ? metrics.totalFetchLatencyMs : 0,
  } satisfies HistoricalBackfillMetrics;
}

function buildHistoricalMetricsMetadata(
  sourceMetadata: Record<string, unknown>,
  result: InvestigationExecutionResult,
) {
  const metrics = getHistoricalMetrics(sourceMetadata);
  const nextMetrics = {
    ...metrics,
    attemptedCount: metrics.attemptedCount + 1,
  };
  const investigation = result.investigation;
  const rawTraceJson =
    investigation?.rawTraceJson && typeof investigation.rawTraceJson === "object"
      ? investigation.rawTraceJson
      : {};
  const errorKind =
    result.fetchTelemetry?.errorKind ??
    (typeof rawTraceJson.errorKind === "string" ? rawTraceJson.errorKind : null);
  const latencyMs =
    typeof result.fetchTelemetry?.latencyMs === "number" &&
    Number.isFinite(result.fetchTelemetry.latencyMs)
      ? result.fetchTelemetry.latencyMs
      : null;

  if (result.resolution === "reused") {
    nextMetrics.reusedCount += 1;
  } else if (investigation?.enrichmentState === "enriched") {
    nextMetrics.enrichedCount += 1;
  } else if (investigation?.enrichmentState === "not_found") {
    nextMetrics.notFoundCount += 1;
  } else {
    nextMetrics.failedCount += 1;
  }

  if (errorKind === "rate_limited") {
    nextMetrics.rateLimitedCount += 1;
  }

  if (errorKind === "server_error") {
    nextMetrics.serverErrorCount += 1;
  }

  if (latencyMs !== null) {
    nextMetrics.latencySampleCount += 1;
    nextMetrics.totalFetchLatencyMs += latencyMs;
    nextMetrics.averageFetchLatencyMs = Math.round(
      nextMetrics.totalFetchLatencyMs / nextMetrics.latencySampleCount,
    );
  }

  return {
    ...sourceMetadata,
    metrics: nextMetrics,
  };
}

function getHistoricalSelectedCandidates(sourceMetadata: Record<string, unknown>) {
  const selectedCandidates = sourceMetadata.selectedCandidates;
  if (!Array.isArray(selectedCandidates)) {
    return [];
  }

  return selectedCandidates.filter((candidate) => {
    return (
      candidate &&
      typeof candidate === "object" &&
      !Array.isArray(candidate) &&
      typeof candidate.bidId === "string"
    );
  }) as Array<{ bidId: string; bidDt: string | null }>;
}

export async function createAsyncImportRun(input: {
  bidIds: string[];
  forceRefresh: boolean;
  sourceType: string;
  notes?: string;
  triggerType?: "manual" | "scheduled";
  scheduleId?: string | null;
}) {
  const importRunId = await createImportRun({
    sourceType: input.sourceType,
    bidIds: dedupeBidIds(input.bidIds),
    forceRefresh: input.forceRefresh,
    notes: input.notes,
    triggerType: input.triggerType,
    scheduleId: input.scheduleId,
  });

  const detail = await getImportRunDetail(importRunId);

  if (!detail) {
    throw new Error(`Unable to load import run detail after creation: ${importRunId}`);
  }

  return detail;
}

export { createHistoricalRingbaBackfillRun };

export async function processImportRun(input: {
  importRunId: string;
  batchSize?: number;
  maxBatches?: number;
}) {
  const claim = await claimImportRunProcessing({
    importRunId: input.importRunId,
  });

  if (!claim.shouldProcess) {
    const current = await getImportRunDetail(input.importRunId);

    if (!current) {
      throw new Error(`Import run not found: ${input.importRunId}`);
    }

    return current;
  }

  try {
    let current = await getImportRunDetail(input.importRunId);

    if (!current) {
      throw new Error(`Import run not found: ${input.importRunId}`);
    }

    if (
      current.sourceType === "ringba_recent_import" &&
      !["queued", "processing", "completed", "failed"].includes(current.sourceStage)
    ) {
      current = await prepareRingbaRecentImportRun({
        importRunId: input.importRunId,
        sourceMetadata: current.sourceMetadata,
      });

      if (!current) {
        throw new Error(`Unable to reload import run after Ringba source preparation.`);
      }
    }

    if (current.totalItems === 0) {
      const detail = await finalizeImportRun(input.importRunId);
      await syncScheduledRunStatus(detail);
      return detail;
    }

    let batchesProcessed = 0;
    const effectiveBatchSize =
      current.sourceType === "historical_ringba_backfill"
        ? Math.min(input.batchSize ?? 10, 3)
        : (input.batchSize ?? 10);

    while (batchesProcessed < (input.maxBatches ?? 2)) {
      const claimedItems = await claimImportRunItems({
        importRunId: input.importRunId,
        batchSize: effectiveBatchSize,
      });

      if (claimedItems.length === 0) {
        break;
      }

      for (const item of claimedItems) {
        try {
          if (current.sourceType === "csv_direct_import") {
            const result = await processCsvDirectImportItem({
              importRunId: input.importRunId,
              bidId: item.bidId,
            });

            await completeImportRunItem({
              itemId: item.id,
              investigationId: result.investigationId,
              resolution: result.resolution,
            });
            continue;
          }

          const result = await investigateBid(item.bidId, {
            importRunId: input.importRunId,
            forceRefresh: claim.forceRefresh,
            sourceType: current.sourceType,
          });
          const investigation = result.investigation;

          if (!investigation) {
            throw new Error(`Investigation did not return persisted data for ${item.bidId}.`);
          }

          if (result.resolution === "failed" || investigation.fetchStatus === "failed") {
            await failImportRunItem({
              itemId: item.id,
              investigationId: investigation.id,
              errorMessage: investigation.lastError ?? "Investigation failed.",
            });
          } else {
            await completeImportRunItem({
              itemId: item.id,
              investigationId: investigation.id,
              resolution: result.resolution,
            });
          }

          if (current.sourceType === "historical_ringba_backfill") {
            const updated = await updateImportRunSourceState({
              importRunId: input.importRunId,
              sourceMetadata: buildHistoricalMetricsMetadata(current.sourceMetadata, {
                resolution: result.resolution,
                investigation,
                fetchTelemetry: result.fetchTelemetry,
              }),
            });

            if (updated) {
              current = updated;
            }
          }
        } catch (error) {
          const message =
            error instanceof Error ? error.message : "Unexpected import run item error.";

          await failImportRunItem({
            itemId: item.id,
            investigationId: null,
            errorMessage: message,
          });

          if (current.sourceType === "historical_ringba_backfill") {
            const updated = await updateImportRunSourceState({
              importRunId: input.importRunId,
              sourceMetadata: buildHistoricalMetricsMetadata(current.sourceMetadata, {
                resolution: "failed",
                investigation: null,
              fetchTelemetry: null,
              }),
            });

            if (updated) {
              current = updated;
            }
          }
        }
      }

      batchesProcessed += 1;
    }

    const detail = await finalizeImportRun(input.importRunId);
    await syncScheduledRunStatus(detail);
    return detail;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unexpected import run processing error.";

    const detail = await markImportRunFailed(input.importRunId, message);
    await syncScheduledRunStatus(detail);
    return detail;
  }
}

export async function retryFailedImportRunItems(input: {
  importRunId: string;
  forceRefresh: boolean;
}) {
  const detail = await resetFailedImportRunItems(input);

  if (!detail) {
    throw new Error(`Unable to reload import run after retry reset: ${input.importRunId}`);
  }

  return detail;
}

export async function rerunImportRun(input: {
  importRunId: string;
  forceRefresh: boolean;
}) {
  const bidIds = await getImportRunBidIds(input.importRunId);

  return createAsyncImportRun({
    bidIds,
    forceRefresh: input.forceRefresh,
    sourceType: "import_run_rerun",
    notes: `Rerun of import run ${input.importRunId}.`,
  });
}
