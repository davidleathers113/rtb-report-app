import "server-only";

import {
  countHistoricalBackfillCandidates,
  listHistoricalBackfillCandidates,
} from "@/lib/db/investigations";
import {
  createImportRun,
  getImportRunDetail,
  getImportSourceCheckpoint,
} from "@/lib/db/import-runs";
import type {
  HistoricalBackfillSourceMetadata,
  RingbaBudgetProfileName,
} from "@/types/import-run";

export interface CreateHistoricalRingbaBackfillRunInput {
  startBidDt?: string;
  endBidDt?: string;
  limit: number;
  sort: "newest_first" | "oldest_first";
  campaignId?: string;
  publisherId?: string;
  sourceImportRunId?: string;
  sourceImportRunIds?: string[];
  forceRefresh: boolean;
  pilotLabel?: string;
  triggerType?: "manual" | "scheduled";
  scheduleId?: string | null;
  scheduleName?: string;
  checkpointSourceKey?: string | null;
  throttleProfileName?: RingbaBudgetProfileName;
}

function dedupeImportRunIds(values: string[]) {
  const normalized: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }

    seen.add(trimmed);
    normalized.push(trimmed);
  }

  return normalized;
}

function resolveSourceImportRunIds(input: CreateHistoricalRingbaBackfillRunInput) {
  const values = dedupeImportRunIds([
    ...(input.sourceImportRunIds ?? []),
    ...(input.sourceImportRunId ? [input.sourceImportRunId] : []),
  ]);

  return values;
}

function resolveHistoricalThrottleProfileName(input: CreateHistoricalRingbaBackfillRunInput) {
  if (input.throttleProfileName) {
    return input.throttleProfileName;
  }

  const selectedRunIds = resolveSourceImportRunIds(input);
  return selectedRunIds.length > 0 ? "direct_csv_bulk" : "historical_backfill";
}

export async function createHistoricalRingbaBackfillRun(
  input: CreateHistoricalRingbaBackfillRunInput,
) {
  const sourceImportRunIds = resolveSourceImportRunIds(input);
  const throttleProfileName = resolveHistoricalThrottleProfileName(input);
  const checkpoint =
    input.checkpointSourceKey ? await getImportSourceCheckpoint(input.checkpointSourceKey) : null;
  const checkpointSourceMetadata =
    checkpoint?.sourceMetadata &&
    typeof checkpoint.sourceMetadata === "object" &&
    !Array.isArray(checkpoint.sourceMetadata)
      ? (checkpoint.sourceMetadata as Record<string, unknown>)
      : {};
  const cursorBidDt = checkpoint?.lastSuccessfulBidDt ?? undefined;
  const cursorBidId =
    typeof checkpointSourceMetadata.lastSuccessfulBidId === "string"
      ? checkpointSourceMetadata.lastSuccessfulBidId
      : undefined;
  const totalCandidateCount = await countHistoricalBackfillCandidates({
    startBidDt: input.startBidDt,
    endBidDt: input.endBidDt,
    campaignId: input.campaignId,
    publisherId: input.publisherId,
    sourceImportRunIds,
  });
  const candidates = await listHistoricalBackfillCandidates({
    startBidDt: input.startBidDt,
    endBidDt: input.endBidDt,
    limit: input.limit,
    sort: input.sort,
    cursorBidDt,
    cursorBidId,
    campaignId: input.campaignId,
    publisherId: input.publisherId,
    sourceImportRunIds,
  });

  const selectedBidIds = candidates.map((candidate) => candidate.bidId);
  const sourceMetadata: HistoricalBackfillSourceMetadata = {
    selection: {
      startBidDt: input.startBidDt ?? null,
      endBidDt: input.endBidDt ?? null,
      sort: input.sort,
      limit: input.limit,
      campaignId: input.campaignId ?? null,
      publisherId: input.publisherId ?? null,
      sourceImportRunId: sourceImportRunIds[0] ?? null,
      sourceImportRunIds,
    },
    pilotLabel: input.pilotLabel ?? null,
    throttleProfileName,
    checkpointSourceKey: input.checkpointSourceKey ?? null,
    checkpointCursor: {
      bidDt: cursorBidDt ?? null,
      bidId: cursorBidId ?? null,
    },
    candidateCount: totalCandidateCount,
    selectedCandidateCount: candidates.length,
    remainingCandidateCount: Math.max(0, totalCandidateCount - candidates.length),
    selectedCandidates: candidates,
    metrics: {
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
    },
  };
  const importRunId = await createImportRun({
    sourceType: "historical_ringba_backfill",
    bidIds: selectedBidIds,
    forceRefresh: input.forceRefresh,
    triggerType: input.triggerType ?? "manual",
    scheduleId: input.scheduleId ?? null,
    notes:
      input.triggerType === "scheduled" && input.scheduleName
        ? `Scheduled historical Ringba backfill from ${input.scheduleName}.`
        : input.pilotLabel && input.pilotLabel.trim()
          ? `Historical Ringba backfill pilot: ${input.pilotLabel.trim()}`
          : sourceImportRunIds.length === 1
            ? `Historical Ringba backfill for direct CSV run ${sourceImportRunIds[0]}.`
            : sourceImportRunIds.length > 1
              ? `Historical Ringba backfill for ${sourceImportRunIds.length} direct CSV runs.`
          : "Historical Ringba backfill.",
    sourceWindowStart: input.startBidDt ?? null,
    sourceWindowEnd: input.endBidDt ?? null,
    sourceMetadata,
  });
  const detail = await getImportRunDetail(importRunId);

  if (!detail) {
    throw new Error(`Unable to load import run detail after creation: ${importRunId}`);
  }

  return detail;
}
