import "server-only";

import { eq, inArray, sql } from "drizzle-orm";

import { getDb, getSqlite } from "@/lib/db/client";
import { getInvestigationListItemsByIds } from "@/lib/db/investigations";
import {
  importRunItems,
  importRuns,
  importSourceCheckpoints,
  type ImportRunItemRow,
  type ImportRunRow,
  type ImportSourceCheckpointRow,
} from "@/lib/db/schema";
import {
  addSeconds,
  createId,
  nowIso,
  toTimestamp,
} from "@/lib/db/utils";
import type { InvestigationListItem } from "@/types/bid";
import type {
  ImportRunDetail,
  ImportRunExportDownloadStatus,
  ImportRunItemResolution,
  ImportRunItemStatus,
  ImportRunSourceStage,
  ImportRunSourceType,
  ImportRunStatus,
} from "@/types/import-run";

export const DEFAULT_IMPORT_RUN_DETAIL_ITEM_LIMIT = 250;

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

function calculateProgress(items: ImportRunItemRow[]) {
  let queuedCount = 0;
  let runningCount = 0;
  let completedCount = 0;
  let reusedCount = 0;
  let fetchedCount = 0;
  let failedCount = 0;

  for (const item of items) {
    if (item.status === "queued") {
      queuedCount += 1;
    }
    if (item.status === "running") {
      runningCount += 1;
    }
    if (item.status === "completed") {
      completedCount += 1;
    }
    if (item.resolution === "reused") {
      reusedCount += 1;
    }
    if (item.resolution === "fetched") {
      fetchedCount += 1;
    }
    if (item.status === "failed" || item.resolution === "failed") {
      failedCount += 1;
    }
  }

  const totalItems = items.length;
  const processedItems = completedCount + failedCount;
  const percentComplete =
    totalItems === 0 ? 0 : Math.min(100, Math.round((processedItems / totalItems) * 100));

  return {
    totalItems,
    queuedCount,
    runningCount,
    completedCount,
    reusedCount,
    fetchedCount,
    failedCount,
    percentComplete,
  };
}

function deriveImportRunStatus(items: ImportRunItemRow[]) {
  const progress = calculateProgress(items);

  if (items.length === 0) {
    return "queued" as const;
  }

  if (progress.runningCount > 0) {
    return "running" as const;
  }

  if (progress.queuedCount > 0) {
    return progress.completedCount === 0 && progress.failedCount === 0
      ? ("queued" as const)
      : ("running" as const);
  }

  if (progress.failedCount > 0) {
    return "completed_with_errors" as const;
  }

  return "completed" as const;
}

function normalizeSourceMetadata(value: Record<string, unknown> | null) {
  return value ?? {};
}

function mapImportRunItem(
  row: ImportRunItemRow,
  investigationsById: Map<string, InvestigationListItem>,
) {
  return {
    id: row.id,
    importRunId: row.importRunId,
    bidId: row.bidId,
    position: row.position,
    status: row.status as ImportRunItemStatus,
    resolution: row.resolution as ImportRunItemResolution | null,
    errorMessage: row.errorMessage,
    investigationId: row.investigationId,
    startedAt: row.startedAt,
    completedAt: row.completedAt,
    attemptCount: row.attemptCount,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    investigation: row.investigationId
      ? investigationsById.get(row.investigationId) ?? null
      : null,
  };
}

async function getImportRunRow(importRunId: string) {
  const db = getDb();
  const row = db
    .select()
    .from(importRuns)
    .where(eq(importRuns.id, importRunId))
    .get() as ImportRunRow | undefined;

  return row ?? null;
}

async function getImportRunItemRows(importRunId: string, itemLimit?: number) {
  const db = getDb();
  const query = db
    .select()
    .from(importRunItems)
    .where(eq(importRunItems.importRunId, importRunId));

  const rows = (
    typeof itemLimit === "number" && itemLimit > 0
      ? query.limit(itemLimit).all()
      : query.all()
  ) as ImportRunItemRow[];

  rows.sort((left, right) => left.position - right.position);
  return rows;
}

async function getImportRunProgress(importRunId: string) {
  const db = getDb();
  const row = db
    .select({
      totalItems: sql<number>`count(*)`,
      queuedCount: sql<number>`sum(case when ${importRunItems.status} = 'queued' then 1 else 0 end)`,
      runningCount: sql<number>`sum(case when ${importRunItems.status} = 'running' then 1 else 0 end)`,
      completedCount: sql<number>`sum(case when ${importRunItems.status} = 'completed' then 1 else 0 end)`,
      reusedCount: sql<number>`sum(case when ${importRunItems.resolution} = 'reused' then 1 else 0 end)`,
      fetchedCount: sql<number>`sum(case when ${importRunItems.resolution} = 'fetched' then 1 else 0 end)`,
      failedCount: sql<number>`sum(case when ${importRunItems.status} = 'failed' or ${importRunItems.resolution} = 'failed' then 1 else 0 end)`,
    })
    .from(importRunItems)
    .where(eq(importRunItems.importRunId, importRunId))
    .get() as
    | {
        totalItems: number;
        queuedCount: number | null;
        runningCount: number | null;
        completedCount: number | null;
        reusedCount: number | null;
        fetchedCount: number | null;
        failedCount: number | null;
      }
    | undefined;

  const totalItems = row?.totalItems ?? 0;
  const completedCount = row?.completedCount ?? 0;
  const failedCount = row?.failedCount ?? 0;
  const processedItems = completedCount + failedCount;

  return {
    totalItems,
    queuedCount: row?.queuedCount ?? 0,
    runningCount: row?.runningCount ?? 0,
    completedCount,
    reusedCount: row?.reusedCount ?? 0,
    fetchedCount: row?.fetchedCount ?? 0,
    failedCount,
    percentComplete:
      totalItems === 0 ? 0 : Math.min(100, Math.round((processedItems / totalItems) * 100)),
  };
}

function splitIntoChunks(values: string[], chunkSize: number) {
  const chunks: string[][] = [];

  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }

  return chunks;
}

async function getImportRunItemBidIds(importRunId: string) {
  const rows = await getImportRunItemRows(importRunId);
  return rows.map((row) => ({
    bidId: row.bidId,
    position: row.position,
  }));
}

async function insertImportRunItems(input: {
  importRunId: string;
  bidIds: string[];
}) {
  const existingRows = await getImportRunItemBidIds(input.importRunId);
  const existingBidIds = new Set(existingRows.map((row) => row.bidId));
  const valuesToInsert = input.bidIds.filter((bidId) => !existingBidIds.has(bidId));

  if (valuesToInsert.length === 0) {
    return {
      insertedCount: 0,
      existingCount: existingRows.length,
    };
  }

  const db = getDb();
  const now = nowIso();
  const chunks = splitIntoChunks(valuesToInsert, 1000);
  let currentPosition = existingRows.length + 1;

  for (const chunk of chunks) {
    const rows = chunk.map((bidId) => {
      const row = {
        id: createId(),
        importRunId: input.importRunId,
        bidId,
        position: currentPosition,
        status: "queued" as ImportRunItemStatus,
        createdAt: now,
        updatedAt: now,
      };
      currentPosition += 1;
      return row;
    });

    db.insert(importRunItems).values(rows).run();
  }

  return {
    insertedCount: valuesToInsert.length,
    existingCount: existingRows.length,
  };
}

export async function createImportRun(input: {
  sourceType: string;
  bidIds?: string[];
  forceRefresh: boolean;
  notes?: string;
  triggerType?: "manual" | "scheduled";
  scheduleId?: string | null;
  sourceStage?: ImportRunSourceStage;
  sourceWindowStart?: string | null;
  sourceWindowEnd?: string | null;
  exportJobId?: string | null;
  exportRowCount?: number;
  exportDownloadStatus?: ImportRunExportDownloadStatus | null;
  sourceMetadata?: Record<string, unknown>;
}) {
  const bidIds = dedupeBidIds(input.bidIds ?? []);
  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const runId = createId();

  sqlite.transaction(() => {
    db.insert(importRuns)
      .values({
        id: runId,
        sourceType: input.sourceType,
        triggerType: input.triggerType ?? "manual",
        scheduleId: input.scheduleId ?? null,
        sourceStage: input.sourceStage ?? "queued",
        status: "queued",
        forceRefresh: input.forceRefresh,
        notes: input.notes ?? null,
        totalFound: bidIds.length,
        totalProcessed: 0,
        sourceWindowStart: input.sourceWindowStart ?? null,
        sourceWindowEnd: input.sourceWindowEnd ?? null,
        exportJobId: input.exportJobId ?? null,
        exportRowCount: input.exportRowCount ?? 0,
        exportDownloadStatus: input.exportDownloadStatus ?? null,
        sourceMetadata: input.sourceMetadata ?? {},
        startedAt: now,
        createdAt: now,
        updatedAt: now,
      })
      .run();

    if (bidIds.length > 0) {
      let position = 1;
      db.insert(importRunItems)
        .values(
          bidIds.map((bidId) => ({
            id: createId(),
            importRunId: runId,
            bidId,
            position: position++,
            status: "queued",
            createdAt: now,
            updatedAt: now,
          })),
        )
        .run();
    }
  })();

  return runId;
}

export async function getImportRunDetail(
  importRunId: string,
  options?: {
    itemLimit?: number;
  },
): Promise<ImportRunDetail | null> {
  const run = await getImportRunRow(importRunId);

  if (!run) {
    return null;
  }

  const itemRows = await getImportRunItemRows(
    importRunId,
    options?.itemLimit ?? DEFAULT_IMPORT_RUN_DETAIL_ITEM_LIMIT,
  );
  const investigationIds = itemRows
    .map((item) => item.investigationId)
    .filter((value): value is string => Boolean(value));
  const investigations = await getInvestigationListItemsByIds(investigationIds);
  const investigationsById = new Map(
    investigations.map((investigation) => [investigation.id, investigation]),
  );
  const progress = await getImportRunProgress(importRunId);

  return {
    id: run.id,
    sourceType: run.sourceType as ImportRunSourceType,
    triggerType: run.triggerType as "manual" | "scheduled",
    scheduleId: run.scheduleId,
    sourceStage: run.sourceStage as ImportRunSourceStage,
    status: run.status as ImportRunStatus,
    forceRefresh: run.forceRefresh,
    notes: run.notes,
    lastError: run.lastError,
    sourceWindowStart: run.sourceWindowStart,
    sourceWindowEnd: run.sourceWindowEnd,
    exportJobId: run.exportJobId,
    exportRowCount: run.exportRowCount,
    exportDownloadStatus: run.exportDownloadStatus as ImportRunExportDownloadStatus | null,
    sourceMetadata: normalizeSourceMetadata(
      (run.sourceMetadata ?? null) as Record<string, unknown> | null,
    ),
    startedAt: run.startedAt,
    completedAt: run.completedAt,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    ...progress,
    items: itemRows.map((item) => mapImportRunItem(item, investigationsById)),
  };
}

export async function updateImportRunSourceState(input: {
  importRunId: string;
  status?: ImportRunStatus;
  sourceStage?: ImportRunSourceStage;
  sourceWindowStart?: string | null;
  sourceWindowEnd?: string | null;
  exportJobId?: string | null;
  exportRowCount?: number;
  exportDownloadStatus?: ImportRunExportDownloadStatus | null;
  sourceMetadata?: Record<string, unknown>;
  lastError?: string | null;
  completedAt?: string | null;
  allowSourceStageRegression?: boolean;
}) {
  const db = getDb();
  const now = nowIso();
  const updateData: Record<string, unknown> = {};
  const currentRun = await getImportRunRow(input.importRunId);

  if (!currentRun) {
    throw new Error(`Unable to update import run source state: ${input.importRunId} not found.`);
  }

  const sourceStageOrder: Record<ImportRunSourceStage, number> = {
    creating_export: 0,
    polling_export: 1,
    downloading: 2,
    extracting: 3,
    parsing: 4,
    queued: 5,
    processing: 6,
    completed: 7,
    failed: 8,
  };

  if (input.status !== undefined) {
    updateData.status = input.status;
  }
  if (input.sourceStage !== undefined) {
    const currentStageOrder = sourceStageOrder[currentRun.sourceStage as ImportRunSourceStage];
    const nextStageOrder = sourceStageOrder[input.sourceStage];
    const shouldAdvanceStage =
      input.allowSourceStageRegression === true ||
      currentRun.sourceStage === input.sourceStage ||
      nextStageOrder >= currentStageOrder;

    if (shouldAdvanceStage) {
      updateData.sourceStage = input.sourceStage;
    }
  }
  if (input.sourceWindowStart !== undefined) {
    updateData.sourceWindowStart = input.sourceWindowStart;
  }
  if (input.sourceWindowEnd !== undefined) {
    updateData.sourceWindowEnd = input.sourceWindowEnd;
  }
  if (input.exportJobId !== undefined) {
    updateData.exportJobId = input.exportJobId;
  }
  if (input.exportRowCount !== undefined) {
    updateData.exportRowCount = input.exportRowCount;
  }
  if (input.exportDownloadStatus !== undefined) {
    updateData.exportDownloadStatus = input.exportDownloadStatus;
  }
  if (input.sourceMetadata !== undefined) {
    updateData.sourceMetadata = input.sourceMetadata;
  }
  if (input.lastError !== undefined) {
    updateData.lastError = input.lastError;
  }
  if (input.completedAt !== undefined) {
    updateData.completedAt = input.completedAt;
  }
  updateData.updatedAt = now;

  db.update(importRuns).set(updateData).where(eq(importRuns.id, input.importRunId)).run();

  return getImportRunDetail(input.importRunId);
}

export async function addImportRunItems(input: {
  importRunId: string;
  bidIds: string[];
  sourceStage?: ImportRunSourceStage;
  exportRowCount?: number;
  exportDownloadStatus?: ImportRunExportDownloadStatus | null;
  sourceMetadata?: Record<string, unknown>;
}) {
  const bidIds = dedupeBidIds(input.bidIds);
  const db = getDb();
  const now = nowIso();
  let insertedCount = 0;

  if (bidIds.length > 0) {
    const result = await insertImportRunItems({
      importRunId: input.importRunId,
      bidIds,
    });
    insertedCount = result.insertedCount;
  }

  const allItems = await getImportRunItemRows(input.importRunId);
  db.update(importRuns)
    .set({
      totalFound: allItems.length,
      sourceStage: input.sourceStage ?? "queued",
      exportRowCount: input.exportRowCount ?? allItems.length,
      exportDownloadStatus: input.exportDownloadStatus ?? "parsed",
      sourceMetadata: input.sourceMetadata ?? {},
      lastError: null,
      updatedAt: now,
    })
    .where(eq(importRuns.id, input.importRunId))
    .run();

  const detail = await getImportRunDetail(input.importRunId);

  if (!detail) {
    throw new Error(`Unable to reload import run after item creation: ${input.importRunId}`);
  }

  return {
    detail,
    insertedCount,
  };
}

export async function getImportRunItemCount(importRunId: string) {
  const db = getDb();
  const row = db
    .select({
      count: sql<number>`count(*)`,
    })
    .from(importRunItems)
    .where(eq(importRunItems.importRunId, importRunId))
    .get() as { count: number } | undefined;

  return row?.count ?? 0;
}

export async function insertImportRunItemsBatch(input: {
  importRunId: string;
  bidIds: string[];
  startPosition: number;
}) {
  if (input.bidIds.length === 0) {
    return {
      insertedCount: 0,
      nextPosition: input.startPosition,
    };
  }

  const db = getDb();
  const now = nowIso();
  const chunks = splitIntoChunks(input.bidIds, 1000);
  let currentPosition = input.startPosition;

  for (const chunk of chunks) {
    const rows = chunk.map((bidId) => {
      const row = {
        id: createId(),
        importRunId: input.importRunId,
        bidId,
        position: currentPosition,
        status: "queued" as ImportRunItemStatus,
        createdAt: now,
        updatedAt: now,
      };
      currentPosition += 1;
      return row;
    });

    db.insert(importRunItems).values(rows).run();
  }

  return {
    insertedCount: input.bidIds.length,
    nextPosition: currentPosition,
  };
}

export async function updateImportRunTotals(input: {
  importRunId: string;
  totalFound: number;
}) {
  const db = getDb();
  const now = nowIso();

  db.update(importRuns)
    .set({
      totalFound: input.totalFound,
      updatedAt: now,
    })
    .where(eq(importRuns.id, input.importRunId))
    .run();
}

export async function getImportSourceCheckpoint(sourceKey: string) {
  const db = getDb();
  const row = db
    .select()
    .from(importSourceCheckpoints)
    .where(eq(importSourceCheckpoints.sourceKey, sourceKey))
    .get() as ImportSourceCheckpointRow | undefined;
  return row ?? null;
}

export async function upsertImportSourceCheckpoint(input: {
  sourceKey: string;
  sourceType: string;
  lastSuccessfulBidDt: string | null;
  sourceMetadata?: Record<string, unknown>;
}) {
  const db = getDb();
  const now = nowIso();
  const existing = await getImportSourceCheckpoint(input.sourceKey);

  if (existing) {
    db.update(importSourceCheckpoints)
      .set({
        sourceType: input.sourceType,
        lastSuccessfulBidDt: input.lastSuccessfulBidDt,
        sourceMetadata: input.sourceMetadata ?? {},
        updatedAt: now,
      })
      .where(eq(importSourceCheckpoints.sourceKey, input.sourceKey))
      .run();
  } else {
    db.insert(importSourceCheckpoints)
      .values({
        sourceKey: input.sourceKey,
        sourceType: input.sourceType,
        lastSuccessfulBidDt: input.lastSuccessfulBidDt,
        sourceMetadata: input.sourceMetadata ?? {},
        createdAt: now,
        updatedAt: now,
      })
      .run();
  }
}

export async function claimImportRunProcessing(input: {
  importRunId: string;
  leaseSeconds?: number;
}) {
  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const leaseExpiresAt = addSeconds(now, input.leaseSeconds ?? 60);
  const row = sqlite.transaction(() => {
    const run = db
      .select()
      .from(importRuns)
      .where(eq(importRuns.id, input.importRunId))
      .get() as ImportRunRow | undefined;

    if (!run) {
      throw new Error(`Unable to claim import run processing: ${input.importRunId} not found.`);
    }

    const terminalStatuses = new Set<ImportRunStatus>([
      "completed",
      "completed_with_errors",
      "failed",
      "cancelled",
    ]);
    const activeLease =
      toTimestamp(run.processorLeaseExpiresAt) !== null &&
      (toTimestamp(run.processorLeaseExpiresAt) as number) > (toTimestamp(now) as number);
    const shouldProcess = !terminalStatuses.has(run.status as ImportRunStatus) && !activeLease;
    const items = db
      .select()
      .from(importRunItems)
      .where(eq(importRunItems.importRunId, input.importRunId))
      .all() as ImportRunItemRow[];
    const totalProcessed = items.filter((item) => {
      return item.status === "completed" || item.status === "failed";
    }).length;

    if (shouldProcess) {
      const hasAvailableItems = items.some((item) => {
        const leaseMs = toTimestamp(item.leaseExpiresAt);
        const nowMs = toTimestamp(now);
        return (
          item.status === "queued" ||
          (item.status === "running" &&
            leaseMs !== null &&
            nowMs !== null &&
            leaseMs <= nowMs)
        );
      });
      db.update(importRuns)
        .set({
          status: "running",
          sourceStage:
            run.sourceStage === "queued" && hasAvailableItems ? "processing" : run.sourceStage,
          processorLeaseExpiresAt: leaseExpiresAt,
          updatedAt: now,
        })
        .where(eq(importRuns.id, run.id))
        .run();
    }

    return {
      id: run.id,
      status: run.status as ImportRunStatus,
      shouldProcess,
      forceRefresh: run.forceRefresh,
      totalItems: items.length,
      totalProcessed,
      lastError: run.lastError,
    };
  })();

  return {
    id: row.id,
    status: row.status as ImportRunStatus,
    shouldProcess: row.shouldProcess,
    forceRefresh: row.forceRefresh,
    totalItems: row.totalItems,
    totalProcessed: row.totalProcessed,
    lastError: row.lastError,
  };
}

export async function claimImportRunItems(input: {
  importRunId: string;
  batchSize?: number;
  leaseSeconds?: number;
}) {
  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const leaseExpiresAt = addSeconds(now, input.leaseSeconds ?? 180);
  const batchSize = Math.max(1, input.batchSize ?? 10);

  return sqlite.transaction(() => {
    const rows = db
      .select()
      .from(importRunItems)
      .where(eq(importRunItems.importRunId, input.importRunId))
      .all() as ImportRunItemRow[];
    const nowMs = toTimestamp(now) ?? 0;

    for (const row of rows) {
      const leaseMs = toTimestamp(row.leaseExpiresAt);
      if (row.status === "running" && leaseMs !== null && leaseMs <= nowMs) {
        db.update(importRunItems)
          .set({
            status: "queued",
            leaseExpiresAt: null,
            updatedAt: now,
          })
          .where(eq(importRunItems.id, row.id))
          .run();
      }
    }

    const available = db
      .select()
      .from(importRunItems)
      .where(eq(importRunItems.importRunId, input.importRunId))
      .all() as ImportRunItemRow[];
    const claimed = available
      .filter((row) => row.status === "queued")
      .sort((left, right) => left.position - right.position)
      .slice(0, batchSize);

    for (const row of claimed) {
      db.update(importRunItems)
        .set({
          status: "running",
          startedAt: now,
          leaseExpiresAt,
          attemptCount: row.attemptCount + 1,
          updatedAt: now,
        })
        .where(eq(importRunItems.id, row.id))
        .run();
    }

    return claimed.map((item) => ({
      id: item.id,
      bidId: item.bidId,
      position: item.position,
    }));
  })();
}

export async function completeImportRunItem(input: {
  itemId: string;
  resolution: Exclude<ImportRunItemResolution, "failed" | "skipped">;
  investigationId: string | null;
}) {
  const db = getDb();
  const now = nowIso();
  db.update(importRunItems)
    .set({
      status: "completed",
      resolution: input.resolution,
      errorMessage: null,
      investigationId: input.investigationId,
      completedAt: now,
      leaseExpiresAt: null,
      updatedAt: now,
    })
    .where(eq(importRunItems.id, input.itemId))
    .run();
}

export async function failImportRunItem(input: {
  itemId: string;
  investigationId: string | null;
  errorMessage: string;
}) {
  const db = getDb();
  const now = nowIso();
  db.update(importRunItems)
    .set({
      status: "failed",
      resolution: "failed",
      errorMessage: input.errorMessage,
      investigationId: input.investigationId,
      completedAt: now,
      leaseExpiresAt: null,
      updatedAt: now,
    })
    .where(eq(importRunItems.id, input.itemId))
    .run();
}

export async function finalizeImportRun(importRunId: string) {
  const db = getDb();
  const now = nowIso();
  const run = await getImportRunRow(importRunId);

  if (!run) {
    throw new Error(`Unable to finalize import run: ${importRunId} not found.`);
  }

  const items = await getImportRunItemRows(importRunId);
  const progress = calculateProgress(items);
  let nextStatus = deriveImportRunStatus(items);

  if (items.length === 0 && run.sourceStage === "queued") {
    nextStatus = "completed";
  }

  const nextSourceStage =
    nextStatus === "completed" || nextStatus === "completed_with_errors"
      ? ("completed" as const)
      : items.length > 0
        ? ("processing" as const)
        : (run.sourceStage as ImportRunSourceStage);

  db.update(importRuns)
    .set({
      status: nextStatus,
      sourceStage: nextSourceStage,
      totalFound: progress.totalItems,
      totalProcessed: progress.completedCount + progress.failedCount,
      completedAt:
        nextStatus === "completed" || nextStatus === "completed_with_errors" ? now : null,
      processorLeaseExpiresAt: null,
      updatedAt: now,
    })
    .where(eq(importRuns.id, importRunId))
    .run();

  return getImportRunDetail(importRunId);
}

export async function markImportRunFailed(importRunId: string, errorMessage: string) {
  const db = getDb();
  const now = nowIso();
  db.update(importRuns)
    .set({
      status: "failed",
      sourceStage: "failed",
      exportDownloadStatus: "failed",
      lastError: errorMessage,
      processorLeaseExpiresAt: null,
      completedAt: now,
      updatedAt: now,
    })
    .where(eq(importRuns.id, importRunId))
    .run();

  return getImportRunDetail(importRunId);
}

export async function resetFailedImportRunItems(input: {
  importRunId: string;
  forceRefresh: boolean;
}) {
  const db = getDb();
  const now = nowIso();
  const existingItems = await getImportRunItemRows(input.importRunId);
  const itemsToRequeue = existingItems.filter((item) => {
    return item.status === "failed" || item.status === "running";
  });
  const hasPendingWork = existingItems.some((item) => {
    return item.status === "queued" || item.status === "failed" || item.status === "running";
  });

  if (!hasPendingWork) {
    return getImportRunDetail(input.importRunId);
  }

  const itemIdsToRequeue = itemsToRequeue.map((item) => item.id);
  if (itemIdsToRequeue.length > 0) {
    db.update(importRunItems)
      .set({
        status: "queued",
        resolution: null,
        errorMessage: null,
        investigationId: null,
        startedAt: null,
        completedAt: null,
        leaseExpiresAt: null,
        updatedAt: now,
      })
      .where(inArray(importRunItems.id, itemIdsToRequeue))
      .run();
  }

  db.update(importRuns)
    .set({
      status: "queued",
      sourceStage: "queued",
      forceRefresh: input.forceRefresh,
      lastError: null,
      completedAt: null,
      processorLeaseExpiresAt: null,
      updatedAt: now,
    })
    .where(eq(importRuns.id, input.importRunId))
    .run();

  return getImportRunDetail(input.importRunId);
}

export async function getImportRunBidIds(importRunId: string) {
  const items = await getImportRunItemRows(importRunId);
  return items.map((item) => item.bidId);
}

export async function getImportRuns(options: {
  page: number;
  pageSize: number;
  status?: ImportRunStatus;
  sourceType?: ImportRunSourceType;
}) {
  const db = getDb();
  const offset = (options.page - 1) * options.pageSize;

  let query = db.select().from(importRuns);

  // Note: For a real production app we'd add where clauses here using drizzle-orm eq/and
  // but for this SQLite implementation we'll handle basic filtering and sorting.
  
  const allRuns = query.all() as ImportRunRow[];
  
  // Apply filters in-memory for this SQLite implementation
  let filtered = allRuns;
  if (options.status) {
    filtered = filtered.filter(r => r.status === options.status);
  }
  if (options.sourceType) {
    filtered = filtered.filter(r => r.sourceType === options.sourceType);
  }

  // Sort by createdAt desc
  filtered.sort((a, b) => {
    return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
  });

  const total = filtered.length;
  const items = filtered.slice(offset, offset + options.pageSize);

  // Convert to detailed view
  const detailedItems = await Promise.all(
    items.map(async (run) => {
      const progress = await getImportRunProgress(run.id);
      return {
        id: run.id,
        sourceType: run.sourceType as ImportRunSourceType,
        triggerType: run.triggerType as "manual" | "scheduled",
        scheduleId: run.scheduleId,
        sourceStage: run.sourceStage as ImportRunSourceStage,
        status: run.status as ImportRunStatus,
        forceRefresh: run.forceRefresh,
        notes: run.notes,
        lastError: run.lastError,
        sourceWindowStart: run.sourceWindowStart,
        sourceWindowEnd: run.sourceWindowEnd,
        exportJobId: run.exportJobId,
        exportRowCount: run.exportRowCount,
        exportDownloadStatus: run.exportDownloadStatus as ImportRunExportDownloadStatus | null,
        sourceMetadata: normalizeSourceMetadata(
          (run.sourceMetadata ?? null) as Record<string, unknown> | null,
        ),
        startedAt: run.startedAt,
        completedAt: run.completedAt,
        createdAt: run.createdAt,
        updatedAt: run.updatedAt,
        ...progress,
      };
    })
  );

  return {
    items: detailedItems,
    total,
    page: options.page,
    pageSize: options.pageSize,
  };
}
