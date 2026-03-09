import "server-only";

import { getInvestigationListItemsByIds } from "@/lib/db/investigations";
import { getSupabaseAdminClient } from "@/lib/db/server";
import type { InvestigationListItem } from "@/types/bid";
import type {
  ImportRunDetail,
  ImportRunItemResolution,
  ImportRunItemStatus,
  ImportRunStatus,
} from "@/types/import-run";

interface ImportRunRow {
  id: string;
  source_type: string;
  status: ImportRunStatus;
  force_refresh: boolean;
  notes: string | null;
  last_error: string | null;
  total_found: number;
  total_processed: number;
  started_at: string | null;
  completed_at: string | null;
  processor_lease_expires_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ImportRunItemRow {
  id: string;
  import_run_id: string;
  bid_id: string;
  position: number;
  status: ImportRunItemStatus;
  resolution: ImportRunItemResolution | null;
  error_message: string | null;
  investigation_id: string | null;
  started_at: string | null;
  completed_at: string | null;
  attempt_count: number;
  lease_expires_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ClaimedImportRunRow {
  id: string;
  status: ImportRunStatus;
  should_process: boolean;
  force_refresh: boolean;
  total_items: number;
  total_processed: number;
  last_error: string | null;
}

interface ClaimedImportRunItemRow {
  id: string;
  bid_id: string;
  position: number;
}

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

function mapImportRunItem(
  row: ImportRunItemRow,
  investigationsById: Map<string, InvestigationListItem>,
) {
  return {
    id: row.id,
    importRunId: row.import_run_id,
    bidId: row.bid_id,
    position: row.position,
    status: row.status,
    resolution: row.resolution,
    errorMessage: row.error_message,
    investigationId: row.investigation_id,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    attemptCount: row.attempt_count,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    investigation: row.investigation_id
      ? investigationsById.get(row.investigation_id) ?? null
      : null,
  };
}

async function getImportRunRow(importRunId: string) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_runs")
    .select("*")
    .eq("id", importRunId)
    .single();

  if (error) {
    if (error.code === "PGRST116") {
      return null;
    }

    throw new Error(`Unable to fetch import run: ${error.message}`);
  }

  return data as ImportRunRow;
}

async function getImportRunItemRows(importRunId: string) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_run_items")
    .select("*")
    .eq("import_run_id", importRunId)
    .order("position", { ascending: true });

  if (error) {
    throw new Error(`Unable to fetch import run items: ${error.message}`);
  }

  return (data ?? []) as ImportRunItemRow[];
}

export async function createImportRun(input: {
  sourceType: string;
  bidIds: string[];
  forceRefresh: boolean;
  notes?: string;
}) {
  const bidIds = dedupeBidIds(input.bidIds);
  const supabase = getSupabaseAdminClient();

  const { data, error } = await supabase
    .from("import_runs")
    .insert({
      source_type: input.sourceType,
      status: "queued",
      force_refresh: input.forceRefresh,
      notes: input.notes ?? null,
      total_found: bidIds.length,
      total_processed: 0,
      started_at: new Date().toISOString(),
    })
    .select("*")
    .single();

  if (error || !data) {
    throw new Error(`Unable to create import run: ${error?.message ?? "unknown error"}`);
  }

  const run = data as ImportRunRow;

  if (bidIds.length > 0) {
    const { error: itemsError } = await supabase.from("import_run_items").insert(
      bidIds.map((bidId, index) => ({
        import_run_id: run.id,
        bid_id: bidId,
        position: index + 1,
        status: "queued",
      })),
    );

    if (itemsError) {
      await supabase
        .from("import_runs")
        .update({
          status: "failed",
          last_error: itemsError.message,
          completed_at: new Date().toISOString(),
        })
        .eq("id", run.id);

      throw new Error(`Unable to create import run items: ${itemsError.message}`);
    }
  }

  return run.id;
}

export async function getImportRunDetail(importRunId: string): Promise<ImportRunDetail | null> {
  const run = await getImportRunRow(importRunId);

  if (!run) {
    return null;
  }

  const itemRows = await getImportRunItemRows(importRunId);
  const investigationIds = itemRows
    .map((item) => item.investigation_id)
    .filter((value): value is string => Boolean(value));
  const investigations = await getInvestigationListItemsByIds(investigationIds);
  const investigationsById = new Map(
    investigations.map((investigation) => [investigation.id, investigation]),
  );
  const progress = calculateProgress(itemRows);

  return {
    id: run.id,
    sourceType: run.source_type,
    status: run.status,
    forceRefresh: run.force_refresh,
    notes: run.notes,
    lastError: run.last_error,
    startedAt: run.started_at,
    completedAt: run.completed_at,
    createdAt: run.created_at,
    updatedAt: run.updated_at,
    ...progress,
    items: itemRows.map((item) => mapImportRunItem(item, investigationsById)),
  };
}

export async function claimImportRunProcessing(input: {
  importRunId: string;
  leaseSeconds?: number;
}) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase.rpc("claim_import_run_processing", {
    p_import_run_id: input.importRunId,
    p_lease_seconds: input.leaseSeconds ?? 60,
  });

  if (error) {
    throw new Error(`Unable to claim import run processing: ${error.message}`);
  }

  const row = (data?.[0] ?? null) as ClaimedImportRunRow | null;

  if (!row) {
    throw new Error("Unable to claim import run processing: missing claim row.");
  }

  return {
    id: row.id,
    status: row.status,
    shouldProcess: row.should_process,
    forceRefresh: row.force_refresh,
    totalItems: row.total_items,
    totalProcessed: row.total_processed,
    lastError: row.last_error,
  };
}

export async function claimImportRunItems(input: {
  importRunId: string;
  batchSize?: number;
  leaseSeconds?: number;
}) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase.rpc("claim_import_run_items", {
    p_import_run_id: input.importRunId,
    p_batch_size: input.batchSize ?? 10,
    p_lease_seconds: input.leaseSeconds ?? 180,
  });

  if (error) {
    throw new Error(`Unable to claim import run items: ${error.message}`);
  }

  return ((data ?? []) as ClaimedImportRunItemRow[]).map((item) => ({
    id: item.id,
    bidId: item.bid_id,
    position: item.position,
  }));
}

export async function completeImportRunItem(input: {
  itemId: string;
  resolution: Exclude<ImportRunItemResolution, "failed" | "skipped">;
  investigationId: string | null;
}) {
  const supabase = getSupabaseAdminClient();
  const { error } = await supabase
    .from("import_run_items")
    .update({
      status: "completed",
      resolution: input.resolution,
      error_message: null,
      investigation_id: input.investigationId,
      completed_at: new Date().toISOString(),
      lease_expires_at: null,
    })
    .eq("id", input.itemId);

  if (error) {
    throw new Error(`Unable to complete import run item: ${error.message}`);
  }
}

export async function failImportRunItem(input: {
  itemId: string;
  investigationId: string | null;
  errorMessage: string;
}) {
  const supabase = getSupabaseAdminClient();
  const { error } = await supabase
    .from("import_run_items")
    .update({
      status: "failed",
      resolution: "failed",
      error_message: input.errorMessage,
      investigation_id: input.investigationId,
      completed_at: new Date().toISOString(),
      lease_expires_at: null,
    })
    .eq("id", input.itemId);

  if (error) {
    throw new Error(`Unable to fail import run item: ${error.message}`);
  }
}

export async function finalizeImportRun(importRunId: string) {
  const supabase = getSupabaseAdminClient();
  const run = await getImportRunRow(importRunId);

  if (!run) {
    throw new Error(`Unable to finalize import run: ${importRunId} not found.`);
  }

  const items = await getImportRunItemRows(importRunId);
  const progress = calculateProgress(items);
  const nextStatus = deriveImportRunStatus(items);

  const { error } = await supabase
    .from("import_runs")
    .update({
      status: nextStatus,
      total_found: progress.totalItems,
      total_processed: progress.completedCount + progress.failedCount,
      completed_at:
        nextStatus === "completed" || nextStatus === "completed_with_errors"
          ? new Date().toISOString()
          : null,
      processor_lease_expires_at: null,
    })
    .eq("id", importRunId);

  if (error) {
    throw new Error(`Unable to finalize import run: ${error.message}`);
  }

  return getImportRunDetail(importRunId);
}

export async function markImportRunFailed(importRunId: string, errorMessage: string) {
  const supabase = getSupabaseAdminClient();
  const { error } = await supabase
    .from("import_runs")
    .update({
      status: "failed",
      last_error: errorMessage,
      processor_lease_expires_at: null,
      completed_at: new Date().toISOString(),
    })
    .eq("id", importRunId);

  if (error) {
    throw new Error(`Unable to mark import run failed: ${error.message}`);
  }

  return getImportRunDetail(importRunId);
}

export async function resetFailedImportRunItems(input: {
  importRunId: string;
  forceRefresh: boolean;
}) {
  const supabase = getSupabaseAdminClient();
  const existingItems = await getImportRunItemRows(input.importRunId);
  const failedItems = existingItems.filter((item) => item.status === "failed");

  if (failedItems.length === 0) {
    return getImportRunDetail(input.importRunId);
  }

  const { error: updateItemsError } = await supabase
    .from("import_run_items")
    .update({
      status: "queued",
      resolution: null,
      error_message: null,
      investigation_id: null,
      started_at: null,
      completed_at: null,
      lease_expires_at: null,
    })
    .eq("import_run_id", input.importRunId)
    .eq("status", "failed");

  if (updateItemsError) {
    throw new Error(`Unable to reset failed import run items: ${updateItemsError.message}`);
  }

  const { error: updateRunError } = await supabase
    .from("import_runs")
    .update({
      status: "queued",
      force_refresh: input.forceRefresh,
      last_error: null,
      completed_at: null,
      processor_lease_expires_at: null,
    })
    .eq("id", input.importRunId);

  if (updateRunError) {
    throw new Error(`Unable to reset import run: ${updateRunError.message}`);
  }

  return getImportRunDetail(input.importRunId);
}

export async function getImportRunBidIds(importRunId: string) {
  const items = await getImportRunItemRows(importRunId);
  return items.map((item) => item.bid_id);
}
