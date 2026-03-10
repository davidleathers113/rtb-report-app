import "server-only";

import { getSupabaseAdminClient } from "@/lib/db/server";
import { getImportRunDetail } from "@/lib/db/import-runs";
import type {
  ImportScheduleDetail,
  ImportScheduleHealthStatus,
  ImportScheduleRunHistoryPage,
  ImportScheduleRunHistoryStatusFilter,
  ImportScheduleRunSummary,
  ImportScheduleAnalytics,
  ImportScheduleSourceType,
} from "@/types/import-schedule";
import type {
  ImportRunSourceStage,
  ImportRunStatus,
} from "@/types/import-run";

const SCHEDULE_STALE_RUN_MINUTES = 30;
const NO_RECENT_SUCCESS_MULTIPLIER = 3;
const ANALYTICS_RECENT_RUN_LIMIT = 25;
const IMPORT_SCHEDULE_RUN_SUMMARY_SELECT =
  "id, schedule_id, trigger_type, status, source_stage, total_found, total_processed, last_error, source_metadata, source_window_start, source_window_end, processor_lease_expires_at, completed_at, created_at, updated_at";

interface ImportScheduleRow {
  id: string;
  name: string;
  is_enabled: boolean;
  account_id: string;
  source_type: ImportScheduleSourceType;
  window_minutes: 5 | 15 | 60;
  overlap_minutes: number;
  max_concurrent_runs: number;
  last_triggered_at: string | null;
  last_succeeded_at: string | null;
  last_failed_at: string | null;
  last_error: string | null;
  consecutive_failure_count: number;
  last_terminal_run_created_at: string | null;
  alert_state: Record<string, unknown> | null;
  trigger_lease_expires_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ClaimedImportScheduleRow {
  id: string;
  name: string;
  is_enabled: boolean;
  account_id: string;
  source_type: ImportScheduleSourceType;
  window_minutes: 5 | 15 | 60;
  overlap_minutes: number;
  max_concurrent_runs: number;
  last_triggered_at: string | null;
  last_succeeded_at: string | null;
  last_failed_at: string | null;
  last_error: string | null;
  consecutive_failure_count: number;
}

interface ImportRunScheduleSummaryRow {
  id: string;
  schedule_id: string | null;
  trigger_type: "manual" | "scheduled";
  status: ImportRunStatus;
  source_stage: ImportRunSourceStage;
  total_found: number;
  total_processed: number;
  last_error: string | null;
  source_metadata: Record<string, unknown> | null;
  source_window_start: string | null;
  source_window_end: string | null;
  processor_lease_expires_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface ImportScheduleAlertState {
  repeatedFailures?: {
    sentAt?: string;
    consecutiveFailureCount?: number;
  };
  stale?: {
    sentAt?: string;
    runId?: string;
  };
  noRecentSuccess?: {
    sentAt?: string;
    referenceAt?: string;
  };
  sourceStageHardFailure?: {
    sentAt?: string;
    runId?: string;
    failedStage?: string | null;
  };
}

function mapSchedule(row: ImportScheduleRow) {
  return {
    id: row.id,
    name: row.name,
    isEnabled: row.is_enabled,
    accountId: row.account_id,
    sourceType: row.source_type,
    windowMinutes: row.window_minutes,
    overlapMinutes: row.overlap_minutes,
    maxConcurrentRuns: row.max_concurrent_runs,
    lastTriggeredAt: row.last_triggered_at,
    lastSucceededAt: row.last_succeeded_at,
    lastFailedAt: row.last_failed_at,
    lastError: row.last_error,
    consecutiveFailureCount: row.consecutive_failure_count,
    isNoRecentSuccess: false,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function normalizeAlertState(
  value: Record<string, unknown> | null,
): ImportScheduleAlertState {
  if (!value || Array.isArray(value)) {
    return {};
  }

  return value as ImportScheduleAlertState;
}

function getRunDiagnostics(row: Pick<ImportRunScheduleSummaryRow, "source_metadata">) {
  const metadata = row.source_metadata;
  if (!metadata || Array.isArray(metadata)) {
    return {};
  }

  const diagnostics = metadata.diagnostics;
  if (!diagnostics || typeof diagnostics !== "object" || Array.isArray(diagnostics)) {
    return {};
  }

  return diagnostics as Record<string, unknown>;
}

function readNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readString(value: unknown) {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function calculateDurationMs(startedAt: string | null, endedAt: string | null) {
  if (!startedAt || !endedAt) {
    return null;
  }

  const start = new Date(startedAt).getTime();
  const end = new Date(endedAt).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) {
    return null;
  }

  return end - start;
}

function averageNumbers(values: Array<number | null>) {
  let total = 0;
  let count = 0;

  for (const value of values) {
    if (value === null) {
      continue;
    }

    total += value;
    count += 1;
  }

  if (count === 0) {
    return null;
  }

  return Math.round(total / count);
}

function incrementBreakdown(
  map: Map<string, number>,
  label: string | null,
) {
  if (!label) {
    return;
  }

  map.set(label, (map.get(label) ?? 0) + 1);
}

function toBreakdownItems(map: Map<string, number>) {
  return Array.from(map.entries())
    .sort((left, right) => right[1] - left[1])
    .slice(0, 5)
    .map(([label, count]) => ({ label, count }));
}

function getExpectedNoSuccessThresholdMinutes(windowMinutes: number) {
  return Math.max(windowMinutes * NO_RECENT_SUCCESS_MULTIPLIER, windowMinutes + 10);
}

function isNoRecentSuccess(input: {
  isEnabled: boolean;
  lastSucceededAt: string | null;
  windowMinutes: number;
  activeRun: ImportScheduleRunSummary | null;
}) {
  if (!input.isEnabled || input.activeRun) {
    return false;
  }

  if (!input.lastSucceededAt) {
    return false;
  }

  const lastSuccess = new Date(input.lastSucceededAt).getTime();
  if (Number.isNaN(lastSuccess)) {
    return false;
  }

  const thresholdMinutes = getExpectedNoSuccessThresholdMinutes(input.windowMinutes);
  return lastSuccess <= Date.now() - thresholdMinutes * 60 * 1000;
}

function isScheduleRunStale(row: Pick<
  ImportRunScheduleSummaryRow,
  "status" | "updated_at" | "processor_lease_expires_at"
>) {
  if (!["queued", "running"].includes(row.status)) {
    return false;
  }

  const now = Date.now();
  const updatedAt = new Date(row.updated_at).getTime();
  const processorLeaseExpiresAt = row.processor_lease_expires_at
    ? new Date(row.processor_lease_expires_at).getTime()
    : null;
  const isLeaseActive =
    processorLeaseExpiresAt !== null && !Number.isNaN(processorLeaseExpiresAt)
      ? processorLeaseExpiresAt > now
      : false;

  if (isLeaseActive) {
    return false;
  }

  if (Number.isNaN(updatedAt)) {
    return false;
  }

  return updatedAt <= now - SCHEDULE_STALE_RUN_MINUTES * 60 * 1000;
}

function mapRunSummary(row: ImportRunScheduleSummaryRow): ImportScheduleRunSummary {
  const diagnostics = getRunDiagnostics(row);
  const failedStage = readString(diagnostics.failedStage);
  const sourceStageError = readString(diagnostics.sourceStageError);
  const durationMs = calculateDurationMs(row.created_at, row.completed_at ?? row.updated_at);
  const exportReadyLatencyMs = readNumber(diagnostics.exportReadyLatencyMs);

  return {
    id: row.id,
    triggerType: row.trigger_type,
    status: row.status,
    sourceStage: row.source_stage,
    totalFound: row.total_found,
    totalProcessed: row.total_processed,
    lastError: row.last_error,
    sourceWindowStart: row.source_window_start,
    sourceWindowEnd: row.source_window_end,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    completedAt: row.completed_at,
    isStale: isScheduleRunStale(row),
    durationMs,
    exportReadyLatencyMs,
    failedStage,
    failureReason: sourceStageError ?? row.last_error,
  };
}

function buildHealthStatus(input: {
  isEnabled: boolean;
  windowMinutes: number;
  activeRun: ImportScheduleRunSummary | null;
  lastSucceededAt: string | null;
  lastFailedAt: string | null;
  consecutiveFailureCount: number;
}) {
  if (!input.isEnabled) {
    return {
      healthStatus: "disabled" as ImportScheduleHealthStatus,
      healthSummary: "Schedule is disabled.",
    };
  }

  if (input.activeRun?.isStale) {
    return {
      healthStatus: "stale" as ImportScheduleHealthStatus,
      healthSummary: "Active scheduled run appears stale and may need intervention.",
      isNoRecentSuccess: false,
    };
  }

  const noRecentSuccess = isNoRecentSuccess({
    isEnabled: input.isEnabled,
    lastSucceededAt: input.lastSucceededAt,
    windowMinutes: input.windowMinutes,
    activeRun: input.activeRun,
  });

  if (input.consecutiveFailureCount >= 3) {
    return {
      healthStatus: "failing" as ImportScheduleHealthStatus,
      healthSummary: `Schedule has failed ${input.consecutiveFailureCount} times in a row.`,
      isNoRecentSuccess: noRecentSuccess,
    };
  }

  if (input.consecutiveFailureCount > 0) {
    return {
      healthStatus: "warning" as ImportScheduleHealthStatus,
      healthSummary: `Schedule has ${input.consecutiveFailureCount} recent consecutive failure${input.consecutiveFailureCount === 1 ? "" : "s"}.`,
      isNoRecentSuccess: noRecentSuccess,
    };
  }

  if (noRecentSuccess) {
    const thresholdMinutes = getExpectedNoSuccessThresholdMinutes(input.windowMinutes);
    return {
      healthStatus: "warning" as ImportScheduleHealthStatus,
      healthSummary: `No successful run within the expected ${thresholdMinutes}-minute window.`,
      isNoRecentSuccess: true,
    };
  }

  if (input.activeRun) {
    return {
      healthStatus: "healthy" as ImportScheduleHealthStatus,
      healthSummary: "Schedule has an active run in progress.",
      isNoRecentSuccess: false,
    };
  }

  if (input.lastSucceededAt) {
    return {
      healthStatus: "healthy" as ImportScheduleHealthStatus,
      healthSummary: "Schedule is healthy.",
      isNoRecentSuccess: false,
    };
  }

  if (input.lastFailedAt) {
    return {
      healthStatus: "warning" as ImportScheduleHealthStatus,
      healthSummary: "Schedule has failed recently and has not recovered yet.",
      isNoRecentSuccess: false,
    };
  }

  return {
    healthStatus: "warning" as ImportScheduleHealthStatus,
    healthSummary: "Schedule has not completed a run yet.",
    isNoRecentSuccess: false,
  };
}

async function getScheduleRows() {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_schedules")
    .select("*")
    .order("created_at", { ascending: false });

  if (error) {
    throw new Error(`Unable to fetch import schedules: ${error.message}`);
  }

  return (data ?? []) as ImportScheduleRow[];
}

async function getScheduleRow(scheduleId: string) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_schedules")
    .select("*")
    .eq("id", scheduleId)
    .single();

  if (error) {
    if (error.code === "PGRST116") {
      return null;
    }

    throw new Error(`Unable to fetch import schedule: ${error.message}`);
  }

  return data as ImportScheduleRow;
}

async function getScheduleRunRows(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as ImportRunScheduleSummaryRow[];
  }

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_runs")
    .select(IMPORT_SCHEDULE_RUN_SUMMARY_SELECT)
    .in("schedule_id", scheduleIds)
    .eq("trigger_type", "scheduled")
    .in("status", ["queued", "running"])
    .order("created_at", { ascending: false });

  if (error) {
    throw new Error(`Unable to fetch scheduled runs by schedule: ${error.message}`);
  }

  return (data ?? []) as ImportRunScheduleSummaryRow[];
}

async function getRecentRunRowsBySchedule(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as ImportRunScheduleSummaryRow[];
  }

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_runs")
    .select(IMPORT_SCHEDULE_RUN_SUMMARY_SELECT)
    .in("schedule_id", scheduleIds)
    .eq("trigger_type", "scheduled")
    .order("created_at", { ascending: false })
    .limit(Math.max(ANALYTICS_RECENT_RUN_LIMIT, scheduleIds.length * 10));

  if (error) {
    throw new Error(`Unable to fetch recent scheduled runs: ${error.message}`);
  }

  return (data ?? []) as ImportRunScheduleSummaryRow[];
}

async function getRunCountRowsBySchedule(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as Array<{ schedule_id: string | null }>;
  }

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_runs")
    .select("schedule_id")
    .in("schedule_id", scheduleIds)
    .eq("trigger_type", "scheduled");

  if (error) {
    throw new Error(`Unable to fetch schedule run counts: ${error.message}`);
  }

  return (data ?? []) as Array<{ schedule_id: string | null }>;
}

function groupRecentRunsBySchedule(rows: ImportRunScheduleSummaryRow[]) {
  const recentRunsBySchedule = new Map<string, ImportScheduleRunSummary[]>();

  for (const row of rows) {
    const scheduleId = row.schedule_id;
    if (!scheduleId) {
      continue;
    }

    const current = recentRunsBySchedule.get(scheduleId) ?? [];
    if (current.length >= 5) {
      continue;
    }

    current.push(mapRunSummary(row));
    recentRunsBySchedule.set(scheduleId, current);
  }

  return recentRunsBySchedule;
}

function countRunsBySchedule(rows: Array<{ schedule_id: string | null }>) {
  const counts = new Map<string, number>();

  for (const row of rows) {
    if (!row.schedule_id) {
      continue;
    }

    counts.set(row.schedule_id, (counts.get(row.schedule_id) ?? 0) + 1);
  }

  return counts;
}

function buildAnalyticsBySchedule(rows: ImportRunScheduleSummaryRow[]) {
  const rowsBySchedule = new Map<string, ImportRunScheduleSummaryRow[]>();

  for (const row of rows) {
    if (!row.schedule_id) {
      continue;
    }

    const current = rowsBySchedule.get(row.schedule_id) ?? [];
    if (current.length >= ANALYTICS_RECENT_RUN_LIMIT) {
      continue;
    }

    current.push(row);
    rowsBySchedule.set(row.schedule_id, current);
  }

  const analyticsBySchedule = new Map<string, ImportScheduleAnalytics>();

  for (const [scheduleId, scheduleRows] of rowsBySchedule.entries()) {
    let successfulRunCount = 0;
    let failedRunCount = 0;
    let completedWithErrorsCount = 0;
    let runningRunCount = 0;
    let queuedRunCount = 0;
    let staleRunCount = 0;
    const runDurations: Array<number | null> = [];
    const exportReadyLatencies: Array<number | null> = [];
    const stageFailures = new Map<string, number>();
    const rootCauses = new Map<string, number>();

    for (const row of scheduleRows) {
      const summary = mapRunSummary(row);
      runDurations.push(summary.durationMs);
      exportReadyLatencies.push(summary.exportReadyLatencyMs);

      if (summary.status === "completed") {
        successfulRunCount += 1;
      }
      if (summary.status === "failed") {
        failedRunCount += 1;
      }
      if (summary.status === "completed_with_errors") {
        completedWithErrorsCount += 1;
      }
      if (summary.status === "running") {
        runningRunCount += 1;
      }
      if (summary.status === "queued") {
        queuedRunCount += 1;
      }
      if (summary.isStale) {
        staleRunCount += 1;
      }

      if (summary.status === "failed" || summary.status === "completed_with_errors") {
        incrementBreakdown(stageFailures, summary.failedStage ?? summary.sourceStage);
        incrementBreakdown(rootCauses, summary.failureReason);
      }
    }

    analyticsBySchedule.set(scheduleId, {
      recentRunCount: scheduleRows.length,
      successfulRunCount,
      failedRunCount,
      completedWithErrorsCount,
      runningRunCount,
      queuedRunCount,
      staleRunCount,
      averageRunDurationMs: averageNumbers(runDurations),
      averageExportReadyLatencyMs: averageNumbers(exportReadyLatencies),
      sourceStageFailureBreakdown: toBreakdownItems(stageFailures),
      rootCauseSummary: toBreakdownItems(rootCauses),
    });
  }

  return analyticsBySchedule;
}

export async function getImportSchedules(): Promise<ImportScheduleDetail[]> {
  const rows = await getScheduleRows();
  const scheduleIds = rows.map((row) => row.id);
  const activeRunRows = await getScheduleRunRows(scheduleIds);
  const recentRunRows = await getRecentRunRowsBySchedule(scheduleIds);
  const runCountRows = await getRunCountRowsBySchedule(scheduleIds);
  const activeRunsBySchedule = new Map<string, ImportScheduleRunSummary | null>();
  const recentRunsBySchedule = groupRecentRunsBySchedule(recentRunRows);
  const recentRunCountsBySchedule = countRunsBySchedule(runCountRows);
  const analyticsBySchedule = buildAnalyticsBySchedule(recentRunRows);

  for (const row of activeRunRows) {
    if (!row.schedule_id || activeRunsBySchedule.has(row.schedule_id)) {
      continue;
    }

    activeRunsBySchedule.set(row.schedule_id, mapRunSummary(row));
  }

  return rows.map((row) => ({
    ...(() => {
      const activeRun = activeRunsBySchedule.get(row.id) ?? null;
      const health = buildHealthStatus({
        isEnabled: row.is_enabled,
        windowMinutes: row.window_minutes,
        activeRun,
        lastSucceededAt: row.last_succeeded_at,
        lastFailedAt: row.last_failed_at,
        consecutiveFailureCount: row.consecutive_failure_count,
      });

      return {
        ...mapSchedule(row),
        ...health,
        activeRun,
        recentRuns: recentRunsBySchedule.get(row.id) ?? [],
        recentRunTotalCount: recentRunCountsBySchedule.get(row.id) ?? 0,
        analytics: analyticsBySchedule.get(row.id) ?? {
          recentRunCount: 0,
          successfulRunCount: 0,
          failedRunCount: 0,
          completedWithErrorsCount: 0,
          runningRunCount: 0,
          queuedRunCount: 0,
          staleRunCount: 0,
          averageRunDurationMs: null,
          averageExportReadyLatencyMs: null,
          sourceStageFailureBreakdown: [],
          rootCauseSummary: [],
        },
      };
    })(),
  })).sort((left, right) => {
    const order: Record<ImportScheduleHealthStatus, number> = {
      stale: 0,
      failing: 1,
      warning: 2,
      healthy: 3,
      disabled: 4,
    };

    if (order[left.healthStatus] !== order[right.healthStatus]) {
      return order[left.healthStatus] - order[right.healthStatus];
    }

    return left.name.localeCompare(right.name);
  });
}

export async function createImportSchedule(input: {
  name: string;
  isEnabled: boolean;
  accountId: string;
  sourceType: ImportScheduleSourceType;
  windowMinutes: 5 | 15 | 60;
  overlapMinutes: number;
  maxConcurrentRuns: number;
}) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_schedules")
    .insert({
      name: input.name,
      is_enabled: input.isEnabled,
      account_id: input.accountId,
      source_type: input.sourceType,
      window_minutes: input.windowMinutes,
      overlap_minutes: input.overlapMinutes,
      max_concurrent_runs: input.maxConcurrentRuns,
    })
    .select("*")
    .single();

  if (error || !data) {
    throw new Error(`Unable to create import schedule: ${error?.message ?? "unknown error"}`);
  }

  return {
    ...mapSchedule(data as ImportScheduleRow),
    healthStatus: input.isEnabled ? "warning" : "disabled",
    healthSummary: input.isEnabled
      ? "Schedule has not completed a run yet."
      : "Schedule is disabled.",
    isNoRecentSuccess: false,
    activeRun: null,
    recentRuns: [],
    recentRunTotalCount: 0,
    analytics: {
      recentRunCount: 0,
      successfulRunCount: 0,
      failedRunCount: 0,
      completedWithErrorsCount: 0,
      runningRunCount: 0,
      queuedRunCount: 0,
      staleRunCount: 0,
      averageRunDurationMs: null,
      averageExportReadyLatencyMs: null,
      sourceStageFailureBreakdown: [],
      rootCauseSummary: [],
    },
  } satisfies ImportScheduleDetail;
}

export async function updateImportSchedule(input: {
  scheduleId: string;
  name?: string;
  isEnabled?: boolean;
  windowMinutes?: 5 | 15 | 60;
  overlapMinutes?: number;
  maxConcurrentRuns?: number;
}) {
  const supabase = getSupabaseAdminClient();
  const updateData: Record<string, unknown> = {};

  if (input.name !== undefined) {
    updateData.name = input.name;
  }
  if (input.isEnabled !== undefined) {
    updateData.is_enabled = input.isEnabled;
  }
  if (input.windowMinutes !== undefined) {
    updateData.window_minutes = input.windowMinutes;
  }
  if (input.overlapMinutes !== undefined) {
    updateData.overlap_minutes = input.overlapMinutes;
  }
  if (input.maxConcurrentRuns !== undefined) {
    updateData.max_concurrent_runs = input.maxConcurrentRuns;
  }

  const { error } = await supabase
    .from("import_schedules")
    .update(updateData)
    .eq("id", input.scheduleId);

  if (error) {
    throw new Error(`Unable to update import schedule: ${error.message}`);
  }

  return getImportSchedules().then((schedules) => {
    const schedule = schedules.find((current) => current.id === input.scheduleId);
    if (!schedule) {
      throw new Error(`Import schedule not found after update: ${input.scheduleId}`);
    }

    return schedule;
  });
}

export async function getImportScheduleDetail(scheduleId: string) {
  const schedules = await getImportSchedules();
  return schedules.find((schedule) => schedule.id === scheduleId) ?? null;
}

export async function claimDueImportSchedules(input?: {
  limit?: number;
  leaseSeconds?: number;
  staleAfterMinutes?: number;
}) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase.rpc("claim_due_import_schedules", {
    p_limit: input?.limit ?? 10,
    p_lease_seconds: input?.leaseSeconds ?? 120,
    p_stale_after_minutes: input?.staleAfterMinutes ?? SCHEDULE_STALE_RUN_MINUTES,
  });

  if (error) {
    throw new Error(`Unable to claim due import schedules: ${error.message}`);
  }

  return ((data ?? []) as ClaimedImportScheduleRow[]).map((row) => ({
    id: row.id,
    name: row.name,
    isEnabled: row.is_enabled,
    accountId: row.account_id,
    sourceType: row.source_type,
    windowMinutes: row.window_minutes,
    overlapMinutes: row.overlap_minutes,
    maxConcurrentRuns: row.max_concurrent_runs,
    lastTriggeredAt: row.last_triggered_at,
    lastSucceededAt: row.last_succeeded_at,
    lastFailedAt: row.last_failed_at,
    lastError: row.last_error,
    consecutiveFailureCount: row.consecutive_failure_count,
  }));
}

export async function markImportScheduleTriggered(input: {
  scheduleId: string;
  clearError?: boolean;
}) {
  const supabase = getSupabaseAdminClient();
  const updateData: Record<string, unknown> = {
    last_triggered_at: new Date().toISOString(),
    trigger_lease_expires_at: null,
  };

  if (input.clearError === true) {
    updateData.last_error = null;
  }

  const { error } = await supabase
    .from("import_schedules")
    .update(updateData)
    .eq("id", input.scheduleId);

  if (error) {
    throw new Error(`Unable to mark import schedule triggered: ${error.message}`);
  }
}

export async function markImportScheduleRunSucceeded(input: {
  scheduleId: string;
  runCreatedAt: string;
  occurredAt?: string;
}) {
  const supabase = getSupabaseAdminClient();
  const current = await getScheduleRow(input.scheduleId);

  if (!current) {
    throw new Error(`Unable to mark import schedule success: ${input.scheduleId} not found.`);
  }

  const currentRunCreatedAtMs = current.last_terminal_run_created_at
    ? new Date(current.last_terminal_run_created_at).getTime()
    : null;
  const inputRunCreatedAtMs = new Date(input.runCreatedAt).getTime();

  if (Number.isNaN(inputRunCreatedAtMs)) {
    throw new Error("Unable to mark import schedule success: invalid runCreatedAt.");
  }

  if (
    currentRunCreatedAtMs !== null &&
    !Number.isNaN(currentRunCreatedAtMs) &&
    inputRunCreatedAtMs < currentRunCreatedAtMs
  ) {
    return;
  }

  const occurredAt = input.occurredAt ?? new Date().toISOString();
  const { error } = await supabase
    .from("import_schedules")
    .update({
      last_terminal_run_created_at: input.runCreatedAt,
      last_succeeded_at: occurredAt,
      consecutive_failure_count: 0,
      last_error: null,
      trigger_lease_expires_at: null,
    })
    .eq("id", input.scheduleId);

  if (error) {
    throw new Error(`Unable to mark import schedule success: ${error.message}`);
  }
}

export async function markImportScheduleRunFailed(input: {
  scheduleId: string;
  runCreatedAt: string;
  occurredAt?: string;
  errorMessage: string;
}) {
  const supabase = getSupabaseAdminClient();
  const current = await getScheduleRow(input.scheduleId);

  if (!current) {
    throw new Error(`Unable to mark import schedule failure: ${input.scheduleId} not found.`);
  }

  const currentRunCreatedAtMs = current.last_terminal_run_created_at
    ? new Date(current.last_terminal_run_created_at).getTime()
    : null;
  const inputRunCreatedAtMs = new Date(input.runCreatedAt).getTime();

  if (Number.isNaN(inputRunCreatedAtMs)) {
    throw new Error("Unable to mark import schedule failure: invalid runCreatedAt.");
  }

  if (
    currentRunCreatedAtMs !== null &&
    !Number.isNaN(currentRunCreatedAtMs) &&
    inputRunCreatedAtMs < currentRunCreatedAtMs
  ) {
    return;
  }

  const nextFailureCount =
    current.last_terminal_run_created_at === input.runCreatedAt
      ? current.consecutive_failure_count
      : typeof current.consecutive_failure_count === "number"
        ? current.consecutive_failure_count + 1
      : 1;
  const occurredAt = input.occurredAt ?? new Date().toISOString();
  const { error } = await supabase
    .from("import_schedules")
    .update({
      last_terminal_run_created_at: input.runCreatedAt,
      last_failed_at: occurredAt,
      consecutive_failure_count: nextFailureCount,
      last_error: input.errorMessage,
      trigger_lease_expires_at: null,
    })
    .eq("id", input.scheduleId);

  if (error) {
    throw new Error(`Unable to mark import schedule failure: ${error.message}`);
  }
}

export async function markImportScheduleTriggerFailure(input: {
  scheduleId: string;
  occurredAt?: string;
  errorMessage: string;
}) {
  const supabase = getSupabaseAdminClient();
  const current = await getScheduleRow(input.scheduleId);

  if (!current) {
    throw new Error(`Unable to mark import schedule trigger failure: ${input.scheduleId} not found.`);
  }

  const nextFailureCount =
    typeof current.consecutive_failure_count === "number"
      ? current.consecutive_failure_count + 1
      : 1;

  const { error } = await supabase
    .from("import_schedules")
    .update({
      last_failed_at: input.occurredAt ?? new Date().toISOString(),
      consecutive_failure_count: nextFailureCount,
      last_error: input.errorMessage,
      trigger_lease_expires_at: null,
    })
    .eq("id", input.scheduleId);

  if (error) {
    throw new Error(`Unable to mark import schedule trigger failure: ${error.message}`);
  }
}

export async function getImportScheduleAlertState(scheduleId: string) {
  const schedule = await getScheduleRow(scheduleId);

  if (!schedule) {
    return null;
  }

  return normalizeAlertState(schedule.alert_state);
}

export async function updateImportScheduleAlertState(input: {
  scheduleId: string;
  alertState: Record<string, unknown>;
}) {
  const supabase = getSupabaseAdminClient();
  const { error } = await supabase
    .from("import_schedules")
    .update({
      alert_state: input.alertState,
    })
    .eq("id", input.scheduleId);

  if (error) {
    throw new Error(`Unable to update import schedule alert state: ${error.message}`);
  }
}

export async function getImportScheduleRunHistory(input: {
  scheduleId: string;
  limit?: number;
  offset?: number;
  statusFilter?: ImportScheduleRunHistoryStatusFilter;
}): Promise<ImportScheduleRunHistoryPage> {
  const supabase = getSupabaseAdminClient();
  const limit = Math.max(1, Math.min(input.limit ?? 10, 50));
  const offset = Math.max(0, input.offset ?? 0);
  const statusFilter = input.statusFilter ?? "all";

  if (statusFilter === "stale") {
    const { data, error } = await supabase
      .from("import_runs")
      .select(IMPORT_SCHEDULE_RUN_SUMMARY_SELECT)
      .eq("schedule_id", input.scheduleId)
      .eq("trigger_type", "scheduled")
      .in("status", ["queued", "running"])
      .order("created_at", { ascending: false });

    if (error) {
      throw new Error(`Unable to fetch stale schedule run history: ${error.message}`);
    }

    const staleRuns = ((data ?? []) as ImportRunScheduleSummaryRow[])
      .filter((row) => isScheduleRunStale(row))
      .map((row) => mapRunSummary(row));

    return {
      items: staleRuns.slice(offset, offset + limit),
      total: staleRuns.length,
      limit,
      offset,
      statusFilter,
    };
  }

  let query = supabase
    .from("import_runs")
    .select(IMPORT_SCHEDULE_RUN_SUMMARY_SELECT, { count: "exact" })
    .eq("schedule_id", input.scheduleId)
    .eq("trigger_type", "scheduled")
    .order("created_at", { ascending: false })
    .range(offset, offset + limit - 1);

  if (statusFilter !== "all") {
    query = query.eq("status", statusFilter);
  }

  const { data, error, count } = await query;

  if (error) {
    throw new Error(`Unable to fetch schedule run history: ${error.message}`);
  }

  return {
    items: ((data ?? []) as ImportRunScheduleSummaryRow[]).map((row) => mapRunSummary(row)),
    total: count ?? 0,
    limit,
    offset,
    statusFilter,
  };
}

export async function getActiveScheduledImportRuns(input?: { limit?: number }) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_runs")
    .select("id")
    .eq("trigger_type", "scheduled")
    .in("status", ["queued", "running"])
    .order("created_at", { ascending: true })
    .limit(input?.limit ?? 10);

  if (error) {
    throw new Error(`Unable to fetch active scheduled import runs: ${error.message}`);
  }

  const runIds = (data ?? []).map((row) => row.id as string);
  const runs = await Promise.all(runIds.map((runId) => getImportRunDetail(runId)));

  return runs.filter((run): run is NonNullable<typeof run> => Boolean(run));
}
