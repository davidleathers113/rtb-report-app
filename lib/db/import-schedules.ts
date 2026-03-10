import "server-only";

import { eq } from "drizzle-orm";

import { getDb, getSqlite } from "@/lib/db/client";
import { getImportRunDetail } from "@/lib/db/import-runs";
import {
  importOpsEvents,
  importRuns,
  importSchedules,
  type ImportOpsEventRow as DbImportOpsEventRow,
  type ImportRunRow as DbImportRunRow,
  type ImportScheduleRow as DbImportScheduleRow,
} from "@/lib/db/schema";
import { addSeconds, createId, nowIso, toTimestamp } from "@/lib/db/utils";
import type { ImportOpsEvent } from "@/types/ops-event";
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
interface ImportScheduleRow {
  id: string;
  name: string;
  is_enabled: boolean;
  account_id: string;
  source_type: ImportScheduleSourceType;
  window_minutes: 5 | 15 | 60;
  overlap_minutes: number;
  max_concurrent_runs: number;
  source_metadata: Record<string, unknown> | null;
  last_triggered_at: string | null;
  last_succeeded_at: string | null;
  last_failed_at: string | null;
  last_error: string | null;
  consecutive_failure_count: number;
  last_terminal_run_created_at: string | null;
  alert_state: Record<string, unknown> | null;
  paused_at: string | null;
  pause_reason: string | null;
  alert_acknowledged_at: string | null;
  alert_acknowledged_key: string | null;
  alert_snoozed_until: string | null;
  trigger_lease_expires_at: string | null;
  created_at: string;
  updated_at: string;
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

interface ImportOpsEventRow {
  id: string;
  event_type: string;
  severity: "info" | "warning" | "error";
  source: "system" | "scheduled_trigger" | "manual_ui" | "api" | "cron";
  schedule_id: string | null;
  import_run_id: string | null;
  message: string;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
}

function mapScheduleDbRow(row: DbImportScheduleRow): ImportScheduleRow {
  return {
    id: row.id,
    name: row.name,
    is_enabled: row.isEnabled,
    account_id: row.accountId,
    source_type: row.sourceType as ImportScheduleSourceType,
    window_minutes: row.windowMinutes as 5 | 15 | 60,
    overlap_minutes: row.overlapMinutes,
    max_concurrent_runs: row.maxConcurrentRuns,
    source_metadata: (row.sourceMetadata ?? null) as Record<string, unknown> | null,
    last_triggered_at: row.lastTriggeredAt,
    last_succeeded_at: row.lastSucceededAt,
    last_failed_at: row.lastFailedAt,
    last_error: row.lastError,
    consecutive_failure_count: row.consecutiveFailureCount,
    last_terminal_run_created_at: row.lastTerminalRunCreatedAt,
    alert_state: (row.alertState ?? null) as Record<string, unknown> | null,
    paused_at: row.pausedAt,
    pause_reason: row.pauseReason,
    alert_acknowledged_at: row.alertAcknowledgedAt,
    alert_acknowledged_key: row.alertAcknowledgedKey,
    alert_snoozed_until: row.alertSnoozedUntil,
    trigger_lease_expires_at: row.triggerLeaseExpiresAt,
    created_at: row.createdAt,
    updated_at: row.updatedAt,
  };
}

function mapRunSummaryDbRow(row: DbImportRunRow): ImportRunScheduleSummaryRow {
  return {
    id: row.id,
    schedule_id: row.scheduleId,
    trigger_type: row.triggerType as "manual" | "scheduled",
    status: row.status as ImportRunStatus,
    source_stage: row.sourceStage as ImportRunSourceStage,
    total_found: row.totalFound,
    total_processed: row.totalProcessed,
    last_error: row.lastError,
    source_metadata: (row.sourceMetadata ?? null) as Record<string, unknown> | null,
    source_window_start: row.sourceWindowStart,
    source_window_end: row.sourceWindowEnd,
    processor_lease_expires_at: row.processorLeaseExpiresAt,
    completed_at: row.completedAt,
    created_at: row.createdAt,
    updated_at: row.updatedAt,
  };
}

function mapOpsEventDbRow(row: DbImportOpsEventRow): ImportOpsEventRow {
  return {
    id: row.id,
    event_type: row.eventType,
    severity: row.severity as "info" | "warning" | "error",
    source: row.source as "system" | "scheduled_trigger" | "manual_ui" | "api" | "cron",
    schedule_id: row.scheduleId,
    import_run_id: row.importRunId,
    message: row.message,
    metadata_json: (row.metadataJson ?? null) as Record<string, unknown> | null,
    created_at: row.createdAt,
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
    sourceMetadata: row.source_metadata ?? {},
    lastTriggeredAt: row.last_triggered_at,
    lastSucceededAt: row.last_succeeded_at,
    lastFailedAt: row.last_failed_at,
    lastError: row.last_error,
    consecutiveFailureCount: row.consecutive_failure_count,
    isNoRecentSuccess: false,
    isPaused: row.paused_at !== null,
    pausedAt: row.paused_at,
    pauseReason: row.pause_reason,
    currentAlertKey: null,
    currentAlertLabel: null,
    alertAcknowledgedAt: row.alert_acknowledged_at,
    alertAcknowledgedKey: row.alert_acknowledged_key,
    isCurrentAlertAcknowledged: false,
    alertSnoozedUntil: row.alert_snoozed_until,
    isAlertSnoozed: false,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapOpsEvent(row: ImportOpsEventRow): ImportOpsEvent {
  return {
    id: row.id,
    eventType: row.event_type as ImportOpsEvent["eventType"],
    severity: row.severity,
    source: row.source,
    scheduleId: row.schedule_id,
    importRunId: row.import_run_id,
    message: row.message,
    metadataJson: row.metadata_json ?? {},
    createdAt: row.created_at,
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

function isFutureTimestamp(value: string | null) {
  if (!value) {
    return false;
  }

  const timestamp = new Date(value).getTime();
  return !Number.isNaN(timestamp) && timestamp > Date.now();
}

function getLatestSourceStageFailure(recentRuns: ImportScheduleRunSummary[]) {
  return (
    recentRuns.find((run) => {
      return (
        (run.status === "failed" || run.status === "completed_with_errors") &&
        Boolean(run.failedStage || run.failureReason)
      );
    }) ?? null
  );
}

function getCurrentAlertDescriptor(input: {
  healthStatus: ImportScheduleHealthStatus;
  activeRun: ImportScheduleRunSummary | null;
  consecutiveFailureCount: number;
  isNoRecentSuccess: boolean;
  lastSucceededAt: string | null;
  recentRuns: ImportScheduleRunSummary[];
}) {
  if (input.healthStatus === "stale" && input.activeRun) {
    return {
      key: `stale:${input.activeRun.id}`,
      label: "Stale active run",
    };
  }

  if (input.consecutiveFailureCount >= 3) {
    return {
      key: `repeated_failures:${input.consecutiveFailureCount}`,
      label: `${input.consecutiveFailureCount} consecutive failures`,
    };
  }

  const latestSourceStageFailure = getLatestSourceStageFailure(input.recentRuns);
  if (latestSourceStageFailure) {
    return {
      key: `source_stage_failure:${latestSourceStageFailure.id}:${latestSourceStageFailure.failedStage ?? latestSourceStageFailure.sourceStage}`,
      label: `Source-stage failure: ${latestSourceStageFailure.failedStage ?? latestSourceStageFailure.sourceStage}`,
    };
  }

  if (input.isNoRecentSuccess) {
    return {
      key: `no_recent_success:${input.lastSucceededAt ?? "never"}`,
      label: "No recent success",
    };
  }

  return {
    key: null,
    label: null,
  };
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
      isNoRecentSuccess: false,
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
  const db = getDb();
  const rows = db.select().from(importSchedules).all() as DbImportScheduleRow[];
  return rows
    .map(mapScheduleDbRow)
    .sort((left, right) => right.created_at.localeCompare(left.created_at));
}

async function getScheduleRow(scheduleId: string) {
  const db = getDb();
  const row = db
    .select()
    .from(importSchedules)
    .where(eq(importSchedules.id, scheduleId))
    .get() as DbImportScheduleRow | undefined;
  return row ? mapScheduleDbRow(row) : null;
}

async function getScheduleRunRows(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as ImportRunScheduleSummaryRow[];
  }

  const db = getDb();
  return (db.select().from(importRuns).all() as DbImportRunRow[])
    .filter((row) => {
      return (
        row.scheduleId !== null &&
        scheduleIds.includes(row.scheduleId) &&
        row.triggerType === "scheduled" &&
        (row.status === "queued" || row.status === "running")
      );
    })
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .map(mapRunSummaryDbRow);
}

async function getRecentRunRowsBySchedule(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as ImportRunScheduleSummaryRow[];
  }

  return (await getScheduleRunRows(scheduleIds)).concat(
    (getDb().select().from(importRuns).all() as DbImportRunRow[])
      .filter((row) => {
        return (
          row.scheduleId !== null &&
          scheduleIds.includes(row.scheduleId) &&
          row.triggerType === "scheduled" &&
          row.status !== "queued" &&
          row.status !== "running"
        );
      })
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
      .map(mapRunSummaryDbRow),
  )
    .sort((left, right) => right.created_at.localeCompare(left.created_at))
    .slice(0, Math.max(ANALYTICS_RECENT_RUN_LIMIT, scheduleIds.length * 10));
}

async function getRunCountRowsBySchedule(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as Array<{ schedule_id: string | null }>;
  }

  return (getDb().select().from(importRuns).all() as DbImportRunRow[])
    .filter((row) => row.scheduleId !== null && scheduleIds.includes(row.scheduleId))
    .filter((row) => row.triggerType === "scheduled")
    .map((row) => ({
      schedule_id: row.scheduleId,
    }));
}

async function getRecentOpsEventRowsBySchedule(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as ImportOpsEventRow[];
  }

  return (getDb().select().from(importOpsEvents).all() as DbImportOpsEventRow[])
    .filter((row) => row.scheduleId !== null && scheduleIds.includes(row.scheduleId))
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .slice(0, Math.max(25, scheduleIds.length * 10))
    .map(mapOpsEventDbRow);
}

async function getOpsEventCountRowsBySchedule(scheduleIds: string[]) {
  if (scheduleIds.length === 0) {
    return [] as Array<{ schedule_id: string | null }>;
  }

  return (getDb().select().from(importOpsEvents).all() as DbImportOpsEventRow[])
    .filter((row) => row.scheduleId !== null && scheduleIds.includes(row.scheduleId))
    .map((row) => ({
      schedule_id: row.scheduleId,
    }));
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

function groupRecentOpsEventsBySchedule(rows: ImportOpsEventRow[]) {
  const eventsBySchedule = new Map<string, ImportOpsEvent[]>();

  for (const row of rows) {
    const scheduleId = row.schedule_id;
    if (!scheduleId) {
      continue;
    }

    const current = eventsBySchedule.get(scheduleId) ?? [];
    if (current.length >= 5) {
      continue;
    }

    current.push(mapOpsEvent(row));
    eventsBySchedule.set(scheduleId, current);
  }

  return eventsBySchedule;
}

export async function getImportSchedules(): Promise<ImportScheduleDetail[]> {
  const rows = await getScheduleRows();
  const scheduleIds = rows.map((row) => row.id);
  const activeRunRows = await getScheduleRunRows(scheduleIds);
  const recentRunRows = await getRecentRunRowsBySchedule(scheduleIds);
  const runCountRows = await getRunCountRowsBySchedule(scheduleIds);
  const recentOpsEventRows = await getRecentOpsEventRowsBySchedule(scheduleIds);
  const opsEventCountRows = await getOpsEventCountRowsBySchedule(scheduleIds);
  const activeRunsBySchedule = new Map<string, ImportScheduleRunSummary | null>();
  const recentRunsBySchedule = groupRecentRunsBySchedule(recentRunRows);
  const recentRunCountsBySchedule = countRunsBySchedule(runCountRows);
  const analyticsBySchedule = buildAnalyticsBySchedule(recentRunRows);
  const recentOpsEventsBySchedule = groupRecentOpsEventsBySchedule(recentOpsEventRows);
  const recentOpsEventCountsBySchedule = countRunsBySchedule(opsEventCountRows);

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
      const recentRuns = recentRunsBySchedule.get(row.id) ?? [];
      const alertDescriptor = getCurrentAlertDescriptor({
        healthStatus: health.healthStatus,
        activeRun,
        consecutiveFailureCount: row.consecutive_failure_count,
        isNoRecentSuccess: health.isNoRecentSuccess,
        lastSucceededAt: row.last_succeeded_at,
        recentRuns,
      });
      const isAlertSnoozed = isFutureTimestamp(row.alert_snoozed_until);
      const isCurrentAlertAcknowledged =
        Boolean(row.alert_acknowledged_at) &&
        row.alert_acknowledged_key !== null &&
        row.alert_acknowledged_key === alertDescriptor.key;

      return {
        ...mapSchedule(row),
        ...health,
        activeRun,
        recentRuns,
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
        currentAlertKey: alertDescriptor.key,
        currentAlertLabel: alertDescriptor.label,
        isCurrentAlertAcknowledged,
        isAlertSnoozed,
        recentOpsEvents: recentOpsEventsBySchedule.get(row.id) ?? [],
        recentOpsEventTotalCount: recentOpsEventCountsBySchedule.get(row.id) ?? 0,
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
  sourceMetadata?: Record<string, unknown>;
}) {
  const db = getDb();
  const now = nowIso();
  const row: ImportScheduleRow = {
    id: createId(),
    name: input.name,
    is_enabled: input.isEnabled,
    account_id: input.accountId,
    source_type: input.sourceType,
    window_minutes: input.windowMinutes,
    overlap_minutes: input.overlapMinutes,
    max_concurrent_runs: input.maxConcurrentRuns,
    source_metadata: input.sourceMetadata ?? {},
    last_triggered_at: null,
    last_succeeded_at: null,
    last_failed_at: null,
    last_error: null,
    consecutive_failure_count: 0,
    last_terminal_run_created_at: null,
    alert_state: {},
    paused_at: null,
    pause_reason: null,
    alert_acknowledged_at: null,
    alert_acknowledged_key: null,
    alert_snoozed_until: null,
    trigger_lease_expires_at: null,
    created_at: now,
    updated_at: now,
  };

  db.insert(importSchedules)
    .values({
      id: row.id,
      name: row.name,
      isEnabled: row.is_enabled,
      accountId: row.account_id,
      sourceType: row.source_type,
      windowMinutes: row.window_minutes,
      overlapMinutes: row.overlap_minutes,
      maxConcurrentRuns: row.max_concurrent_runs,
      sourceMetadata: row.source_metadata ?? {},
      alertState: row.alert_state ?? {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    })
    .run();

  return {
    ...mapSchedule(row),
    healthStatus: input.isEnabled ? "warning" : "disabled",
    healthSummary: input.isEnabled
      ? "Schedule has not completed a run yet."
      : "Schedule is disabled.",
    isNoRecentSuccess: false,
    activeRun: null,
    recentRuns: [],
    recentRunTotalCount: 0,
    recentOpsEvents: [],
    recentOpsEventTotalCount: 0,
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
  sourceType?: ImportScheduleSourceType;
  windowMinutes?: 5 | 15 | 60;
  overlapMinutes?: number;
  maxConcurrentRuns?: number;
  sourceMetadata?: Record<string, unknown>;
}) {
  const db = getDb();
  const now = nowIso();
  const updateData: Record<string, unknown> = {};

  if (input.name !== undefined) {
    updateData.name = input.name;
  }
  if (input.isEnabled !== undefined) {
    updateData.isEnabled = input.isEnabled;
  }
  if (input.sourceType !== undefined) {
    updateData.sourceType = input.sourceType;
  }
  if (input.windowMinutes !== undefined) {
    updateData.windowMinutes = input.windowMinutes;
  }
  if (input.overlapMinutes !== undefined) {
    updateData.overlapMinutes = input.overlapMinutes;
  }
  if (input.maxConcurrentRuns !== undefined) {
    updateData.maxConcurrentRuns = input.maxConcurrentRuns;
  }
  if (input.sourceMetadata !== undefined) {
    updateData.sourceMetadata = input.sourceMetadata;
  }
  updateData.updatedAt = now;

  db.update(importSchedules).set(updateData).where(eq(importSchedules.id, input.scheduleId)).run();

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
  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const limit = Math.max(1, input?.limit ?? 10);
  const staleAfterMinutes = Math.max(5, input?.staleAfterMinutes ?? SCHEDULE_STALE_RUN_MINUTES);
  const leaseExpiresAt = addSeconds(now, input?.leaseSeconds ?? 120);

  return sqlite.transaction(() => {
    const scheduleRows = (db.select().from(importSchedules).all() as DbImportScheduleRow[])
      .map(mapScheduleDbRow)
      .sort((left, right) => {
        const leftTrigger = left.last_triggered_at ?? "";
        const rightTrigger = right.last_triggered_at ?? "";
        if (leftTrigger !== rightTrigger) {
          return leftTrigger.localeCompare(rightTrigger);
        }
        return left.created_at.localeCompare(right.created_at);
      });
    const runRows = (db.select().from(importRuns).all() as DbImportRunRow[]).map(
      mapRunSummaryDbRow,
    );
    const claimed: ImportScheduleRow[] = [];
    const nowMs = toTimestamp(now) ?? 0;
    const staleCutoffMs = nowMs - staleAfterMinutes * 60 * 1000;

    for (const schedule of scheduleRows) {
      if (claimed.length >= limit) {
        break;
      }

      if (!schedule.is_enabled || schedule.paused_at !== null) {
        continue;
      }

      const leaseMs = toTimestamp(schedule.trigger_lease_expires_at);
      if (leaseMs !== null && leaseMs > nowMs) {
        continue;
      }

      const dueCutoffMs = nowMs - Math.max(schedule.window_minutes, 1) * 60 * 1000;
      const lastTriggeredMs = toTimestamp(schedule.last_triggered_at);
      if (lastTriggeredMs !== null && lastTriggeredMs > dueCutoffMs) {
        continue;
      }

      const activeNonStaleRunCount = runRows.filter((run) => {
        if (run.schedule_id !== schedule.id || run.trigger_type !== "scheduled") {
          return false;
        }

        if (run.status !== "queued" && run.status !== "running") {
          return false;
        }

        const updatedAtMs = toTimestamp(run.updated_at);
        const processorLeaseMs = toTimestamp(run.processor_lease_expires_at);
        const leaseActive = processorLeaseMs !== null && processorLeaseMs > nowMs;
        const stale =
          !leaseActive && updatedAtMs !== null && updatedAtMs <= staleCutoffMs;

        return !stale;
      }).length;

      if (activeNonStaleRunCount >= schedule.max_concurrent_runs) {
        continue;
      }

      db.update(importSchedules)
        .set({
          triggerLeaseExpiresAt: leaseExpiresAt,
          updatedAt: now,
        })
        .where(eq(importSchedules.id, schedule.id))
        .run();

      claimed.push({
        ...schedule,
        trigger_lease_expires_at: leaseExpiresAt,
        updated_at: now,
      });
    }

    return claimed.map((row) => ({
      id: row.id,
      name: row.name,
      isEnabled: row.is_enabled,
      accountId: row.account_id,
      sourceType: row.source_type,
      windowMinutes: row.window_minutes,
      overlapMinutes: row.overlap_minutes,
      maxConcurrentRuns: row.max_concurrent_runs,
      sourceMetadata: row.source_metadata ?? {},
      lastTriggeredAt: row.last_triggered_at,
      lastSucceededAt: row.last_succeeded_at,
      lastFailedAt: row.last_failed_at,
      lastError: row.last_error,
      consecutiveFailureCount: row.consecutive_failure_count,
    }));
  })();
}

export async function markImportScheduleTriggered(input: {
  scheduleId: string;
  clearError?: boolean;
}) {
  const db = getDb();
  const now = nowIso();
  const updateData: Record<string, unknown> = {
    lastTriggeredAt: now,
    triggerLeaseExpiresAt: null,
    updatedAt: now,
  };

  if (input.clearError === true) {
    updateData.lastError = null;
  }

  db.update(importSchedules).set(updateData).where(eq(importSchedules.id, input.scheduleId)).run();
}

export async function markImportScheduleRunSucceeded(input: {
  scheduleId: string;
  runCreatedAt: string;
  occurredAt?: string;
}) {
  const db = getDb();
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

  const occurredAt = input.occurredAt ?? nowIso();
  db.update(importSchedules)
    .set({
      lastTerminalRunCreatedAt: input.runCreatedAt,
      lastSucceededAt: occurredAt,
      consecutiveFailureCount: 0,
      lastError: null,
      triggerLeaseExpiresAt: null,
      updatedAt: occurredAt,
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
}

export async function markImportScheduleRunFailed(input: {
  scheduleId: string;
  runCreatedAt: string;
  occurredAt?: string;
  errorMessage: string;
}) {
  const db = getDb();
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
  const occurredAt = input.occurredAt ?? nowIso();
  db.update(importSchedules)
    .set({
      lastTerminalRunCreatedAt: input.runCreatedAt,
      lastFailedAt: occurredAt,
      consecutiveFailureCount: nextFailureCount,
      lastError: input.errorMessage,
      triggerLeaseExpiresAt: null,
      updatedAt: occurredAt,
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
}

export async function markImportScheduleTriggerFailure(input: {
  scheduleId: string;
  occurredAt?: string;
  errorMessage: string;
}) {
  const db = getDb();
  const current = await getScheduleRow(input.scheduleId);

  if (!current) {
    throw new Error(`Unable to mark import schedule trigger failure: ${input.scheduleId} not found.`);
  }

  const nextFailureCount =
    typeof current.consecutive_failure_count === "number"
      ? current.consecutive_failure_count + 1
      : 1;

  const occurredAt = input.occurredAt ?? nowIso();
  db.update(importSchedules)
    .set({
      lastFailedAt: occurredAt,
      consecutiveFailureCount: nextFailureCount,
      lastError: input.errorMessage,
      triggerLeaseExpiresAt: null,
      updatedAt: occurredAt,
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
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
  getDb()
    .update(importSchedules)
    .set({
      alertState: input.alertState,
      updatedAt: nowIso(),
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
}

export async function acknowledgeImportScheduleAlert(input: {
  scheduleId: string;
  alertKey: string;
}) {
  const now = nowIso();
  getDb()
    .update(importSchedules)
    .set({
      alertAcknowledgedAt: now,
      alertAcknowledgedKey: input.alertKey,
      updatedAt: now,
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
}

export async function clearImportScheduleAlertAcknowledgement(scheduleId: string) {
  getDb()
    .update(importSchedules)
    .set({
      alertAcknowledgedAt: null,
      alertAcknowledgedKey: null,
      updatedAt: nowIso(),
    })
    .where(eq(importSchedules.id, scheduleId))
    .run();
}

export async function snoozeImportScheduleAlerts(input: {
  scheduleId: string;
  snoozedUntil: string;
}) {
  getDb()
    .update(importSchedules)
    .set({
      alertSnoozedUntil: input.snoozedUntil,
      updatedAt: nowIso(),
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
}

export async function clearImportScheduleAlertSnooze(scheduleId: string) {
  getDb()
    .update(importSchedules)
    .set({
      alertSnoozedUntil: null,
      updatedAt: nowIso(),
    })
    .where(eq(importSchedules.id, scheduleId))
    .run();
}

export async function pauseImportSchedule(input: {
  scheduleId: string;
  reason?: string | null;
}) {
  const now = nowIso();
  getDb()
    .update(importSchedules)
    .set({
      pausedAt: now,
      pauseReason: input.reason ?? null,
      triggerLeaseExpiresAt: null,
      updatedAt: now,
    })
    .where(eq(importSchedules.id, input.scheduleId))
    .run();
}

export async function resumeImportSchedule(scheduleId: string) {
  getDb()
    .update(importSchedules)
    .set({
      pausedAt: null,
      pauseReason: null,
      updatedAt: nowIso(),
    })
    .where(eq(importSchedules.id, scheduleId))
    .run();
}

export async function getImportScheduleRunHistory(input: {
  scheduleId: string;
  limit?: number;
  offset?: number;
  statusFilter?: ImportScheduleRunHistoryStatusFilter;
}): Promise<ImportScheduleRunHistoryPage> {
  const limit = Math.max(1, Math.min(input.limit ?? 10, 50));
  const offset = Math.max(0, input.offset ?? 0);
  const statusFilter = input.statusFilter ?? "all";
  const rows = (getDb().select().from(importRuns).all() as DbImportRunRow[])
    .filter((row) => row.scheduleId === input.scheduleId && row.triggerType === "scheduled")
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .map(mapRunSummaryDbRow);

  if (statusFilter === "stale") {
    const staleRuns = rows
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

  const filteredRows =
    statusFilter === "all" ? rows : rows.filter((row) => row.status === statusFilter);

  return {
    items: filteredRows.slice(offset, offset + limit).map((row) => mapRunSummary(row)),
    total: filteredRows.length,
    limit,
    offset,
    statusFilter,
  };
}

export async function getActiveScheduledImportRuns(input?: { limit?: number }) {
  const runIds = (getDb().select().from(importRuns).all() as DbImportRunRow[])
    .filter((row) => row.triggerType === "scheduled")
    .filter((row) => row.status === "queued" || row.status === "running")
    .sort((left, right) => left.createdAt.localeCompare(right.createdAt))
    .slice(0, input?.limit ?? 10)
    .map((row) => row.id);
  const runs = await Promise.all(runIds.map((runId) => getImportRunDetail(runId)));

  return runs.filter((run): run is NonNullable<typeof run> => Boolean(run));
}
