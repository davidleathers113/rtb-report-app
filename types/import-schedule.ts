import type { ImportRunSourceStage, ImportRunStatus } from "@/types/import-run";
import type { ImportOpsEvent, ImportOpsEventPage } from "@/types/ops-event";

export const IMPORT_SCHEDULE_SOURCE_TYPES = [
  "ringba_recent_import",
  "historical_ringba_backfill",
] as const;

export type ImportScheduleSourceType = (typeof IMPORT_SCHEDULE_SOURCE_TYPES)[number];

export const IMPORT_SCHEDULE_HEALTH_STATUSES = [
  "healthy",
  "warning",
  "failing",
  "stale",
  "disabled",
] as const;

export type ImportScheduleHealthStatus =
  (typeof IMPORT_SCHEDULE_HEALTH_STATUSES)[number];

export const IMPORT_SCHEDULE_RUN_HISTORY_STATUS_FILTERS = [
  "all",
  "queued",
  "running",
  "completed",
  "completed_with_errors",
  "failed",
  "cancelled",
  "stale",
] as const;

export type ImportScheduleRunHistoryStatusFilter =
  (typeof IMPORT_SCHEDULE_RUN_HISTORY_STATUS_FILTERS)[number];

export interface ImportScheduleRunSummary {
  id: string;
  triggerType: "manual" | "scheduled";
  status: ImportRunStatus;
  sourceStage: ImportRunSourceStage;
  totalFound: number;
  totalProcessed: number;
  lastError: string | null;
  sourceWindowStart: string | null;
  sourceWindowEnd: string | null;
  createdAt: string;
  updatedAt: string;
  completedAt?: string | null;
  isStale: boolean;
  durationMs: number | null;
  exportReadyLatencyMs: number | null;
  failedStage: string | null;
  failureReason: string | null;
}

export interface ImportScheduleAnalyticsBreakdownItem {
  label: string;
  count: number;
}

export interface ImportScheduleAnalytics {
  recentRunCount: number;
  successfulRunCount: number;
  failedRunCount: number;
  completedWithErrorsCount: number;
  runningRunCount: number;
  queuedRunCount: number;
  staleRunCount: number;
  averageRunDurationMs: number | null;
  averageExportReadyLatencyMs: number | null;
  sourceStageFailureBreakdown: ImportScheduleAnalyticsBreakdownItem[];
  rootCauseSummary: ImportScheduleAnalyticsBreakdownItem[];
}

export interface ImportScheduleRunHistoryPage {
  items: ImportScheduleRunSummary[];
  total: number;
  limit: number;
  offset: number;
  statusFilter: ImportScheduleRunHistoryStatusFilter;
}

export interface ImportScheduleDetail {
  id: string;
  name: string;
  isEnabled: boolean;
  accountId: string;
  sourceType: ImportScheduleSourceType;
  windowMinutes: 5 | 15 | 60;
  overlapMinutes: number;
  maxConcurrentRuns: number;
  sourceMetadata: Record<string, unknown>;
  lastTriggeredAt: string | null;
  lastSucceededAt: string | null;
  lastFailedAt: string | null;
  lastError: string | null;
  consecutiveFailureCount: number;
  healthStatus: ImportScheduleHealthStatus;
  healthSummary: string;
  isNoRecentSuccess: boolean;
  isPaused: boolean;
  pausedAt: string | null;
  pauseReason: string | null;
  currentAlertKey: string | null;
  currentAlertLabel: string | null;
  alertAcknowledgedAt: string | null;
  alertAcknowledgedKey: string | null;
  isCurrentAlertAcknowledged: boolean;
  alertSnoozedUntil: string | null;
  isAlertSnoozed: boolean;
  createdAt: string;
  updatedAt: string;
  activeRun: ImportScheduleRunSummary | null;
  recentRuns: ImportScheduleRunSummary[];
  recentRunTotalCount: number;
  analytics: ImportScheduleAnalytics;
  recentOpsEvents: ImportOpsEvent[];
  recentOpsEventTotalCount: number;
}

export type ImportScheduleOpsEventPage = ImportOpsEventPage;
