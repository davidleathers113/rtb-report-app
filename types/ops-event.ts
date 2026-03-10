export const IMPORT_OPS_EVENT_TYPES = [
  "trigger_attempted",
  "trigger_auth_failed",
  "schedule_claimed",
  "schedule_skipped_overlap",
  "scheduled_run_created",
  "scheduled_run_succeeded",
  "scheduled_run_failed",
  "schedule_became_stale",
  "alert_sent",
  "alert_failed",
  "alert_acknowledged",
  "alert_snoozed",
  "alert_snooze_cleared",
  "schedule_paused",
  "schedule_resumed",
  "operator_retry_failed_run",
  "operator_force_refresh_rerun",
  "operator_run_now",
] as const;

export const IMPORT_OPS_EVENT_SEVERITIES = [
  "info",
  "warning",
  "error",
] as const;

export const IMPORT_OPS_EVENT_SOURCES = [
  "system",
  "scheduled_trigger",
  "manual_ui",
  "api",
  "cron",
] as const;

export type ImportOpsEventType = (typeof IMPORT_OPS_EVENT_TYPES)[number];
export type ImportOpsEventSeverity = (typeof IMPORT_OPS_EVENT_SEVERITIES)[number];
export type ImportOpsEventSource = (typeof IMPORT_OPS_EVENT_SOURCES)[number];

export interface ImportOpsEvent {
  id: string;
  eventType: ImportOpsEventType;
  severity: ImportOpsEventSeverity;
  source: ImportOpsEventSource;
  scheduleId: string | null;
  importRunId: string | null;
  message: string;
  metadataJson: Record<string, unknown>;
  createdAt: string;
}

export interface ImportOpsEventPage {
  items: ImportOpsEvent[];
  total: number;
  limit: number;
  offset: number;
  eventType: ImportOpsEventType | "all";
  severity: ImportOpsEventSeverity | "all";
}
