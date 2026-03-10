import { sql } from "drizzle-orm";
import { index, integer, real, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";

const nowIso = sql`(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))`;

export const bidInvestigations = sqliteTable(
  "bid_investigations",
  {
    id: text("id").primaryKey(),
    importRunId: text("import_run_id").references(() => importRuns.id, {
      onDelete: "set null",
    }),
    bidId: text("bid_id").notNull(),
    bidDt: text("bid_dt"),
    campaignName: text("campaign_name"),
    campaignId: text("campaign_id"),
    publisherName: text("publisher_name"),
    publisherId: text("publisher_id"),
    targetName: text("target_name"),
    targetId: text("target_id"),
    buyerName: text("buyer_name"),
    buyerId: text("buyer_id"),
    bidAmount: real("bid_amount"),
    winningBid: real("winning_bid"),
    isZeroBid: integer("is_zero_bid", { mode: "boolean" }).notNull().default(false),
    reasonForReject: text("reason_for_reject"),
    httpStatusCode: integer("http_status_code"),
    parsedErrorMessage: text("parsed_error_message"),
    requestBody: text("request_body", { mode: "json" }),
    responseBody: text("response_body", { mode: "json" }),
    rawTraceJson: text("raw_trace_json", { mode: "json" }).notNull().default(sql`'{}'`),
    outcome: text("outcome").notNull().default("unknown"),
    rootCause: text("root_cause").notNull().default("unknown_needs_review"),
    rootCauseConfidence: real("root_cause_confidence").notNull().default(0),
    severity: text("severity").notNull().default("high"),
    ownerType: text("owner_type").notNull().default("system"),
    suggestedFix: text("suggested_fix").notNull().default(""),
    explanation: text("explanation").notNull().default(""),
    evidenceJson: text("evidence_json", { mode: "json" }).notNull().default(sql`'[]'`),
    fetchStatus: text("fetch_status").notNull().default("pending"),
    fetchedAt: text("fetched_at"),
    fetchStartedAt: text("fetch_started_at"),
    lastError: text("last_error"),
    refreshRequestedAt: text("refresh_requested_at"),
    leaseExpiresAt: text("lease_expires_at"),
    fetchAttemptCount: integer("fetch_attempt_count").notNull().default(0),
    importedAt: text("imported_at").notNull().default(nowIso),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    bidIdUnique: uniqueIndex("bid_investigations_bid_id_unique").on(table.bidId),
    fetchStatusIdx: index("bid_investigations_fetch_status_idx").on(table.fetchStatus),
    importedAtIdx: index("bid_investigations_imported_at_idx").on(table.importedAt),
    bidDtIdx: index("bid_investigations_bid_dt_idx").on(table.bidDt),
    importRunIdIdx: index("bid_investigations_import_run_id_idx").on(table.importRunId),
  }),
);

export const bidEvents = sqliteTable(
  "bid_events",
  {
    id: text("id").primaryKey(),
    bidInvestigationId: text("bid_investigation_id")
      .notNull()
      .references(() => bidInvestigations.id, {
        onDelete: "cascade",
      }),
    eventName: text("event_name").notNull(),
    eventTimestamp: text("event_timestamp"),
    eventValsJson: text("event_vals_json", { mode: "json" }),
    eventStrValsJson: text("event_str_vals_json", { mode: "json" }),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    bidInvestigationIdx: index("bid_events_bid_investigation_id_idx").on(
      table.bidInvestigationId,
    ),
    eventTimestampIdx: index("bid_events_event_timestamp_idx").on(table.eventTimestamp),
  }),
);

export const importSchedules = sqliteTable(
  "import_schedules",
  {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    isEnabled: integer("is_enabled", { mode: "boolean" }).notNull().default(true),
    accountId: text("account_id").notNull(),
    sourceType: text("source_type").notNull().default("ringba_recent_import"),
    windowMinutes: integer("window_minutes").notNull(),
    overlapMinutes: integer("overlap_minutes").notNull().default(2),
    maxConcurrentRuns: integer("max_concurrent_runs").notNull().default(1),
    lastTriggeredAt: text("last_triggered_at"),
    lastSucceededAt: text("last_succeeded_at"),
    lastFailedAt: text("last_failed_at"),
    lastError: text("last_error"),
    consecutiveFailureCount: integer("consecutive_failure_count").notNull().default(0),
    lastTerminalRunCreatedAt: text("last_terminal_run_created_at"),
    alertState: text("alert_state", { mode: "json" }).notNull().default(sql`'{}'`),
    pausedAt: text("paused_at"),
    pauseReason: text("pause_reason"),
    alertAcknowledgedAt: text("alert_acknowledged_at"),
    alertAcknowledgedKey: text("alert_acknowledged_key"),
    alertSnoozedUntil: text("alert_snoozed_until"),
    triggerLeaseExpiresAt: text("trigger_lease_expires_at"),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    enabledIdx: index("import_schedules_enabled_idx").on(table.isEnabled, table.pausedAt),
    triggeredIdx: index("import_schedules_last_triggered_at_idx").on(table.lastTriggeredAt),
    sourceIdx: index("import_schedules_source_type_idx").on(table.sourceType),
  }),
);

export const importRuns = sqliteTable(
  "import_runs",
  {
    id: text("id").primaryKey(),
    sourceType: text("source_type").notNull(),
    triggerType: text("trigger_type").notNull().default("manual"),
    scheduleId: text("schedule_id").references(() => importSchedules.id, {
      onDelete: "set null",
    }),
    sourceStage: text("source_stage").notNull().default("queued"),
    status: text("status").notNull().default("queued"),
    forceRefresh: integer("force_refresh", { mode: "boolean" }).notNull().default(false),
    notes: text("notes"),
    lastError: text("last_error"),
    totalFound: integer("total_found").notNull().default(0),
    totalProcessed: integer("total_processed").notNull().default(0),
    sourceWindowStart: text("source_window_start"),
    sourceWindowEnd: text("source_window_end"),
    exportJobId: text("export_job_id"),
    exportRowCount: integer("export_row_count").notNull().default(0),
    exportDownloadStatus: text("export_download_status"),
    sourceMetadata: text("source_metadata", { mode: "json" }).notNull().default(sql`'{}'`),
    startedAt: text("started_at"),
    completedAt: text("completed_at"),
    processorLeaseExpiresAt: text("processor_lease_expires_at"),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    statusIdx: index("import_runs_status_idx").on(table.status),
    createdAtIdx: index("import_runs_created_at_idx").on(table.createdAt),
    scheduleIdx: index("import_runs_schedule_id_idx").on(table.scheduleId, table.triggerType),
    sourceStageIdx: index("import_runs_source_stage_idx").on(table.sourceStage),
  }),
);

export const importRunItems = sqliteTable(
  "import_run_items",
  {
    id: text("id").primaryKey(),
    importRunId: text("import_run_id")
      .notNull()
      .references(() => importRuns.id, {
        onDelete: "cascade",
      }),
    bidId: text("bid_id").notNull(),
    position: integer("position").notNull(),
    status: text("status").notNull().default("queued"),
    resolution: text("resolution"),
    errorMessage: text("error_message"),
    investigationId: text("investigation_id").references(() => bidInvestigations.id, {
      onDelete: "set null",
    }),
    startedAt: text("started_at"),
    completedAt: text("completed_at"),
    attemptCount: integer("attempt_count").notNull().default(0),
    leaseExpiresAt: text("lease_expires_at"),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    importRunPositionUnique: uniqueIndex("import_run_items_import_run_position_unique").on(
      table.importRunId,
      table.position,
    ),
    importRunBidUnique: uniqueIndex("import_run_items_import_run_bid_unique").on(
      table.importRunId,
      table.bidId,
    ),
    importRunStatusIdx: index("import_run_items_import_run_status_idx").on(
      table.importRunId,
      table.status,
    ),
    investigationIdx: index("import_run_items_investigation_id_idx").on(table.investigationId),
  }),
);

export const importSourceCheckpoints = sqliteTable(
  "import_source_checkpoints",
  {
    sourceKey: text("source_key").primaryKey(),
    sourceType: text("source_type").notNull(),
    lastSuccessfulBidDt: text("last_successful_bid_dt"),
    sourceMetadata: text("source_metadata", { mode: "json" }).notNull().default(sql`'{}'`),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    sourceTypeIdx: index("import_source_checkpoints_source_type_idx").on(table.sourceType),
  }),
);

export const importOpsEvents = sqliteTable(
  "import_ops_events",
  {
    id: text("id").primaryKey(),
    eventType: text("event_type").notNull(),
    severity: text("severity").notNull(),
    source: text("source").notNull(),
    scheduleId: text("schedule_id").references(() => importSchedules.id, {
      onDelete: "set null",
    }),
    importRunId: text("import_run_id").references(() => importRuns.id, {
      onDelete: "set null",
    }),
    message: text("message").notNull(),
    metadataJson: text("metadata_json", { mode: "json" }).notNull().default(sql`'{}'`),
    createdAt: text("created_at").notNull().default(nowIso),
  },
  (table) => ({
    scheduleCreatedIdx: index("import_ops_events_schedule_created_idx").on(
      table.scheduleId,
      table.createdAt,
    ),
    eventTypeIdx: index("import_ops_events_event_type_idx").on(table.eventType),
    severityIdx: index("import_ops_events_severity_idx").on(table.severity),
    importRunIdx: index("import_ops_events_import_run_id_idx").on(table.importRunId),
  }),
);

export type BidInvestigationRow = typeof bidInvestigations.$inferSelect;
export type BidEventRow = typeof bidEvents.$inferSelect;
export type ImportRunRow = typeof importRuns.$inferSelect;
export type ImportRunItemRow = typeof importRunItems.$inferSelect;
export type ImportSourceCheckpointRow = typeof importSourceCheckpoints.$inferSelect;
export type ImportScheduleRow = typeof importSchedules.$inferSelect;
export type ImportOpsEventRow = typeof importOpsEvents.$inferSelect;
