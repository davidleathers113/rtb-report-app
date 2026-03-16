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
    sourceImportRunId: text("source_import_run_id").references(() => importRuns.id, {
      onDelete: "set null",
    }),
    sourceImportSourceFileId: text("source_import_source_file_id").references(
      () => importSourceFiles.id,
      {
        onDelete: "set null",
      },
    ),
    sourceImportSourceRowId: text("source_import_source_row_id").references(
      () => importSourceRows.id,
      {
        onDelete: "set null",
      },
    ),
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
    bidElapsedMs: integer("bid_elapsed_ms"),
    isZeroBid: integer("is_zero_bid", { mode: "boolean" }).notNull().default(false),
    reasonForReject: text("reason_for_reject"),
    httpStatusCode: integer("http_status_code"),
    parsedErrorMessage: text("parsed_error_message"),
    primaryFailureStage: text("primary_failure_stage").notNull().default("unknown"),
    primaryTargetName: text("primary_target_name"),
    primaryTargetId: text("primary_target_id"),
    primaryBuyerName: text("primary_buyer_name"),
    primaryBuyerId: text("primary_buyer_id"),
    primaryErrorCode: integer("primary_error_code"),
    primaryErrorMessage: text("primary_error_message"),
    requestBody: text("request_body", { mode: "json" }),
    responseBody: text("response_body", { mode: "json" }),
    rawTraceJson: text("raw_trace_json", { mode: "json" }).notNull().default(sql`'{}'`),
    outcome: text("outcome").notNull().default("unknown"),
    outcomeReasonCategory: text("outcome_reason_category"),
    outcomeReasonCode: text("outcome_reason_code"),
    outcomeReasonMessage: text("outcome_reason_message"),
    classificationSource: text("classification_source"),
    classificationConfidence: real("classification_confidence"),
    classificationWarningsJson: text("classification_warnings_json", { mode: "json" })
      .notNull()
      .default(sql`'[]'`),
    parseStatus: text("parse_status").notNull().default("not_attempted"),
    normalizationVersion: text("normalization_version"),
    schemaVariant: text("schema_variant"),
    normalizationConfidence: real("normalization_confidence"),
    normalizationWarningsJson: text("normalization_warnings_json", { mode: "json" })
      .notNull()
      .default(sql`'[]'`),
    missingCriticalFieldsJson: text("missing_critical_fields_json", { mode: "json" })
      .notNull()
      .default(sql`'[]'`),
    missingOptionalFieldsJson: text("missing_optional_fields_json", { mode: "json" })
      .notNull()
      .default(sql`'[]'`),
    unknownEventNamesJson: text("unknown_event_names_json", { mode: "json" })
      .notNull()
      .default(sql`'[]'`),
    rawPathsUsedJson: text("raw_paths_used_json", { mode: "json" }).notNull().default(sql`'{}'`),
    primaryErrorCodeSource: text("primary_error_code_source"),
    primaryErrorCodeConfidence: real("primary_error_code_confidence"),
    primaryErrorCodeRawMatch: text("primary_error_code_raw_match"),
    rootCause: text("root_cause").notNull().default("unknown_needs_review"),
    rootCauseConfidence: real("root_cause_confidence").notNull().default(0),
    severity: text("severity").notNull().default("high"),
    ownerType: text("owner_type").notNull().default("system"),
    suggestedFix: text("suggested_fix").notNull().default(""),
    explanation: text("explanation").notNull().default(""),
    evidenceJson: text("evidence_json", { mode: "json" }).notNull().default(sql`'[]'`),
    detailSource: text("detail_source").notNull().default("ringba_api"),
    enrichmentState: text("enrichment_state").notNull().default("enriched"),
    fetchStatus: text("fetch_status").notNull().default("pending"),
    fetchedAt: text("fetched_at"),
    fetchStartedAt: text("fetch_started_at"),
    lastError: text("last_error"),
    lastRingbaAttemptAt: text("last_ringba_attempt_at"),
    lastRingbaFetchAt: text("last_ringba_fetch_at"),
    ringbaFailureCount: integer("ringba_failure_count").notNull().default(0),
    nextRingbaRetryAt: text("next_ringba_retry_at"),
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
    enrichmentStateIdx: index("bid_investigations_enrichment_state_idx").on(
      table.enrichmentState,
    ),
    nextRingbaRetryAtIdx: index("bid_investigations_next_ringba_retry_at_idx").on(
      table.nextRingbaRetryAt,
    ),
    importedAtIdx: index("bid_investigations_imported_at_idx").on(table.importedAt),
    bidDtIdx: index("bid_investigations_bid_dt_idx").on(table.bidDt),
    importRunIdIdx: index("bid_investigations_import_run_id_idx").on(table.importRunId),
    sourceImportRunIdIdx: index("bid_investigations_source_import_run_id_idx").on(
      table.sourceImportRunId,
    ),
  }),
);

export const bidTargetAttempts = sqliteTable(
  "bid_target_attempts",
  {
    id: text("id").primaryKey(),
    bidInvestigationId: text("bid_investigation_id")
      .notNull()
      .references(() => bidInvestigations.id, {
        onDelete: "cascade",
      }),
    sequence: integer("sequence").notNull(),
    eventName: text("event_name").notNull(),
    eventTimestamp: text("event_timestamp"),
    targetName: text("target_name"),
    targetId: text("target_id"),
    targetBuyer: text("target_buyer"),
    targetBuyerId: text("target_buyer_id"),
    targetNumber: text("target_number"),
    targetGroupName: text("target_group_name"),
    targetGroupId: text("target_group_id"),
    targetSubId: text("target_sub_id"),
    targetBuyerSubId: text("target_buyer_sub_id"),
    requestUrl: text("request_url"),
    httpMethod: text("http_method"),
    requestStatus: text("request_status"),
    httpStatusCode: integer("http_status_code"),
    durationMs: integer("duration_ms"),
    routePriority: integer("route_priority"),
    routeWeight: integer("route_weight"),
    accepted: integer("accepted", { mode: "boolean" }),
    winning: integer("winning", { mode: "boolean" }),
    bidAmount: real("bid_amount"),
    minDurationSeconds: integer("min_duration_seconds"),
    rejectReason: text("reject_reason"),
    errorCode: integer("error_code"),
    errorMessage: text("error_message"),
    errorsJson: text("errors_json", { mode: "json" }).notNull().default(sql`'[]'`),
    requestBody: text("request_body", { mode: "json" }),
    responseBody: text("response_body", { mode: "json" }),
    summaryReason: text("summary_reason"),
    rawEventJson: text("raw_event_json", { mode: "json" }).notNull().default(sql`'{}'`),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    bidInvestigationIdx: index("bid_target_attempts_bid_investigation_id_idx").on(
      table.bidInvestigationId,
    ),
    bidInvestigationSequenceUnique: uniqueIndex(
      "bid_target_attempts_investigation_sequence_unique",
    ).on(table.bidInvestigationId, table.sequence),
    targetIdx: index("bid_target_attempts_target_idx").on(table.targetName, table.targetBuyer),
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
    sourceMetadata: text("source_metadata", { mode: "json" }).notNull().default(sql`'{}'`),
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

export const importSourceFiles = sqliteTable(
  "import_source_files",
  {
    id: text("id").primaryKey(),
    importRunId: text("import_run_id").references(() => importRuns.id, {
      onDelete: "cascade",
    }),
    sourceType: text("source_type").notNull(),
    fileName: text("file_name").notNull(),
    rowCount: integer("row_count").notNull().default(0),
    headerJson: text("header_json", { mode: "json" }).notNull().default(sql`'[]'`),
    sourceMetadata: text("source_metadata", { mode: "json" }).notNull().default(sql`'{}'`),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    importRunIdx: index("import_source_files_import_run_id_idx").on(table.importRunId),
    sourceTypeIdx: index("import_source_files_source_type_idx").on(table.sourceType),
  }),
);

export const importSourceRows = sqliteTable(
  "import_source_rows",
  {
    id: text("id").primaryKey(),
    importSourceFileId: text("import_source_file_id")
      .notNull()
      .references(() => importSourceFiles.id, {
        onDelete: "cascade",
      }),
    importRunId: text("import_run_id")
      .notNull()
      .references(() => importRuns.id, {
        onDelete: "cascade",
      }),
    rowNumber: integer("row_number").notNull(),
    bidId: text("bid_id"),
    bidDt: text("bid_dt"),
    campaignName: text("campaign_name"),
    campaignId: text("campaign_id"),
    publisherName: text("publisher_name"),
    publisherId: text("publisher_id"),
    bidAmount: real("bid_amount"),
    winningBid: real("winning_bid"),
    bidRejected: integer("bid_rejected", { mode: "boolean" }),
    reasonForReject: text("reason_for_reject"),
    bidDid: text("bid_did"),
    bidExpireDate: text("bid_expire_date"),
    expirationSeconds: integer("expiration_seconds"),
    winningBidCallAccepted: integer("winning_bid_call_accepted", { mode: "boolean" }),
    winningBidCallRejected: integer("winning_bid_call_rejected", { mode: "boolean" }),
    bidElapsedMs: integer("bid_elapsed_ms"),
    rowJson: text("row_json", { mode: "json" }).notNull().default(sql`'{}'`),
    createdAt: text("created_at").notNull().default(nowIso),
    updatedAt: text("updated_at").notNull().default(nowIso),
  },
  (table) => ({
    importSourceFileIdx: index("import_source_rows_file_id_idx").on(
      table.importSourceFileId,
    ),
    importRunIdx: index("import_source_rows_import_run_id_idx").on(table.importRunId),
    bidIdIdx: index("import_source_rows_bid_id_idx").on(table.bidId),
    bidDtIdx: index("import_source_rows_bid_dt_idx").on(table.bidDt),
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
export type BidTargetAttemptRow = typeof bidTargetAttempts.$inferSelect;
export type BidEventRow = typeof bidEvents.$inferSelect;
export type ImportRunRow = typeof importRuns.$inferSelect;
export type ImportRunItemRow = typeof importRunItems.$inferSelect;
export type ImportSourceCheckpointRow = typeof importSourceCheckpoints.$inferSelect;
export type ImportScheduleRow = typeof importSchedules.$inferSelect;
export type ImportOpsEventRow = typeof importOpsEvents.$inferSelect;
export type ImportSourceFileRow = typeof importSourceFiles.$inferSelect;
export type ImportSourceRow = typeof importSourceRows.$inferSelect;
