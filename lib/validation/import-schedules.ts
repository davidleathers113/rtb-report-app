import { z } from "zod";

export const createImportScheduleSchema = z.object({
  name: z.string().trim().min(1).max(100),
  isEnabled: z.boolean().optional().default(true),
  accountId: z.string().trim().min(1),
  sourceType: z
    .union([z.literal("ringba_recent_import"), z.literal("historical_ringba_backfill")])
    .optional()
    .default("ringba_recent_import"),
  windowMinutes: z.union([z.literal(5), z.literal(15), z.literal(60)]),
  overlapMinutes: z.coerce.number().int().min(0).max(15).optional().default(2),
  maxConcurrentRuns: z.coerce.number().int().min(1).max(3).optional().default(1),
  backfillStartBidDt: z.string().datetime().optional(),
  backfillEndBidDt: z.string().datetime().optional(),
  backfillLimit: z.coerce.number().int().min(1).max(250).optional(),
  backfillSort: z
    .union([z.literal("newest_first"), z.literal("oldest_first")])
    .optional(),
  pilotLabel: z.string().trim().max(120).optional(),
});

export const updateImportScheduleSchema = z
  .object({
    name: z.string().trim().min(1).max(100).optional(),
    isEnabled: z.boolean().optional(),
    sourceType: z
      .union([z.literal("ringba_recent_import"), z.literal("historical_ringba_backfill")])
      .optional(),
    windowMinutes: z.union([z.literal(5), z.literal(15), z.literal(60)]).optional(),
    overlapMinutes: z.coerce.number().int().min(0).max(15).optional(),
    maxConcurrentRuns: z.coerce.number().int().min(1).max(3).optional(),
    backfillStartBidDt: z.string().datetime().optional(),
    backfillEndBidDt: z.string().datetime().optional(),
    backfillLimit: z.coerce.number().int().min(1).max(250).optional(),
    backfillSort: z.union([z.literal("newest_first"), z.literal("oldest_first")]).optional(),
    pilotLabel: z.string().trim().max(120).optional(),
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "Provide at least one schedule field to update.",
  });

export const triggerImportSchedulesSchema = z.object({
  scheduleLimit: z.coerce.number().int().min(1).max(25).optional().default(10),
  activeRunLimit: z.coerce.number().int().min(1).max(25).optional().default(10),
  processBatchSize: z.coerce.number().int().min(1).max(50).optional().default(25),
  processMaxBatches: z.coerce.number().int().min(1).max(20).optional().default(10),
  staleAfterMinutes: z.coerce.number().int().min(5).max(240).optional().default(30),
});

export const importScheduleRunHistoryQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(50).optional().default(10),
  offset: z.coerce.number().int().min(0).max(500).optional().default(0),
  status: z
    .union([
      z.literal("all"),
      z.literal("queued"),
      z.literal("running"),
      z.literal("completed"),
      z.literal("completed_with_errors"),
      z.literal("failed"),
      z.literal("cancelled"),
      z.literal("stale"),
    ])
    .optional()
    .default("all"),
});

export const importScheduleOpsEventsQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(50).optional().default(10),
  offset: z.coerce.number().int().min(0).max(500).optional().default(0),
  eventType: z
    .union([
      z.literal("all"),
      z.literal("trigger_attempted"),
      z.literal("trigger_auth_failed"),
      z.literal("schedule_claimed"),
      z.literal("schedule_skipped_overlap"),
      z.literal("scheduled_run_created"),
      z.literal("scheduled_run_succeeded"),
      z.literal("scheduled_run_failed"),
      z.literal("schedule_became_stale"),
      z.literal("alert_sent"),
      z.literal("alert_failed"),
      z.literal("alert_acknowledged"),
      z.literal("alert_snoozed"),
      z.literal("alert_snooze_cleared"),
      z.literal("schedule_paused"),
      z.literal("schedule_resumed"),
      z.literal("operator_retry_failed_run"),
      z.literal("operator_force_refresh_rerun"),
      z.literal("operator_run_now"),
    ])
    .optional()
    .default("all"),
  severity: z
    .union([z.literal("all"), z.literal("info"), z.literal("warning"), z.literal("error")])
    .optional()
    .default("all"),
});

export const importScheduleActionSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("acknowledge_alert"),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("snooze_alert"),
    snoozedUntil: z.string().datetime(),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("clear_snooze"),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("pause_schedule"),
    reason: z.string().trim().max(250).optional(),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("resume_schedule"),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("run_now"),
    forceRefresh: z.boolean().optional().default(false),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("retry_failed_run"),
    importRunId: z.string().uuid(),
    forceRefresh: z.boolean().optional().default(false),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
  z.object({
    action: z.literal("force_refresh_rerun"),
    importRunId: z.string().uuid(),
    actionSource: z
      .union([
        z.literal("system"),
        z.literal("scheduled_trigger"),
        z.literal("manual_ui"),
        z.literal("api"),
        z.literal("cron"),
      ])
      .optional()
      .default("manual_ui"),
  }),
]);
