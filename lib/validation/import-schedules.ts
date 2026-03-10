import { z } from "zod";

export const createImportScheduleSchema = z.object({
  name: z.string().trim().min(1).max(100),
  isEnabled: z.boolean().optional().default(true),
  accountId: z.string().trim().min(1),
  sourceType: z.literal("ringba_recent_import").optional().default("ringba_recent_import"),
  windowMinutes: z.union([z.literal(5), z.literal(15), z.literal(60)]),
  overlapMinutes: z.coerce.number().int().min(0).max(15).optional().default(2),
  maxConcurrentRuns: z.coerce.number().int().min(1).max(3).optional().default(1),
});

export const updateImportScheduleSchema = z
  .object({
    name: z.string().trim().min(1).max(100).optional(),
    isEnabled: z.boolean().optional(),
    windowMinutes: z.union([z.literal(5), z.literal(15), z.literal(60)]).optional(),
    overlapMinutes: z.coerce.number().int().min(0).max(15).optional(),
    maxConcurrentRuns: z.coerce.number().int().min(1).max(3).optional(),
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
