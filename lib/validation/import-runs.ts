import { z } from "zod";

const positiveInt = z.coerce.number().int().min(1);

export const processImportRunSchema = z.object({
  batchSize: positiveInt.max(50).optional().default(10),
  maxBatches: positiveInt.max(10).optional().default(2),
});

export const retryImportRunSchema = z.object({
  forceRefresh: z.boolean().optional().default(false),
});

export const rerunImportRunSchema = z.object({
  forceRefresh: z.boolean().optional().default(false),
});

export const createRingbaRecentImportRunSchema = z.object({
  windowMinutes: z.union([z.literal(5), z.literal(15), z.literal(60)]),
  forceRefresh: z.boolean().optional().default(false),
});
