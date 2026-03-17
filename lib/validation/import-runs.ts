import { z } from "zod";

const positiveInt = z.coerce.number().int().min(1);
const importRunIdList = z.array(z.string().trim().min(1)).max(25);

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

export const createHistoricalBackfillRunSchema = z.object({
  startBidDt: z.string().datetime().optional(),
  endBidDt: z.string().datetime().optional(),
  limit: positiveInt.max(250).optional().default(10),
  sort: z.union([z.literal("newest_first"), z.literal("oldest_first")]).optional().default(
    "newest_first",
  ),
  campaignId: z.string().trim().min(1).optional(),
  publisherId: z.string().trim().min(1).optional(),
  sourceImportRunId: z.string().trim().min(1).optional(),
  sourceImportRunIds: importRunIdList.optional(),
  forceRefresh: z.boolean().optional().default(false),
  pilotLabel: z.string().trim().max(120).optional(),
});

export const recoverCsvDirectImportRunsSchema = z.object({
  importRunIds: importRunIdList.optional(),
  stalledOnly: z.boolean().optional().default(false),
  batchSize: positiveInt.max(50).optional().default(25),
  maxBatchesPerPass: positiveInt.max(20).optional().default(10),
  maxRuns: positiveInt.max(25).optional().default(10),
  createHistoricalBackfill: z.boolean().optional().default(false),
  historicalBackfillLimit: positiveInt.max(250).optional().default(250),
  historicalBackfillSort: z
    .union([z.literal("newest_first"), z.literal("oldest_first")])
    .optional()
    .default("oldest_first"),
});
