import { z } from "zod";

import { OWNER_TYPES, ROOT_CAUSES } from "@/types/bid";

const positiveInt = z.coerce.number().int().min(1);

export const bulkInvestigateSchema = z.object({
  bidIds: z.array(z.string().trim().min(1)).min(1).max(500),
  forceRefresh: z.boolean().optional().default(false),
});

export const fetchOneBidSchema = z.object({
  bidId: z.string().trim().min(1),
  forceRefresh: z.boolean().optional().default(false),
});

export const investigationsQuerySchema = z.object({
  page: positiveInt.default(1),
  pageSize: positiveInt.max(100).default(25),
  rootCause: z.enum(ROOT_CAUSES).optional(),
  ownerType: z.enum(OWNER_TYPES).optional(),
  search: z.string().trim().min(1).optional(),
});
