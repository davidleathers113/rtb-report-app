import "server-only";

import {
  claimImportRunItems,
  claimImportRunProcessing,
  completeImportRunItem,
  createImportRun,
  failImportRunItem,
  finalizeImportRun,
  getImportRunBidIds,
  getImportRunDetail,
  markImportRunFailed,
  resetFailedImportRunItems,
} from "@/lib/db/import-runs";
import { investigateBid } from "@/lib/investigations/service";

function dedupeBidIds(bidIds: string[]) {
  const values: string[] = [];
  const seen = new Set<string>();

  for (const bidId of bidIds) {
    const trimmed = bidId.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }

    seen.add(trimmed);
    values.push(trimmed);
  }

  return values;
}

export async function createAsyncImportRun(input: {
  bidIds: string[];
  forceRefresh: boolean;
  sourceType: string;
  notes?: string;
}) {
  const importRunId = await createImportRun({
    sourceType: input.sourceType,
    bidIds: dedupeBidIds(input.bidIds),
    forceRefresh: input.forceRefresh,
    notes: input.notes,
  });

  const detail = await getImportRunDetail(importRunId);

  if (!detail) {
    throw new Error(`Unable to load import run detail after creation: ${importRunId}`);
  }

  return detail;
}

export async function processImportRun(input: {
  importRunId: string;
  batchSize?: number;
  maxBatches?: number;
}) {
  const claim = await claimImportRunProcessing({
    importRunId: input.importRunId,
  });

  if (!claim.shouldProcess) {
    const current = await getImportRunDetail(input.importRunId);

    if (!current) {
      throw new Error(`Import run not found: ${input.importRunId}`);
    }

    return current;
  }

  try {
    let batchesProcessed = 0;

    while (batchesProcessed < (input.maxBatches ?? 2)) {
      const claimedItems = await claimImportRunItems({
        importRunId: input.importRunId,
        batchSize: input.batchSize ?? 10,
      });

      if (claimedItems.length === 0) {
        break;
      }

      for (const item of claimedItems) {
        try {
          const result = await investigateBid(item.bidId, {
            importRunId: input.importRunId,
            forceRefresh: claim.forceRefresh,
          });
          const investigation = result.investigation;

          if (!investigation) {
            throw new Error(`Investigation did not return persisted data for ${item.bidId}.`);
          }

          if (result.resolution === "failed" || investigation.fetchStatus === "failed") {
            await failImportRunItem({
              itemId: item.id,
              investigationId: investigation.id,
              errorMessage: investigation.lastError ?? "Investigation failed.",
            });
          } else {
            await completeImportRunItem({
              itemId: item.id,
              investigationId: investigation.id,
              resolution: result.resolution,
            });
          }
        } catch (error) {
          const message =
            error instanceof Error ? error.message : "Unexpected import run item error.";

          await failImportRunItem({
            itemId: item.id,
            investigationId: null,
            errorMessage: message,
          });
        }
      }

      batchesProcessed += 1;
    }

    return finalizeImportRun(input.importRunId);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unexpected import run processing error.";

    return markImportRunFailed(input.importRunId, message);
  }
}

export async function retryFailedImportRunItems(input: {
  importRunId: string;
  forceRefresh: boolean;
}) {
  const detail = await resetFailedImportRunItems(input);

  if (!detail) {
    throw new Error(`Unable to reload import run after retry reset: ${input.importRunId}`);
  }

  return detail;
}

export async function rerunImportRun(input: {
  importRunId: string;
  forceRefresh: boolean;
}) {
  const bidIds = await getImportRunBidIds(input.importRunId);

  return createAsyncImportRun({
    bidIds,
    forceRefresh: input.forceRefresh,
    sourceType: "import_run_rerun",
    notes: `Rerun of import run ${input.importRunId}.`,
  });
}
