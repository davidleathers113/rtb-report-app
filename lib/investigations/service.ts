import "server-only";

import {
  claimInvestigationFetch,
  getInvestigationByBidId,
  markInvestigationFetchFailed,
  upsertInvestigation,
} from "@/lib/db/investigations";
import { diagnoseBid } from "@/lib/diagnostics/rules";
import { fetchRingbaBidDetail } from "@/lib/ringba/client";
import { normalizeRingbaBidDetail } from "@/lib/ringba/normalize";

interface InvestigateBidOptions {
  importRunId: string | null;
  forceRefresh?: boolean;
  waitForPendingMs?: number;
  pollIntervalMs?: number;
}

export interface InvestigationExecutionResult {
  investigation: Awaited<ReturnType<typeof getInvestigationByBidId>>;
  resolution: "fetched" | "reused" | "failed";
}

function delay(milliseconds: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForSettledInvestigation(
  bidId: string,
  maxWaitMs: number,
  pollIntervalMs: number,
) {
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitMs) {
    const current = await getInvestigationByBidId(bidId);

    if (current && current.fetchStatus !== "pending") {
      return current;
    }

    await delay(pollIntervalMs);
  }

  return getInvestigationByBidId(bidId);
}

export async function investigateBid(
  bidId: string,
  options: InvestigateBidOptions,
): Promise<InvestigationExecutionResult> {
  const claim = await claimInvestigationFetch({
    bidId,
    importRunId: options.importRunId,
    forceRefresh: options.forceRefresh ?? false,
  });

  if (!claim.shouldFetch) {
    const existing =
      claim.fetchStatus === "pending"
        ? await waitForSettledInvestigation(
            bidId,
            options.waitForPendingMs ?? 8000,
            options.pollIntervalMs ?? 500,
          )
        : await getInvestigationByBidId(bidId);

    if (!existing) {
      throw new Error(`Unable to reuse stored investigation for bid ${bidId}.`);
    }

    return {
      investigation: existing,
      resolution: "reused",
    };
  }

  try {
    const fetchResult = await fetchRingbaBidDetail(bidId);

    if (fetchResult.transportError) {
      const failedInvestigation = await markInvestigationFetchFailed({
        bidId,
        importRunId: options.importRunId,
        errorMessage: fetchResult.transportError,
        httpStatusCode: fetchResult.httpStatusCode,
        responseBody: fetchResult.rawBody,
        rawTraceJson: {
          requestUrl: fetchResult.requestUrl,
          fetchedAt: fetchResult.fetchedAt,
          httpStatusCode: fetchResult.httpStatusCode,
          responseHeaders: fetchResult.responseHeaders,
          transportError: fetchResult.transportError,
          payload:
            typeof fetchResult.rawBody === "string" || fetchResult.rawBody === null
              ? {
                  raw: fetchResult.rawBody,
                }
              : fetchResult.rawBody,
        },
      });

      return {
        investigation:
          failedInvestigation ??
          (() => {
            throw new Error(`Unable to persist failed investigation for ${bidId}.`);
          })(),
        resolution: "failed",
      };
    }

    const normalizedBid = normalizeRingbaBidDetail(fetchResult);
    const diagnosis = diagnoseBid(normalizedBid);
    const investigation = await upsertInvestigation({
      importRunId: options.importRunId,
      normalizedBid,
      diagnosis,
    });

    return {
      investigation,
      resolution: "fetched",
    };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unexpected investigation error";
    const failedInvestigation = await markInvestigationFetchFailed({
      bidId,
      importRunId: options.importRunId,
      errorMessage: message,
    });

    return {
      investigation:
        failedInvestigation ??
        (() => {
          throw new Error(`Unable to persist failed investigation for ${bidId}.`);
        })(),
      resolution: "failed",
    };
  }
}

export async function refreshInvestigation(bidId: string) {
  return investigateBid(bidId, {
    importRunId: null,
    forceRefresh: true,
  });
}

export async function getExistingOrInvestigateBid(bidId: string) {
  const result = await investigateBid(bidId, {
    importRunId: null,
    forceRefresh: false,
  });

  return result.investigation;
}
