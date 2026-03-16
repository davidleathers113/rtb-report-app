import "server-only";

import {
  claimInvestigationFetch,
  getInvestigationByBidId,
  markInvestigationFetchFailed,
  upsertInvestigation,
} from "@/lib/db/investigations";
import { diagnoseBid } from "@/lib/diagnostics/rules";
import { fetchRingbaBidDetail, type RingbaFetchResult } from "@/lib/ringba/client";
import { normalizeRingbaBidDetail } from "@/lib/ringba/normalize";
import { isRecord } from "@/lib/utils/json";

interface InvestigateBidOptions {
  importRunId: string | null;
  forceRefresh?: boolean;
  waitForPendingMs?: number;
  pollIntervalMs?: number;
  sourceType?: string;
}

export interface InvestigationExecutionResult {
  investigation: Awaited<ReturnType<typeof getInvestigationByBidId>>;
  resolution: "fetched" | "reused" | "failed";
  fetchTelemetry: {
    latencyMs: number;
    attemptCount: number;
    errorKind: RingbaFetchResult["errorKind"];
  } | null;
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
      claim.blockReason === "pending"
        ? await waitForSettledInvestigation(
            bidId,
            options.waitForPendingMs ?? 8000,
            options.pollIntervalMs ?? 500,
          )
        : await getInvestigationByBidId(bidId);

    if (!existing) {
      throw new Error(`Unable to reuse stored investigation for bid ${bidId}.`);
    }

    if (claim.blockReason === "retry_scheduled") {
      return {
        investigation: existing,
        resolution: existing.fetchStatus === "failed" ? "failed" : "reused",
        fetchTelemetry: null,
      };
    }

    return {
      investigation: existing,
      resolution: "reused",
      fetchTelemetry: null,
    };
  }

  try {
    const fetchResult = await fetchRingbaBidDetail(bidId, {
      budgetProfile:
        options.sourceType === "historical_ringba_backfill"
          ? "historical_backfill"
          : "default",
    });
    const nextRingbaRetryAt =
      fetchResult.retryAfterMs !== null
        ? new Date(Date.now() + fetchResult.retryAfterMs).toISOString()
        : null;

    if (fetchResult.transportError || !fetchResult.ok) {
      const failedInvestigation = await markInvestigationFetchFailed({
        bidId,
        importRunId: options.importRunId,
        errorMessage:
          fetchResult.transportError ??
          `Ringba bid detail request failed with HTTP ${fetchResult.httpStatusCode}.`,
        httpStatusCode: fetchResult.httpStatusCode,
        responseBody: fetchResult.rawBody,
        rawTraceJson: {
          requestUrl: fetchResult.requestUrl,
          fetchedAt: fetchResult.fetchedAt,
          httpStatusCode: fetchResult.httpStatusCode,
          errorKind: fetchResult.errorKind,
          latencyMs: fetchResult.latencyMs,
          attemptCount: fetchResult.attemptCount,
          responseHeaders: fetchResult.responseHeaders,
          transportError: fetchResult.transportError,
          payload:
            typeof fetchResult.rawBody === "string" || fetchResult.rawBody === null
              ? {
                  raw: fetchResult.rawBody,
                }
              : fetchResult.rawBody,
        },
        enrichmentState: fetchResult.errorKind === "not_found" ? "not_found" : "failed",
        nextRingbaRetryAt:
          fetchResult.errorKind === "rate_limited" ||
          fetchResult.errorKind === "server_error" ||
          fetchResult.errorKind === "transport_error"
            ? nextRingbaRetryAt
            : null,
      });

      return {
        investigation:
          failedInvestigation ??
          (() => {
            throw new Error(`Unable to persist failed investigation for ${bidId}.`);
          })(),
        resolution: "failed",
        fetchTelemetry: {
          latencyMs: fetchResult.latencyMs,
          attemptCount: fetchResult.attemptCount,
          errorKind: fetchResult.errorKind,
        },
      };
    }

    const normalizedBid = normalizeRingbaBidDetail(fetchResult);
    const diagnosis = diagnoseBid(normalizedBid);
    const investigation = await upsertInvestigation({
      importRunId: options.importRunId,
      normalizedBid,
      diagnosis,
      persistence: {
        detailSource: "ringba_api",
        enrichmentState: "enriched",
        lastRingbaAttemptAt: fetchResult.fetchedAt,
        lastRingbaFetchAt: fetchResult.fetchedAt,
        ringbaFailureCount: 0,
        nextRingbaRetryAt: null,
      },
    });

    return {
      investigation,
      resolution: "fetched",
      fetchTelemetry: {
        latencyMs: fetchResult.latencyMs,
        attemptCount: fetchResult.attemptCount,
        errorKind: fetchResult.errorKind,
      },
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
      fetchTelemetry: null,
    };
  }
}

export async function refreshInvestigation(bidId: string) {
  return investigateBid(bidId, {
    importRunId: null,
    forceRefresh: true,
  });
}

export async function reclassifyStoredInvestigation(bidId: string) {
  const existing = await getInvestigationByBidId(bidId);
  if (!existing) {
    throw new Error(`Unable to load stored investigation for bid ${bidId}.`);
  }

  if (existing.fetchStatus !== "fetched") {
    throw new Error(
      `Stored investigation for bid ${bidId} is not fetched, so there is not enough persisted evidence to reclassify it.`,
    );
  }

  const rawTrace = isRecord(existing.rawTraceJson) ? existing.rawTraceJson : {};
  const payload = rawTrace.payload;
  const rawBody =
    isRecord(payload) &&
    Object.keys(payload).length === 1 &&
    typeof payload.raw === "string"
      ? payload.raw
      : (payload as RingbaFetchResult["rawBody"]);
  const responseHeaders = isRecord(rawTrace.responseHeaders)
    ? Object.fromEntries(
        Object.entries(rawTrace.responseHeaders)
          .filter((entry): entry is [string, string] => typeof entry[1] === "string"),
      )
    : {};
  const storedFetchResult: RingbaFetchResult = {
    bidId: existing.bidId,
    requestUrl: typeof rawTrace.requestUrl === "string" ? rawTrace.requestUrl : "",
    fetchedAt:
      typeof rawTrace.fetchedAt === "string"
        ? rawTrace.fetchedAt
        : (existing.fetchedAt ?? existing.updatedAt),
    httpStatusCode:
      typeof rawTrace.httpStatusCode === "number"
        ? rawTrace.httpStatusCode
        : existing.httpStatusCode,
    ok: true,
    rawBody: rawBody ?? existing.responseBody,
    responseHeaders,
    transportError: typeof rawTrace.transportError === "string" ? rawTrace.transportError : null,
    errorKind: typeof rawTrace.errorKind === "string" ? rawTrace.errorKind as RingbaFetchResult["errorKind"] : "none",
    latencyMs: typeof rawTrace.latencyMs === "number" ? rawTrace.latencyMs : 0,
    attemptCount: typeof rawTrace.attemptCount === "number" ? rawTrace.attemptCount : 1,
    retryAfterMs: null,
  };

  const normalizedBid = normalizeRingbaBidDetail(storedFetchResult);
  const diagnosis = diagnoseBid(normalizedBid);

  return upsertInvestigation({
    importRunId: existing.importRunId,
    normalizedBid,
    diagnosis,
    persistence: {
      detailSource: existing.detailSource,
      enrichmentState: existing.enrichmentState,
      lastRingbaAttemptAt: existing.lastRingbaAttemptAt,
      lastRingbaFetchAt: existing.lastRingbaFetchAt,
      ringbaFailureCount: existing.ringbaFailureCount,
      nextRingbaRetryAt: existing.nextRingbaRetryAt,
      fetchedAt: existing.fetchedAt,
      preserveImportedAt: true,
    },
  });
}

export async function getExistingOrInvestigateBid(bidId: string) {
  const result = await investigateBid(bidId, {
    importRunId: null,
    forceRefresh: false,
  });

  return result.investigation;
}
