import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db/investigations", () => ({
  claimInvestigationFetch: vi.fn(),
  getInvestigationByBidId: vi.fn(),
  markInvestigationFetchFailed: vi.fn(),
  upsertInvestigation: vi.fn(),
}));

vi.mock("@/lib/ringba/client", () => ({
  fetchRingbaBidDetail: vi.fn(),
}));

vi.mock("@/lib/ringba/normalize", () => ({
  normalizeRingbaBidDetail: vi.fn(),
}));

vi.mock("@/lib/diagnostics/rules", () => ({
  diagnoseBid: vi.fn(),
}));

import {
  claimInvestigationFetch,
  getInvestigationByBidId,
  markInvestigationFetchFailed,
  upsertInvestigation,
} from "@/lib/db/investigations";
import { diagnoseBid } from "@/lib/diagnostics/rules";
import {
  investigateBid,
  reclassifyStoredInvestigation,
} from "@/lib/investigations/service";
import { fetchRingbaBidDetail } from "@/lib/ringba/client";
import { normalizeRingbaBidDetail } from "@/lib/ringba/normalize";
import type { InvestigationDetail } from "@/types/bid";

function buildInvestigation(
  overrides: Partial<InvestigationDetail> = {},
): InvestigationDetail {
  return {
    id: "investigation-1",
    importRunId: null,
    bidId: "bid-1",
    bidDt: "2026-03-09T00:00:00.000Z",
    campaignName: "Campaign",
    campaignId: "campaign-1",
    publisherName: "Publisher",
    publisherId: "publisher-1",
    targetName: "Target",
    targetId: "target-1",
    buyerName: "Buyer",
    buyerId: "buyer-1",
    bidAmount: 1.25,
    winningBid: 1.25,
    bidElapsedMs: 100,
    isZeroBid: false,
    reasonForReject: null,
    httpStatusCode: 200,
    errorMessage: null,
    primaryFailureStage: "accepted",
    primaryTargetName: null,
    primaryTargetId: null,
    primaryBuyerName: null,
    primaryBuyerId: null,
    primaryErrorCode: null,
    primaryErrorMessage: null,
    requestBody: {},
    responseBody: {},
    rawTraceJson: {},
    relevantEvents: [],
    targetAttempts: [],
    events: [],
    sourceContext: null,
    outcome: "accepted",
    outcomeReasonCategory: "accepted",
    outcomeReasonCode: null,
    outcomeReasonMessage: null,
    classificationSource: "heuristic",
    classificationConfidence: 0.99,
    classificationWarnings: [],
    parseStatus: "complete",
    normalizationVersion: "test-v1",
    schemaVariant: "test_fixture",
    normalizationConfidence: 1,
    normalizationWarnings: [],
    missingCriticalFields: [],
    missingOptionalFields: [],
    unknownEventNames: [],
    rawPathsUsed: {},
    primaryErrorCodeSource: null,
    primaryErrorCodeConfidence: null,
    primaryErrorCodeRawMatch: null,
    rootCause: "unknown_needs_review",
    confidence: 0.5,
    severity: "medium",
    ownerType: "unknown",
    suggestedFix: "Review manually.",
    explanation: "Stored investigation.",
    evidence: [],
    detailSource: "ringba_api",
    enrichmentState: "enriched",
    fetchStatus: "fetched",
    fetchedAt: "2026-03-09T00:01:00.000Z",
    fetchStartedAt: "2026-03-09T00:00:30.000Z",
    lastError: null,
    lastRingbaAttemptAt: "2026-03-09T00:00:30.000Z",
    lastRingbaFetchAt: "2026-03-09T00:01:00.000Z",
    ringbaFailureCount: 0,
    nextRingbaRetryAt: null,
    refreshRequestedAt: null,
    leaseExpiresAt: null,
    fetchAttemptCount: 1,
    importedAt: "2026-03-09T00:01:00.000Z",
    createdAt: "2026-03-09T00:01:00.000Z",
    updatedAt: "2026-03-09T00:01:00.000Z",
    ...overrides,
  };
}

describe("investigateBid", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("reuses an existing investigation when the claim says no fetch", async () => {
    vi.mocked(claimInvestigationFetch).mockResolvedValue({
      id: "investigation-1",
      bidId: "bid-1",
      fetchStatus: "fetched",
      enrichmentState: "enriched",
      shouldFetch: false,
      blockReason: "reused",
      fetchedAt: "2026-03-09T00:01:00.000Z",
      lastError: null,
      fetchAttemptCount: 1,
      leaseExpiresAt: null,
    });
    vi.mocked(getInvestigationByBidId).mockResolvedValue(buildInvestigation());

    const result = await investigateBid("bid-1", {
      importRunId: null,
      forceRefresh: false,
    });

    expect(result.resolution).toBe("reused");
    expect(result.fetchTelemetry).toBeNull();
    expect(fetchRingbaBidDetail).not.toHaveBeenCalled();
    expect(result.investigation?.bidId).toBe("bid-1");
  });

  it("fetches and persists when the claim allows a fetch", async () => {
    vi.mocked(claimInvestigationFetch).mockResolvedValue({
      id: "investigation-1",
      bidId: "bid-1",
      fetchStatus: "pending",
      enrichmentState: "fetching",
      shouldFetch: true,
      blockReason: null,
      fetchedAt: null,
      lastError: null,
      fetchAttemptCount: 1,
      leaseExpiresAt: "2026-03-09T00:03:00.000Z",
    });
    vi.mocked(fetchRingbaBidDetail).mockResolvedValue({
      bidId: "bid-1",
      requestUrl: "https://api.example.com",
      fetchedAt: "2026-03-09T00:02:00.000Z",
      httpStatusCode: 200,
      ok: true,
      rawBody: { bidId: "bid-1" },
      responseHeaders: {},
      transportError: null,
      errorKind: "none",
      latencyMs: 120,
      attemptCount: 1,
      retryAfterMs: null,
    });
    vi.mocked(normalizeRingbaBidDetail).mockReturnValue({
      bidId: "bid-1",
      bidDt: "2026-03-09T00:00:00.000Z",
      campaignName: "Campaign",
      campaignId: "campaign-1",
      publisherName: "Publisher",
      publisherId: "publisher-1",
      targetName: "Target",
      targetId: "target-1",
      buyerName: "Buyer",
      buyerId: "buyer-1",
      bidAmount: 1.25,
      winningBid: 1.25,
      bidElapsedMs: 100,
      isZeroBid: false,
      reasonForReject: null,
      httpStatusCode: 200,
      errorMessage: null,
      primaryFailureStage: "accepted",
      primaryTargetName: null,
      primaryTargetId: null,
      primaryBuyerName: null,
      primaryBuyerId: null,
      primaryErrorCode: null,
      primaryErrorMessage: null,
      requestBody: {},
      responseBody: {},
      rawTraceJson: {},
      relevantEvents: [],
      targetAttempts: [],
      outcome: "accepted",
      outcomeReasonCategory: "accepted",
      outcomeReasonCode: null,
      outcomeReasonMessage: null,
      classificationSource: "heuristic",
      classificationConfidence: 0.99,
      classificationWarnings: [],
      parseStatus: "complete",
      normalizationVersion: "test-v1",
      schemaVariant: "test_fixture",
      normalizationConfidence: 1,
      normalizationWarnings: [],
      missingCriticalFields: [],
      missingOptionalFields: [],
      unknownEventNames: [],
      rawPathsUsed: {},
      primaryErrorCodeSource: null,
      primaryErrorCodeConfidence: null,
      primaryErrorCodeRawMatch: null,
    });
    vi.mocked(diagnoseBid).mockReturnValue({
      rootCause: "unknown_needs_review",
      confidence: 0.99,
      severity: "low",
      ownerType: "unknown",
      suggestedFix: "No failure remediation is needed for accepted bids.",
      explanation: "The winning bid was accepted, so failure diagnostics were skipped.",
      evidence: [],
    });
    vi.mocked(upsertInvestigation).mockResolvedValue(buildInvestigation());

    const result = await investigateBid("bid-1", {
      importRunId: "run-1",
      forceRefresh: false,
    });

    expect(result.resolution).toBe("fetched");
    expect(fetchRingbaBidDetail).toHaveBeenCalledWith("bid-1", {
      budgetProfile: "default",
    });
    expect(upsertInvestigation).toHaveBeenCalledWith(
      expect.objectContaining({
        normalizedBid: expect.objectContaining({
          outcome: "accepted",
          outcomeReasonCategory: "accepted",
          parseStatus: "complete",
          normalizationVersion: "test-v1",
        }),
        diagnosis: expect.objectContaining({
          rootCause: "unknown_needs_review",
          severity: "low",
        }),
      }),
    );
    expect(result.fetchTelemetry).toEqual({
      latencyMs: 120,
      attemptCount: 1,
      errorKind: "none",
    });
  });

  it("persists a failed state when the upstream fetch has a transport error", async () => {
    vi.mocked(claimInvestigationFetch).mockResolvedValue({
      id: "investigation-1",
      bidId: "bid-1",
      fetchStatus: "pending",
      enrichmentState: "fetching",
      shouldFetch: true,
      blockReason: null,
      fetchedAt: null,
      lastError: null,
      fetchAttemptCount: 1,
      leaseExpiresAt: "2026-03-09T00:03:00.000Z",
    });
    vi.mocked(fetchRingbaBidDetail).mockResolvedValue({
      bidId: "bid-1",
      requestUrl: "https://api.example.com",
      fetchedAt: "2026-03-09T00:02:00.000Z",
      httpStatusCode: null,
      ok: false,
      rawBody: {
        error: "ringba_transport_error",
      },
      responseHeaders: {},
      transportError: "network timeout",
      errorKind: "transport_error",
      latencyMs: 1000,
      attemptCount: 1,
      retryAfterMs: 1000,
    });
    vi.mocked(markInvestigationFetchFailed).mockResolvedValue(
      buildInvestigation({
        fetchStatus: "failed",
        lastError: "network timeout",
      }),
    );

    const result = await investigateBid("bid-1", {
      importRunId: "run-1",
      forceRefresh: true,
    });

    expect(result.resolution).toBe("failed");
    expect(markInvestigationFetchFailed).toHaveBeenCalled();
    expect(result.fetchTelemetry).toEqual({
      latencyMs: 1000,
      attemptCount: 1,
      errorKind: "transport_error",
    });
    expect(result.investigation?.fetchStatus).toBe("failed");
  });

  it("reclassifies a stored fetched investigation from persisted raw payloads", async () => {
    vi.mocked(getInvestigationByBidId).mockResolvedValue(
      buildInvestigation({
        rawTraceJson: {
          requestUrl: "https://api.example.com/bid-1",
          fetchedAt: "2026-03-09T00:02:00.000Z",
          httpStatusCode: 200,
          errorKind: "none",
          latencyMs: 120,
          attemptCount: 1,
          responseHeaders: {},
          payload: {
            bidId: "bid-1",
            report: {
              records: [
                {
                  bidId: "bid-1",
                },
              ],
            },
          },
        },
      }),
    );
    vi.mocked(normalizeRingbaBidDetail).mockReturnValue({
      bidId: "bid-1",
      bidDt: "2026-03-09T00:00:00.000Z",
      campaignName: "Campaign",
      campaignId: "campaign-1",
      publisherName: "Publisher",
      publisherId: "publisher-1",
      targetName: "Target",
      targetId: "target-1",
      buyerName: "Buyer",
      buyerId: "buyer-1",
      bidAmount: 1.25,
      winningBid: 1.25,
      bidElapsedMs: 100,
      isZeroBid: false,
      reasonForReject: null,
      httpStatusCode: 200,
      errorMessage: null,
      primaryFailureStage: "accepted",
      primaryTargetName: null,
      primaryTargetId: null,
      primaryBuyerName: null,
      primaryBuyerId: null,
      primaryErrorCode: null,
      primaryErrorMessage: null,
      requestBody: {},
      responseBody: {},
      rawTraceJson: {},
      relevantEvents: [],
      targetAttempts: [],
      outcome: "accepted",
      outcomeReasonCategory: "accepted",
      outcomeReasonCode: null,
      outcomeReasonMessage: null,
      classificationSource: "heuristic",
      classificationConfidence: 0.99,
      classificationWarnings: [],
      parseStatus: "complete",
      normalizationVersion: "test-v1",
      schemaVariant: "test_fixture",
      normalizationConfidence: 1,
      normalizationWarnings: [],
      missingCriticalFields: [],
      missingOptionalFields: [],
      unknownEventNames: [],
      rawPathsUsed: {},
      primaryErrorCodeSource: null,
      primaryErrorCodeConfidence: null,
      primaryErrorCodeRawMatch: null,
    });
    vi.mocked(diagnoseBid).mockReturnValue({
      rootCause: "unknown_needs_review",
      confidence: 0.5,
      severity: "medium",
      ownerType: "unknown",
      suggestedFix: "Review manually.",
      explanation: "Investigated.",
      evidence: [],
    });
    vi.mocked(upsertInvestigation).mockResolvedValue(buildInvestigation());

    await reclassifyStoredInvestigation("bid-1");

    expect(normalizeRingbaBidDetail).toHaveBeenCalledWith(
      expect.objectContaining({
        bidId: "bid-1",
        requestUrl: "https://api.example.com/bid-1",
        httpStatusCode: 200,
        ok: true,
      }),
    );
    expect(upsertInvestigation).toHaveBeenCalledWith(
      expect.objectContaining({
        persistence: expect.objectContaining({
          fetchedAt: "2026-03-09T00:01:00.000Z",
          preserveImportedAt: true,
        }),
      }),
    );
  });
});
