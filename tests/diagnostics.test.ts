import { afterEach, describe, expect, it } from "vitest";

import { diagnoseBid } from "@/lib/diagnostics/rules";
import type { NormalizedBidData } from "@/types/bid";

function buildBid(overrides: Partial<NormalizedBidData>): NormalizedBidData {
  return {
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
    requestBody: { caller_id: "123" },
    responseBody: { ok: true },
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
    ...overrides,
  };
}

describe("diagnoseBid", () => {
  afterEach(() => {
    delete process.env.MINIMUM_REVENUE_THRESHOLD;
  });

  it("classifies missing caller id", () => {
    const result = diagnoseBid(
      buildBid({
        httpStatusCode: 422,
        outcome: "rejected",
        outcomeReasonCategory: "missing_caller_id",
        outcomeReasonCode: "caller_id_required",
        outcomeReasonMessage: "caller_id_required",
        classificationSource: "response_body_structured",
        classificationConfidence: 0.98,
        responseBody: {
          error: "caller_id_required",
        },
        requestBody: {},
      }),
    );

    expect(result.rootCause).toBe("missing_caller_id");
    expect(result.ownerType).toBe("publisher");
  });

  it("classifies rate limited responses", () => {
    const result = diagnoseBid(
      buildBid({
        httpStatusCode: 429,
        outcome: "rejected",
        outcomeReasonCategory: "rate_limited",
        outcomeReasonCode: "429",
        outcomeReasonMessage: "too many requests",
        classificationSource: "top_level_error",
        classificationConfidence: 0.88,
        responseBody: {
          message: "too many requests",
        },
      }),
    );

    expect(result.rootCause).toBe("rate_limited");
  });

  it("prefers derived no matching buyer classifications over zero-bid heuristics", () => {
    const result = diagnoseBid(
      buildBid({
        outcome: "zero_bid",
        isZeroBid: true,
        outcomeReasonCategory: "no_matching_buyer",
        outcomeReasonCode: "no_matching_buyer",
        outcomeReasonMessage: "There are no matching buyers.",
        classificationSource: "primary_attempt_structured",
        classificationConfidence: 0.98,
        responseBody: {
          success: false,
          status: "no_matching_buyer",
          errors: ["There are no matching buyers."],
        },
      }),
    );

    expect(result.rootCause).toBe("no_eligible_targets");
    expect(result.ownerType).toBe("ringba_config");
  });

  it("keeps accepted bids neutral even when loser attempts contain failure text", () => {
    const result = diagnoseBid(
      buildBid({
        relevantEvents: [
          {
            eventName: "PingRAWResult",
            eventTimestamp: "2026-03-09T00:00:01.000Z",
            eventValsJson: {
              httpStatusCode: 200,
            },
            eventStrValsJson: {
              responseBody:
                '{"success":false,"status":"caller_id_required","errors":["You must submit a caller_id param since this webhook requires it."]}',
              rejectReason: "Final capacity check (Code: 1006)",
            },
          },
        ],
        responseBody: {
          bidAmount: 56.06,
          bidTerms: [{ code: 100, callMinDuration: 90 }],
        },
      }),
    );

    expect(result.rootCause).toBe("unknown_needs_review");
    expect(result.severity).toBe("low");
    expect(result.explanation).toContain("accepted");
  });

  it("does not apply minimum revenue failure rules to accepted bids", () => {
    process.env.MINIMUM_REVENUE_THRESHOLD = "10";

    const result = diagnoseBid(
      buildBid({
        bidAmount: 5,
        winningBid: 5,
      }),
    );

    expect(result.rootCause).toBe("unknown_needs_review");
    expect(result.severity).toBe("low");
  });
});
