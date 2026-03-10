import { describe, expect, it } from "vitest";

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
    ...overrides,
  };
}

describe("diagnoseBid", () => {
  it("classifies missing caller id", () => {
    const result = diagnoseBid(
      buildBid({
        httpStatusCode: 422,
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
        responseBody: {
          message: "too many requests",
        },
      }),
    );

    expect(result.rootCause).toBe("rate_limited");
  });
});
