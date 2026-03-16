import { describe, expect, it } from "vitest";

import { normalizeRingbaBidDetail } from "@/lib/ringba/normalize";

function buildAcceptedPayload() {
  return {
    isSuccessful: true,
    transactionId: "TR-accepted",
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          campaignName: "Roofing IB",
          publisherName: "ESTA Company",
          campaignId: "CA77206a2bb0314286995b2c3549574ef3",
          publisherId: "AF4ea28b4781d242beafc81114b965a3e1",
          publisherSubId: "",
          bidAmount: "44.85",
          bidDt: 1773119672218,
          bidId: "RTBfe387aa88fcd41b9b7a13d410813e747",
          bidElapsedMs: 1867,
          trace:
            '{"BaselineBid":44.848,"MaxBid":56.06,"FinalBid":44.848,"Adjustments":[]}',
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Oncore Roofing - RTT",
              targetId: "PId6608168d0504d55af58827cac867413",
              targetBuyer: "Oncore Leads",
              targetBuyerId: "buyer-oncore",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://rtb.ringba.com/v1/production/oncore.json" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"CID":"16787878433","zipCode":"30340"}' },
                {
                  name: "responseBody",
                  value:
                    '{"bidId":"RTBb823a8b4117e4e1e97f492bffa6f42e9","bidAmount":0,"rejectReason":"Final capacity check (Code: 1006)"}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.5043019Z",
              targetName: "Kyler W Roofing IB - RTT",
              targetId: "PI06af143b53dc48eb81845cd39deb7e98",
              targetBuyer: "Kyler Walterson",
              targetBuyerId: "buyer-kyler",
              eventVals: [
                { name: "duration", value: 135 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                {
                  name: "url",
                  value:
                    "https://kyler.trackdrive.com/api/v1/inbound_webhooks/ping/check_for_available_roofing_buyers",
                },
                { name: "method", value: "POST" },
                {
                  name: "requestBody",
                  value:
                    '{"trackdrive_number":"+18773556026","traffic_source_id":"1023","caller_id":"+16787878433","zip":"30340"}',
                },
                {
                  name: "responseBody",
                  value:
                    '{"success":false,"status":"no_matching_buyer","errors":["There are no matching buyers."],"buyers":[]}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
            {
              name: "PingTreePingingSummary",
              dtStamp: "2026-03-10T05:14:32.2106790Z",
              eventStrVals: [
                {
                  name: "notAcceptedRingTreeTargets",
                  value:
                    "Oncore Roofing - RTT[0,0,PId6608168d0504d55af58827cac867413] Call Acceptance Parsing Rejection, Minimum Revenue (Ring Tree Setting: $10.00) \r\nKyler W Roofing IB - RTT[0,0,PI06af143b53dc48eb81845cd39deb7e98] Call Acceptance Parsing Rejection, Missing Bid Amount, Minimum Revenue (Ring Tree Setting: $10.00) ",
                },
                {
                  name: "acceptedRingTreeTargets",
                  value: "NLD Roofing - RTT[56.06,90,PI593b6f3fcf914070b100ebb0d4cc8c87]",
                },
                {
                  name: "winningRingTreeTarget",
                  value: "NLD Roofing - RTT[56.06,90,PI593b6f3fcf914070b100ebb0d4cc8c87]",
                },
              ],
            },
            {
              name: "CallPlanDetail",
              dtStamp: "2026-03-10T05:14:32.2112369Z",
              eventStrVals: [
                {
                  name: "eligibleOrderedTargets",
                  value:
                    "Roofing IB - RT:Oncore Roofing - RTT[1,1], Roofing IB - RT:NLD Roofing - RTT[1,1], Roofing IB - RT:Kyler W Roofing IB - RTT[1,1]",
                },
              ],
            },
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:32.2089677Z",
              targetName: "NLD Roofing - RTT",
              targetId: "PI593b6f3fcf914070b100ebb0d4cc8c87",
              targetBuyer: "NLD",
              targetBuyerId: "buyer-nld",
              eventVals: [
                { name: "duration", value: 1839 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://rtb.ringba.com/v1/production/nld.json" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"CID":"16787878433","zipCode":"30340"}' },
                {
                  name: "responseBody",
                  value:
                    '{"bidId":"RTBcff49164d20f40ed80912b58495fbef8","bidAmount":56.06,"bidTerms":[{"code":100,"callMinDuration":90}],"warnings":[{"code":205,"description":"Sending calls via SIP is preferred."}]}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildZeroBidPayload() {
  return {
    isSuccessful: true,
    transactionId: "TR-zero-bid",
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          campaignName: "Roofing IB",
          publisherName: "ESTA Company",
          campaignId: "CA77206a2bb0314286995b2c3549574ef3",
          publisherId: "AF4ea28b4781d242beafc81114b965a3e1",
          bidDt: 1773119672218,
          bidId: "RTB387882ad65e84c6ab175719540697d74",
          bidElapsedMs: 2033,
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.5043019Z",
              targetName: "Kyler W Roofing IB - RTT",
              targetId: "PI06af143b53dc48eb81845cd39deb7e98",
              targetBuyer: "Kyler Walterson",
              targetBuyerId: "buyer-kyler",
              eventVals: [
                { name: "duration", value: 159 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://kyler.trackdrive.com/api/v1/ping" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"caller_id":"+17403608265","zip":"43302"}' },
                {
                  name: "responseBody",
                  value:
                    '{"success":false,"status":"no_matching_buyer","errors":["There are no matching buyers."],"buyers":[]}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Oncore Roofing - RTT",
              targetId: "PId6608168d0504d55af58827cac867413",
              targetBuyer: "Oncore Leads",
              targetBuyerId: "buyer-oncore",
              eventVals: [
                { name: "duration", value: 485 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://rtb.ringba.com/v1/production/oncore.json" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"CID":"17403608265","zipCode":"43302"}' },
                {
                  name: "responseBody",
                  value:
                    '{"bidId":"RTB9f92e57d962d46e0af4ccfd90cc50586","bidAmount":0,"rejectReason":"Final capacity check (Code: 1006)"}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
            {
              name: "PingTreePingingSummary",
              dtStamp: "2026-03-10T05:14:32.2106790Z",
              eventStrVals: [
                {
                  name: "notAcceptedRingTreeTargets",
                  value:
                    "Oncore Roofing - RTT[0,0,PId6608168d0504d55af58827cac867413] Call Acceptance Parsing Rejection, Minimum Revenue (Ring Tree Setting: $10.00) \r\nKyler W Roofing IB - RTT[0,0,PI06af143b53dc48eb81845cd39deb7e98] Call Acceptance Parsing Rejection, Missing Bid Amount, Minimum Revenue (Ring Tree Setting: $10.00) ",
                },
                { name: "acceptedRingTreeTargets", value: "" },
              ],
            },
            {
              name: "ZeroRTBBid",
              dtStamp: "2026-03-10T05:14:32.2200000Z",
            },
          ],
        },
      ],
    },
  };
}

function buildRejectedStructuredPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-structured-reject",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          campaignId: "campaign-1",
          publisherName: "Publisher",
          publisherId: "publisher-1",
          reasonForReject: "Buyer rejected request",
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Structured Buyer",
              targetId: "target-structured",
              targetBuyer: "Structured Buyer LLC",
              targetBuyerId: "buyer-structured",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://example.com/structured" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"zipCode":"30340"}' },
                {
                  name: "responseBody",
                  value:
                    '{"bidAmount":12.5,"error":{"code":4812,"message":"Buyer rejected request"},"rejectReason":"Buyer rejected request"}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildRejectedTextOnlyPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-text-reject",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          campaignId: "campaign-1",
          publisherName: "Publisher",
          publisherId: "publisher-1",
          reasonForReject: "Final capacity check code=1006",
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Text Buyer",
              targetId: "target-text",
              targetBuyer: "Text Buyer LLC",
              targetBuyerId: "buyer-text",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://example.com/text-only" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"zipCode":"30340"}' },
                {
                  name: "responseBody",
                  value:
                    '{"bidAmount":12.5,"rejectReason":"Final capacity check code=1006"}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildMissingCallerIdPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-missing-caller-id",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          campaignId: "campaign-1",
          publisherName: "Publisher",
          publisherId: "publisher-1",
          isZeroBid: true,
          reasonForReject: "Final capacity check (Code: 1006)",
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Caller ID Buyer",
              targetId: "target-caller-id",
              targetBuyer: "Caller ID Buyer LLC",
              targetBuyerId: "buyer-caller-id",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://example.com/caller-id" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"zipCode":"30340"}' },
                {
                  name: "responseBody",
                  value:
                    '{"success":false,"status":"caller_id_required","errors":["You must submit a caller_id param since this webhook requires it."],"buyers":[]}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildMissingZipPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-missing-zip",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          campaignId: "campaign-1",
          publisherName: "Publisher",
          publisherId: "publisher-1",
          isZeroBid: true,
          reasonForReject: "Final capacity check (Code: 1006)",
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Zip Buyer",
              targetId: "target-zip",
              targetBuyer: "Zip Buyer LLC",
              targetBuyerId: "buyer-zip",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 422 },
              ],
              eventStrVals: [
                { name: "url", value: "https://example.com/zip" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"caller_id":"13035550123"}' },
                {
                  name: "responseBody",
                  value:
                    '{"status":422,"success":false,"errors":{"zip":["zip is required on ping. You did not send a value."]},"metadata":{"root":null}}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Failure" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildNoMatchingBuyerPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-no-matching-buyer",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          campaignId: "campaign-1",
          publisherName: "Publisher",
          publisherId: "publisher-1",
          isZeroBid: true,
          reasonForReject: "Final capacity check (Code: 1006)",
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "No Match Buyer",
              targetId: "target-no-match",
              targetBuyer: "No Match Buyer LLC",
              targetBuyerId: "buyer-no-match",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://example.com/no-match" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"caller_id":"13035550123","zip":"30340"}' },
                {
                  name: "responseBody",
                  value:
                    '{"success":false,"status":"no_matching_buyer","errors":["There are no matching buyers."],"buyers":[]}',
                },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildZeroBidWithoutPingPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-zero-no-ping",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          publisherName: "Publisher",
          bidAmount: 0,
          reasonForReject: "Zero bid",
          events: [
            {
              name: "ZeroRTBBid",
              dtStamp: "2026-03-10T05:14:32.2200000Z",
            },
          ],
        },
      ],
    },
  };
}

function buildPartialPayload() {
  return {
    report: {
      partialResult: true,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-partial",
          campaignName: "Roofing IB",
        },
      ],
    },
  };
}

function buildTopLevelPayload() {
  return {
    bidId: "RTB-top-level",
    bidDt: 1773119672218,
    campaign: {
      id: "campaign-1",
      name: "Roofing IB",
    },
    publisher: {
      id: "publisher-1",
      name: "Publisher",
    },
    reasonForReject: "Structured top-level rejection",
    events: [
      {
        name: "PingRAWResult",
        dtStamp: "2026-03-10T05:14:30.7906085Z",
        targetName: "Top Level Buyer",
        targetId: "target-top-level",
        targetBuyer: "Top Level Buyer LLC",
        targetBuyerId: "buyer-top-level",
        eventVals: [
          { name: "duration", value: 421 },
          { name: "httpStatusCode", value: 200 },
        ],
        eventStrVals: [
          { name: "url", value: "https://example.com/top-level" },
          { name: "method", value: "POST" },
          { name: "requestBody", value: '{"zipCode":"30340"}' },
          {
            name: "responseBody",
            value:
              '{"bidAmount":12.5,"error":{"code":3001,"message":"Structured top-level rejection"},"rejectReason":"Structured top-level rejection"}',
          },
          { name: "errorMessage", value: "" },
          { name: "requestStatus", value: "Success" },
        ],
      },
    ],
  };
}

function buildMalformedJsonPayload() {
  return {
    report: {
      partialResult: false,
      totalCount: 1,
      records: [
        {
          bidId: "RTB-malformed-json",
          bidDt: 1773119672218,
          campaignName: "Roofing IB",
          publisherName: "Publisher",
          reasonForReject: "Malformed upstream payload",
          events: [
            {
              name: "PingRAWResult",
              dtStamp: "2026-03-10T05:14:30.7906085Z",
              targetName: "Malformed Buyer",
              targetId: "target-malformed",
              targetBuyer: "Malformed Buyer LLC",
              targetBuyerId: "buyer-malformed",
              eventVals: [
                { name: "duration", value: 421 },
                { name: "httpStatusCode", value: 200 },
              ],
              eventStrVals: [
                { name: "url", value: "https://example.com/malformed" },
                { name: "method", value: "POST" },
                { name: "requestBody", value: '{"zipCode":"30340"' },
                { name: "responseBody", value: '{"rejectReason":"Malformed upstream payload"' },
                { name: "errorMessage", value: "" },
                { name: "requestStatus", value: "Success" },
              ],
            },
          ],
        },
      ],
    },
  };
}

function buildFetchResult(rawBody: Record<string, unknown>, bidId: string) {
  return {
    bidId,
    requestUrl: `https://api.ringba.com/v2/account/rtb/bid/${bidId}`,
    fetchedAt: "2026-03-10T05:14:33.000Z",
    httpStatusCode: 200,
    ok: true,
    rawBody,
    responseHeaders: {},
    transportError: null,
    errorKind: "none" as const,
    latencyMs: 100,
    attemptCount: 1,
    retryAfterMs: null,
  };
}

describe("normalizeRingbaBidDetail", () => {
  it("extracts target attempts and winning target details from report records", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(
        buildAcceptedPayload(),
        "RTBfe387aa88fcd41b9b7a13d410813e747",
      ),
    );

    expect(normalized.bidDt).toBe("2026-03-10T05:14:32.218Z");
    expect(normalized.outcome).toBe("accepted");
    expect(normalized.primaryFailureStage).toBe("accepted");
    expect(normalized.primaryTargetName).toBe("NLD Roofing - RTT");
    expect(normalized.primaryBuyerName).toBe("NLD");
    expect(normalized.targetAttempts).toHaveLength(3);
    expect(normalized.targetAttempts[1]?.errors).toEqual(["There are no matching buyers."]);
    expect(normalized.targetAttempts[1]?.summaryReason).toContain("Missing Bid Amount");
    expect(normalized.targetAttempts[2]?.winning).toBe(true);
    expect(normalized.targetAttempts[2]?.bidAmount).toBe(56.06);
    expect(normalized.targetAttempts[2]?.minDurationSeconds).toBe(90);
    expect(normalized.requestBody).toEqual({
      CID: "16787878433",
      zipCode: "30340",
    });
    expect(normalized.rawTraceJson.latencyMs).toBe(100);
    expect(normalized.rawTraceJson.attemptCount).toBe(1);
    expect(normalized.rawTraceJson.errorKind).toBe("none");
    expect(normalized.parseStatus).toBe("complete");
    expect(normalized.outcomeReasonCategory).toBe("accepted");
    expect(normalized.classificationSource).toBe("heuristic");
    expect(normalized.schemaVariant).toBe("report_records");
    expect(normalized.normalizationVersion).toBe("ringba-normalizer-v2");
    expect(normalized.normalizationConfidence).toBeGreaterThan(0.7);
  });

  it("promotes the most actionable rejected target for zero-bid summaries", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(
        buildZeroBidPayload(),
        "RTB387882ad65e84c6ab175719540697d74",
      ),
    );

    expect(normalized.outcome).toBe("zero_bid");
    expect(normalized.primaryFailureStage).toBe("zero_bid");
    expect(normalized.primaryTargetName).toBe("Oncore Roofing - RTT");
    expect(normalized.primaryBuyerName).toBe("Oncore Leads");
    expect(normalized.primaryErrorCode).toBe(1006);
    expect(normalized.primaryErrorMessage).toBe("Final capacity check (Code: 1006)");
    expect(normalized.targetAttempts[0]?.errorMessage).toBe("no_matching_buyer");
    expect(normalized.targetAttempts[1]?.summaryReason).toContain("Minimum Revenue");
    expect(normalized.parseStatus).toBe("text_fallback");
    expect(normalized.outcomeReasonCategory).toBe("tag_filtered_final");
    expect(normalized.primaryErrorCodeSource).toBe("rejectReason_text");
    expect(
      normalized.normalizationWarnings.some(
        (warning) => warning.code === "primary_error_code_text_fallback",
      ),
    ).toBe(true);
  });

  it("prefers structured error codes over text fallback when the response body has an error object", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildRejectedStructuredPayload(), "RTB-structured-reject"),
    );

    expect(normalized.outcome).toBe("rejected");
    expect(normalized.primaryErrorCode).toBe(4812);
    expect(normalized.primaryErrorCodeSource).toBe(
      "targetAttempts[0].responseBody.error.code",
    );
    expect(normalized.primaryErrorCodeConfidence).toBe(1);
    expect(normalized.outcomeReasonCategory).toBe("request_invalid");
    expect(normalized.classificationSource).toBe("primary_attempt_structured");
    expect(normalized.parseStatus).toBe("complete");
  });

  it("falls back to text parsing when only text contains the error code", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildRejectedTextOnlyPayload(), "RTB-text-reject"),
    );

    expect(normalized.outcome).toBe("zero_bid");
    expect(normalized.primaryErrorCode).toBe(1006);
    expect(normalized.primaryErrorCodeSource).toBe("rejectReason_text");
    expect(normalized.outcomeReasonCategory).toBe("tag_filtered_final");
    expect(normalized.parseStatus).toBe("text_fallback");
  });

  it("marks zero-bid payloads without PingRAWResult attempts as partial", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildZeroBidWithoutPingPayload(), "RTB-zero-no-ping"),
    );

    expect(normalized.outcome).toBe("zero_bid");
    expect(normalized.targetAttempts).toHaveLength(0);
    expect(normalized.parseStatus).toBe("partial");
    expect(normalized.missingCriticalFields).toContain("targetAttempts");
  });

  it("flags partial reports with missing critical fields", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildPartialPayload(), "RTB-partial"),
    );

    expect(normalized.parseStatus).toBe("partial");
    expect(normalized.normalizationWarnings.some((warning) => warning.code === "partial_report")).toBe(
      true,
    );
    expect(normalized.missingCriticalFields).toContain("bidDt");
  });

  it("supports top-level payloads without report.records nesting", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildTopLevelPayload(), "RTB-top-level"),
    );

    expect(normalized.schemaVariant).toBe("top_level_record");
    expect(normalized.outcome).toBe("rejected");
    expect(normalized.primaryErrorCode).toBe(3001);
    expect(normalized.outcomeReasonCategory).toBe("request_invalid");
    expect(normalized.primaryErrorCodeSource).toBe(
      "targetAttempts[0].responseBody.error.code",
    );
  });

  it("prefers structured caller id failures over generic top-level final capacity text", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildMissingCallerIdPayload(), "RTB-missing-caller-id"),
    );

    expect(normalized.outcome).toBe("rejected");
    expect(normalized.outcomeReasonCategory).toBe("missing_caller_id");
    expect(normalized.classificationSource).toBe("primary_attempt_structured");
    expect(normalized.outcomeReasonCode).toBe("caller_id_required");
    expect(normalized.classificationWarnings.some((warning) => warning.code === "classification_conflict")).toBe(
      true,
    );
  });

  it("classifies missing required field payloads from structured response bodies", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildMissingZipPayload(), "RTB-missing-zip"),
    );

    expect(normalized.outcome).toBe("rejected");
    expect(normalized.outcomeReasonCategory).toBe("missing_required_field");
    expect(normalized.classificationSource).toBe("primary_attempt_structured");
  });

  it("classifies no matching buyer payloads without collapsing them into buyer zero bid", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildNoMatchingBuyerPayload(), "RTB-no-matching-buyer"),
    );

    expect(normalized.outcome).toBe("zero_bid");
    expect(normalized.outcomeReasonCategory).toBe("no_matching_buyer");
    expect(normalized.classificationSource).toBe("primary_attempt_structured");
  });

  it("records warnings when request or response bodies contain malformed JSON", () => {
    const normalized = normalizeRingbaBidDetail(
      buildFetchResult(buildMalformedJsonPayload(), "RTB-malformed-json"),
    );

    expect(normalized.normalizationWarnings.some((warning) => warning.code === "json_parse_failed")).toBe(
      true,
    );
    expect(normalized.targetAttempts).toHaveLength(1);
  });
});
