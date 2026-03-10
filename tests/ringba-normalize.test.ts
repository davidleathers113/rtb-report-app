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

describe("normalizeRingbaBidDetail", () => {
  it("extracts target attempts and winning target details from report records", () => {
    const normalized = normalizeRingbaBidDetail({
      bidId: "RTBfe387aa88fcd41b9b7a13d410813e747",
      requestUrl: "https://api.ringba.com/v2/account/rtb/bid/RTBfe387aa88fcd41b9b7a13d410813e747",
      fetchedAt: "2026-03-10T05:14:33.000Z",
      httpStatusCode: 200,
      ok: true,
      rawBody: buildAcceptedPayload(),
      responseHeaders: {},
      transportError: null,
      errorKind: "none",
      latencyMs: 100,
      attemptCount: 1,
      retryAfterMs: null,
    });

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
  });

  it("promotes the most actionable rejected target for zero-bid summaries", () => {
    const normalized = normalizeRingbaBidDetail({
      bidId: "RTB387882ad65e84c6ab175719540697d74",
      requestUrl: "https://api.ringba.com/v2/account/rtb/bid/RTB387882ad65e84c6ab175719540697d74",
      fetchedAt: "2026-03-10T05:14:33.000Z",
      httpStatusCode: 200,
      ok: true,
      rawBody: buildZeroBidPayload(),
      responseHeaders: {},
      transportError: null,
      errorKind: "none",
      latencyMs: 100,
      attemptCount: 1,
      retryAfterMs: null,
    });

    expect(normalized.outcome).toBe("zero_bid");
    expect(normalized.primaryFailureStage).toBe("zero_bid");
    expect(normalized.primaryTargetName).toBe("Oncore Roofing - RTT");
    expect(normalized.primaryBuyerName).toBe("Oncore Leads");
    expect(normalized.primaryErrorCode).toBe(1006);
    expect(normalized.primaryErrorMessage).toBe("Final capacity check (Code: 1006)");
    expect(normalized.targetAttempts[0]?.errorMessage).toBe("no_matching_buyer");
    expect(normalized.targetAttempts[1]?.summaryReason).toContain("Minimum Revenue");
  });
});
