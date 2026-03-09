import type { BidEvent, InvestigationOutcome, NormalizedBidData } from "@/types/bid";
import type { RingbaFetchResult } from "@/lib/ringba/client";
import { isRecord } from "@/lib/utils/json";

function readPath(
  source: Record<string, unknown> | undefined,
  path: string,
): unknown {
  if (!source) {
    return undefined;
  }

  const parts = path.split(".");
  let current: unknown = source;

  for (const part of parts) {
    if (!isRecord(current)) {
      return undefined;
    }

    current = current[part];
  }

  return current;
}

function pickFirstValue(
  source: Record<string, unknown> | undefined,
  paths: string[],
): unknown {
  for (const path of paths) {
    const value = readPath(source, path);

    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return null;
}

function toStringValue(value: unknown) {
  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return null;
}

function toNumberValue(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function toObjectOrString(
  value: unknown,
): Record<string, unknown> | string | null {
  if (isRecord(value)) {
    return value;
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return {
      items: value,
    };
  }

  return null;
}

function extractEvents(source: Record<string, unknown> | undefined): BidEvent[] {
  if (!source) {
    return [];
  }

  const candidates = [
    "events",
    "traceEvents",
    "bidEvents",
    "trace.events",
    "timeline",
  ];

  let rawEvents: unknown = null;

  for (const candidate of candidates) {
    const value = readPath(source, candidate);
    if (Array.isArray(value)) {
      rawEvents = value;
      break;
    }
  }

  if (!Array.isArray(rawEvents)) {
    return [];
  }

  return rawEvents.map((entry, index) => {
    if (isRecord(entry)) {
      const eventStrVals = readPath(entry, "strVals");
      return {
        eventName:
          toStringValue(
            pickFirstValue(entry, ["eventName", "name", "type", "event"]),
          ) ?? `event_${index + 1}`,
        eventTimestamp: toStringValue(
          pickFirstValue(entry, [
            "eventTimestamp",
            "timestamp",
            "dt",
            "createdAt",
          ]),
        ),
        eventValsJson: isRecord(readPath(entry, "vals"))
          ? (readPath(entry, "vals") as Record<string, unknown>)
          : isRecord(entry)
            ? entry
            : null,
        eventStrValsJson: isRecord(eventStrVals)
          ? Object.fromEntries(
              Object.entries(eventStrVals).map(([key, value]) => [
                key,
                toStringValue(value) ?? "",
              ]),
            )
          : null,
      };
    }

    return {
      eventName: `event_${index + 1}`,
      eventTimestamp: null,
      eventValsJson: null,
      eventStrValsJson: {
        raw: String(entry),
      },
    };
  });
}

function deriveOutcome(
  bidAmount: number | null,
  winningBid: number | null,
  isZeroBid: boolean,
  reasonForReject: string | null,
  httpStatusCode: number | null,
): InvestigationOutcome {
  if (isZeroBid) {
    return "zero_bid";
  }

  if (reasonForReject || (httpStatusCode !== null && httpStatusCode >= 400)) {
    return "rejected";
  }

  if ((winningBid ?? 0) > 0 || (bidAmount ?? 0) > 0) {
    return "accepted";
  }

  return "unknown";
}

export function normalizeRingbaBidDetail(
  fetchResult: RingbaFetchResult,
): NormalizedBidData {
  const body = isRecord(fetchResult.rawBody) ? fetchResult.rawBody : undefined;

  const bidId =
    toStringValue(pickFirstValue(body, ["bidId", "id", "bid.id"])) ??
    fetchResult.bidId;
  const bidAmount = toNumberValue(
    pickFirstValue(body, [
      "bidAmount",
      "amount",
      "bid.amount",
      "buyerResponse.bidAmount",
    ]),
  );
  const winningBid = toNumberValue(
    pickFirstValue(body, [
      "winningBid",
      "winningBidAmount",
      "acceptedBid",
      "winning.amount",
    ]),
  );
  const reasonForReject = toStringValue(
    pickFirstValue(body, [
      "reasonForReject",
      "rejectReason",
      "rejection.reason",
      "statusReason",
    ]),
  );
  const httpStatusCode =
    toNumberValue(
      pickFirstValue(body, ["httpStatusCode", "response.statusCode", "statusCode"]),
    ) ?? fetchResult.httpStatusCode;
  const errorMessage =
    toStringValue(
      pickFirstValue(body, [
        "errorMessage",
        "error.message",
        "message",
        "response.error",
      ]),
    ) ?? fetchResult.transportError;
  const requestBody =
    toObjectOrString(
      pickFirstValue(body, [
        "requestBody",
        "request.body",
        "requestPayload",
        "payload",
      ]),
    ) ?? null;
  const responseBody =
    toObjectOrString(
      pickFirstValue(body, ["responseBody", "response.body", "buyerResponse", "body"]),
    ) ??
    (typeof fetchResult.rawBody === "string" || isRecord(fetchResult.rawBody)
      ? fetchResult.rawBody
      : null);

  const isZeroBid =
    bidAmount === 0 ||
    winningBid === 0 ||
    String(reasonForReject ?? "").toLowerCase().includes("zero bid");

  const relevantEvents = extractEvents(body);

  return {
    bidId,
    bidDt: toStringValue(
      pickFirstValue(body, [
        "bidDt",
        "bidDateTime",
        "timestamp",
        "createdAt",
        "bid.timestamp",
      ]),
    ),
    campaignName: toStringValue(
      pickFirstValue(body, ["campaignName", "campaign.name"]),
    ),
    campaignId: toStringValue(
      pickFirstValue(body, ["campaignId", "campaign.id"]),
    ),
    publisherName: toStringValue(
      pickFirstValue(body, ["publisherName", "publisher.name"]),
    ),
    publisherId: toStringValue(
      pickFirstValue(body, ["publisherId", "publisher.id"]),
    ),
    targetName: toStringValue(
      pickFirstValue(body, ["targetName", "target.name"]),
    ),
    targetId: toStringValue(
      pickFirstValue(body, ["targetId", "target.id"]),
    ),
    buyerName: toStringValue(pickFirstValue(body, ["buyerName", "buyer.name"])),
    buyerId: toStringValue(pickFirstValue(body, ["buyerId", "buyer.id"])),
    bidAmount,
    winningBid,
    isZeroBid,
    reasonForReject,
    httpStatusCode,
    errorMessage,
    requestBody,
    responseBody,
    rawTraceJson: {
      requestUrl: fetchResult.requestUrl,
      fetchedAt: fetchResult.fetchedAt,
      httpStatusCode: fetchResult.httpStatusCode,
      responseHeaders: fetchResult.responseHeaders,
      transportError: fetchResult.transportError,
      payload: isRecord(fetchResult.rawBody)
        ? fetchResult.rawBody
        : {
            raw: fetchResult.rawBody,
          },
    },
    relevantEvents,
    outcome: deriveOutcome(
      bidAmount,
      winningBid,
      isZeroBid,
      reasonForReject,
      httpStatusCode,
    ),
  };
}
