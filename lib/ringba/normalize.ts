import type {
  BidEvent,
  BidTargetAttempt,
  FailureStage,
  InvestigationOutcome,
  NormalizedBidData,
} from "@/types/bid";
import type { RingbaFetchResult } from "@/lib/ringba/client";
import { isRecord, safeJsonParse, stringifyJson } from "@/lib/utils/json";

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

function readArrayPath(
  source: Record<string, unknown> | undefined,
  path: string,
): unknown[] | null {
  const value = readPath(source, path);
  return Array.isArray(value) ? value : null;
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

function pickFirstValueFromSources(
  sources: Array<Record<string, unknown> | undefined>,
  paths: string[],
): unknown {
  for (const source of sources) {
    const value = pickFirstValue(source, paths);
    if (value !== null) {
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

function toIsoDateTime(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value)) {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
  }

  if (typeof value === "string" && value.trim()) {
    const direct = new Date(value);
    if (!Number.isNaN(direct.getTime())) {
      return direct.toISOString();
    }

    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      const parsed = new Date(numeric);
      return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
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

function toStringArray(value: unknown) {
  if (!Array.isArray(value)) {
    return [];
  }

  const values: string[] = [];

  for (const entry of value) {
    if (typeof entry === "string" && entry.trim()) {
      values.push(entry.trim());
      continue;
    }

    if (isRecord(entry)) {
      const message =
        toStringValue(pickFirstValue(entry, ["message", "description", "error", "reason"])) ??
        stringifyJson(entry);
      if (message) {
        values.push(message);
      }
    }
  }

  return values;
}

function toNameValueMap(value: unknown) {
  const mapped: Record<string, unknown> = {};

  if (!Array.isArray(value)) {
    return mapped;
  }

  for (const entry of value) {
    if (!isRecord(entry)) {
      continue;
    }

    const key = toStringValue(entry.name);
    if (!key) {
      continue;
    }

    mapped[key] = entry.value ?? null;
  }

  return mapped;
}

function getPrimaryRecord(body: Record<string, unknown> | undefined) {
  if (!body) {
    return undefined;
  }

  const records = readArrayPath(body, "report.records");
  if (!records) {
    return body;
  }

  for (const entry of records) {
    if (isRecord(entry)) {
      return entry;
    }
  }

  return body;
}

function parseTraceValue(value: unknown) {
  if (isRecord(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = safeJsonParse(value);
    return isRecord(parsed) ? parsed : null;
  }

  return null;
}

function parseStructuredBody(value: unknown) {
  if (isRecord(value) || Array.isArray(value)) {
    return value;
  }

  if (typeof value !== "string" || !value.trim()) {
    return value ?? null;
  }

  return safeJsonParse(value);
}

function extractErrorCodeFromText(value: string | null) {
  if (!value) {
    return null;
  }

  const markers = ["(Code:", "Code:", "\"code\":", "'code':"];

  for (const marker of markers) {
    const markerIndex = value.indexOf(marker);
    if (markerIndex === -1) {
      continue;
    }

    let index = markerIndex + marker.length;
    while (index < value.length && value[index] === " ") {
      index += 1;
    }

    let digits = "";
    while (index < value.length) {
      const character = value[index];
      const code = character.charCodeAt(0);
      if (code < 48 || code > 57) {
        break;
      }
      digits += character;
      index += 1;
    }

    if (digits) {
      const parsed = Number(digits);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return null;
}

function parseTargetSummaryLine(value: string) {
  const line = value.trim();
  if (!line) {
    return null;
  }

  const bracketStart = line.indexOf("[");
  const bracketEnd = line.indexOf("]", bracketStart + 1);
  if (bracketStart === -1 || bracketEnd === -1) {
    return {
      targetName: line,
      bidAmount: null,
      minDurationSeconds: null,
      targetId: null,
      reason: null,
    };
  }

  const targetName = line.slice(0, bracketStart).trim();
  const bracketContents = line
    .slice(bracketStart + 1, bracketEnd)
    .split(",")
    .map((item) => item.trim());
  const trailing = line.slice(bracketEnd + 1).trim();

  return {
    targetName,
    bidAmount: toNumberValue(bracketContents[0] ?? null),
    minDurationSeconds: toNumberValue(bracketContents[1] ?? null),
    targetId: bracketContents[2] || null,
    reason: trailing || null,
  };
}

function parseSummaryList(value: string | null) {
  if (!value) {
    return [];
  }

  return value
    .split("\n")
    .map((line) => line.split("\r").join("").trim())
    .filter(Boolean)
    .map(parseTargetSummaryLine)
    .filter((entry): entry is NonNullable<ReturnType<typeof parseTargetSummaryLine>> => Boolean(entry));
}

function parseCallPlanTargets(value: string | null) {
  if (!value) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry, index) => {
      const colonIndex = entry.indexOf(":");
      const bracketStart = entry.indexOf("[");
      const bracketEnd = entry.indexOf("]", bracketStart + 1);
      const targetName = (
        colonIndex >= 0
          ? entry.slice(colonIndex + 1, bracketStart >= 0 ? bracketStart : undefined)
          : entry.slice(0, bracketStart >= 0 ? bracketStart : undefined)
      ).trim();
      const bracketContents =
        bracketStart >= 0 && bracketEnd >= 0
          ? entry
              .slice(bracketStart + 1, bracketEnd)
              .split(",")
              .map((part) => part.trim())
          : [];

      return {
        targetName,
        routePriority: toNumberValue(bracketContents[0] ?? null),
        routeWeight: toNumberValue(bracketContents[1] ?? null),
        sequence: index + 1,
      };
    });
}

function extractEvents(source: Record<string, unknown> | undefined): BidEvent[] {
  if (!source) {
    return [];
  }

  const rawEvents = readArrayPath(source, "events") ?? readArrayPath(source, "trace.events");

  if (!rawEvents) {
    return [];
  }

  return rawEvents.map((entry, index) => {
    if (!isRecord(entry)) {
      return {
        eventName: `event_${index + 1}`,
        eventTimestamp: null,
        eventValsJson: null,
        eventStrValsJson: {
          raw: String(entry),
        },
      };
    }

    return {
      eventName:
        toStringValue(pickFirstValue(entry, ["name", "eventName", "type", "event"])) ??
        `event_${index + 1}`,
      eventTimestamp: toIsoDateTime(
        pickFirstValue(entry, ["dtStamp", "eventTimestamp", "timestamp", "dt", "createdAt"]),
      ),
      eventValsJson: (() => {
        const values = toNameValueMap(entry.eventVals);
        return Object.keys(values).length > 0 ? values : null;
      })(),
      eventStrValsJson: (() => {
        const values = toNameValueMap(entry.eventStrVals);
        const mapped = Object.fromEntries(
          Object.entries(values).map(([key, value]) => [key, toStringValue(value) ?? ""]),
        );
        return Object.keys(mapped).length > 0 ? mapped : null;
      })(),
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

function extractAttemptErrorDetails(input: {
  responseBody: Record<string, unknown> | string | null;
  rejectReason: string | null;
  errorMessage: string | null;
}) {
  const responseRecord = isRecord(input.responseBody) ? input.responseBody : undefined;
  const errors = toStringArray(pickFirstValue(responseRecord, ["errors", "error.errors"]));
  const nestedErrorCode = toNumberValue(
    pickFirstValue(responseRecord, ["error.code", "code", "statusCode"]),
  );
  const nestedErrorMessage = toStringValue(
    pickFirstValue(responseRecord, ["error.message", "message", "status"]),
  );
  const errorCode =
    nestedErrorCode ??
    extractErrorCodeFromText(input.rejectReason) ??
    extractErrorCodeFromText(input.errorMessage) ??
    errors.map((value) => extractErrorCodeFromText(value)).find((value) => value !== null) ??
    null;
  const errorMessage =
    input.errorMessage ??
    nestedErrorMessage ??
    input.rejectReason ??
    errors[0] ??
    null;

  return {
    errorCode,
    errorMessage,
    errors,
  };
}

function parseTargetAttempts(
  record: Record<string, unknown> | undefined,
): BidTargetAttempt[] {
  if (!record) {
    return [];
  }

  const rawEvents = readArrayPath(record, "events");
  if (!rawEvents) {
    return [];
  }

  const attempts: BidTargetAttempt[] = [];
  const acceptedTargets = new Map<string, { bidAmount: number | null; minDurationSeconds: number | null }>();
  const winningTargets = new Set<string>();
  const rejectedTargets = new Map<string, { reason: string | null; bidAmount: number | null; minDurationSeconds: number | null }>();
  const callPlanTargets = new Map<string, { routePriority: number | null; routeWeight: number | null; sequence: number }>();

  for (const rawEvent of rawEvents) {
    if (!isRecord(rawEvent)) {
      continue;
    }

    const eventName = toStringValue(rawEvent.name);
    const eventStrVals = Object.fromEntries(
      Object.entries(toNameValueMap(rawEvent.eventStrVals)).map(([key, value]) => [
        key,
        toStringValue(value) ?? "",
      ]),
    );

    if (eventName === "PingTreePingingSummary") {
      const acceptedEntries = parseSummaryList(eventStrVals.acceptedRingTreeTargets ?? null);
      const winningEntry = parseTargetSummaryLine(eventStrVals.winningRingTreeTarget ?? "");
      const rejectedEntries = parseSummaryList(eventStrVals.notAcceptedRingTreeTargets ?? null);

      for (const entry of acceptedEntries) {
        acceptedTargets.set(entry.targetName, {
          bidAmount: entry.bidAmount,
          minDurationSeconds: entry.minDurationSeconds,
        });
      }

      if (winningEntry) {
        winningTargets.add(winningEntry.targetName);
      }

      for (const entry of rejectedEntries) {
        rejectedTargets.set(entry.targetName, {
          reason: entry.reason,
          bidAmount: entry.bidAmount,
          minDurationSeconds: entry.minDurationSeconds,
        });
      }
    }

    if (eventName === "CallPlanDetail") {
      const callPlanEntries = parseCallPlanTargets(eventStrVals.eligibleOrderedTargets ?? null);
      for (const entry of callPlanEntries) {
        callPlanTargets.set(entry.targetName, entry);
      }
    }
  }

  for (const rawEvent of rawEvents) {
    if (!isRecord(rawEvent)) {
      continue;
    }

    const eventName = toStringValue(rawEvent.name);
    if (eventName !== "PingRAWResult") {
      continue;
    }

    const eventVals = toNameValueMap(rawEvent.eventVals);
    const eventStrVals = Object.fromEntries(
      Object.entries(toNameValueMap(rawEvent.eventStrVals)).map(([key, value]) => [
        key,
        toStringValue(value) ?? "",
      ]),
    );
    const responseBody = toObjectOrString(parseStructuredBody(eventStrVals.responseBody ?? null));
    const requestBody = toObjectOrString(parseStructuredBody(eventStrVals.requestBody ?? null));
    const rejectReason =
      toStringValue(pickFirstValue(isRecord(responseBody) ? responseBody : undefined, ["rejectReason"])) ??
      null;
    const errorMessage = eventStrVals.errorMessage || null;
    const acceptedSummary = acceptedTargets.get(toStringValue(rawEvent.targetName) ?? "");
    const rejectedSummary = rejectedTargets.get(toStringValue(rawEvent.targetName) ?? "");
    const routeSummary = callPlanTargets.get(toStringValue(rawEvent.targetName) ?? "");
    const bidAmount =
      toNumberValue(
        pickFirstValue(isRecord(responseBody) ? responseBody : undefined, [
          "bidAmount",
          "acceptedBid",
          "winningBid",
        ]),
      ) ??
      acceptedSummary?.bidAmount ??
      rejectedSummary?.bidAmount ??
      null;
    const minDurationSeconds =
      toNumberValue(
        pickFirstValue(isRecord(responseBody) ? responseBody : undefined, [
          "bidTerms.0.callMinDuration",
          "callMinDuration",
        ]),
      ) ??
      acceptedSummary?.minDurationSeconds ??
      rejectedSummary?.minDurationSeconds ??
      null;
    const parsedErrors = extractAttemptErrorDetails({
      responseBody,
      rejectReason,
      errorMessage,
    });
    const targetName = toStringValue(rawEvent.targetName);

    attempts.push({
      sequence: attempts.length + 1,
      eventName,
      eventTimestamp: toIsoDateTime(rawEvent.dtStamp),
      targetName,
      targetId: toStringValue(rawEvent.targetId),
      targetBuyer: toStringValue(rawEvent.targetBuyer),
      targetBuyerId: toStringValue(rawEvent.targetBuyerId),
      targetNumber: toStringValue(rawEvent.targetNumber),
      targetGroupName: toStringValue(rawEvent.targetGroupName),
      targetGroupId: toStringValue(rawEvent.targetGroupId),
      targetSubId: toStringValue(rawEvent.targetSubId),
      targetBuyerSubId: toStringValue(rawEvent.targetBuyerSubId),
      requestUrl: eventStrVals.url || null,
      httpMethod: eventStrVals.method || null,
      requestStatus: eventStrVals.requestStatus || null,
      httpStatusCode: toNumberValue(eventVals.httpStatusCode),
      durationMs: toNumberValue(eventVals.duration),
      routePriority: routeSummary?.routePriority ?? null,
      routeWeight: routeSummary?.routeWeight ?? null,
      accepted: acceptedSummary ? true : rejectedSummary ? false : null,
      winning: targetName ? winningTargets.has(targetName) : false,
      bidAmount,
      minDurationSeconds,
      rejectReason,
      errorCode: parsedErrors.errorCode,
      errorMessage: parsedErrors.errorMessage,
      errors: parsedErrors.errors,
      requestBody,
      responseBody,
      summaryReason: rejectedSummary?.reason ?? null,
      rawEventJson: rawEvent,
    });
  }

  return attempts;
}

function pickPrimaryAttempt(
  targetAttempts: BidTargetAttempt[],
  outcome: InvestigationOutcome,
) {
  if (targetAttempts.length === 0) {
    return null;
  }

  if (outcome === "accepted") {
    return (
      targetAttempts.find((attempt) => attempt.winning) ??
      targetAttempts.find((attempt) => attempt.accepted) ??
      targetAttempts[0]
    );
  }

  let bestAttempt = targetAttempts[0];
  let bestScore = -1;

  for (const attempt of targetAttempts) {
    let score = 0;
    if (attempt.accepted === false) {
      score += 8;
    }
    if (attempt.errorCode !== null) {
      score += 8;
    }
    if (attempt.errorMessage) {
      score += 6;
    }
    if (attempt.errors.length > 0) {
      score += 5;
    }
    if (attempt.rejectReason) {
      score += 5;
    }
    if (attempt.summaryReason) {
      score += 4;
    }
    if (attempt.bidAmount === 0) {
      score += 3;
    }
    if (attempt.httpStatusCode !== null && attempt.httpStatusCode >= 400) {
      score += 3;
    }
    if (attempt.requestStatus && attempt.requestStatus.toLowerCase() !== "success") {
      score += 2;
    }
    if (score > bestScore) {
      bestScore = score;
      bestAttempt = attempt;
    }
  }

  return bestAttempt;
}

function deriveFailureStage(
  outcome: InvestigationOutcome,
  primaryAttempt: BidTargetAttempt | null,
): FailureStage {
  if (outcome === "accepted") {
    return "accepted";
  }

  if (outcome === "zero_bid") {
    return "zero_bid";
  }

  if (primaryAttempt) {
    return "target_rejected";
  }

  if (outcome === "rejected") {
    return "routing";
  }

  return "unknown";
}

export function normalizeRingbaBidDetail(
  fetchResult: RingbaFetchResult,
): NormalizedBidData {
  const body = isRecord(fetchResult.rawBody) ? fetchResult.rawBody : undefined;
  const record = getPrimaryRecord(body);
  const sources = [record, body];
  const targetAttempts = parseTargetAttempts(record);

  const bidId =
    toStringValue(pickFirstValueFromSources(sources, ["bidId", "id", "bid.id"])) ??
    fetchResult.bidId;
  const bidAmount = toNumberValue(
    pickFirstValueFromSources(sources, [
      "bidAmount",
      "amount",
      "bid.amount",
      "buyerResponse.bidAmount",
    ]),
  );
  const winningBid = toNumberValue(
    pickFirstValueFromSources(sources, [
      "winningBid",
      "winningBidAmount",
      "acceptedBid",
      "winning.amount",
    ]),
  );
  const reasonForReject = toStringValue(
    pickFirstValueFromSources(sources, [
      "reasonForReject",
      "rejectReason",
      "rejection.reason",
      "statusReason",
    ]),
  );
  const httpStatusCode =
    toNumberValue(
      pickFirstValueFromSources(sources, ["httpStatusCode", "response.statusCode", "statusCode"]),
    ) ?? fetchResult.httpStatusCode;
  const errorMessage =
    toStringValue(
      pickFirstValueFromSources(sources, [
        "errorMessage",
        "error.message",
        "message",
        "response.error",
      ]),
    ) ?? fetchResult.transportError;
  const requestBody =
    toObjectOrString(
      pickFirstValueFromSources(sources, [
        "requestBody",
        "request.body",
        "requestPayload",
        "payload",
      ]),
    ) ?? null;
  const responseBody =
    toObjectOrString(
      pickFirstValueFromSources(sources, ["responseBody", "response.body", "buyerResponse", "body"]),
    ) ??
    (record
      ? record
      : typeof fetchResult.rawBody === "string" || isRecord(fetchResult.rawBody)
        ? fetchResult.rawBody
        : null);
  const bidElapsedMs = toNumberValue(
    pickFirstValueFromSources(sources, ["bidElapsedMs", "elapsedMs", "trace.bidElapsedMs"]),
  );
  const primaryAttemptResponse = pickPrimaryAttempt(
    targetAttempts,
    deriveOutcome(
      bidAmount,
      winningBid,
      bidAmount === 0 || winningBid === 0 || String(reasonForReject ?? "").toLowerCase().includes("zero bid"),
      reasonForReject,
      httpStatusCode,
    ),
  );
  const normalizedRequestBody = primaryAttemptResponse?.requestBody ?? requestBody;
  const normalizedResponseBody = primaryAttemptResponse?.responseBody ?? responseBody;
  const relevantEvents = extractEvents(record ?? body);

  const isZeroBid =
    bidAmount === 0 ||
    winningBid === 0 ||
    String(reasonForReject ?? "").toLowerCase().includes("zero bid") ||
    relevantEvents.some((event) => event.eventName === "ZeroRTBBid") ||
    (targetAttempts.length > 0 &&
      targetAttempts.every((attempt) => {
        const hasExplicitBid = attempt.bidAmount !== null;
        return attempt.accepted !== true && (!hasExplicitBid || attempt.bidAmount === 0);
      }));

  const outcome = deriveOutcome(
    bidAmount,
    winningBid,
    isZeroBid,
    reasonForReject,
    httpStatusCode,
  );
  const primaryAttempt = pickPrimaryAttempt(targetAttempts, outcome);
  const traceJson = parseTraceValue(readPath(record, "trace"));

  return {
    bidId,
    bidDt:
      toIsoDateTime(
        pickFirstValueFromSources(sources, [
          "bidDt",
          "bidDateTime",
          "timestamp",
          "createdAt",
          "bid.timestamp",
        ]),
      ),
    campaignName: toStringValue(
      pickFirstValueFromSources(sources, ["campaignName", "campaign.name"]),
    ),
    campaignId: toStringValue(
      pickFirstValueFromSources(sources, ["campaignId", "campaign.id"]),
    ),
    publisherName: toStringValue(
      pickFirstValueFromSources(sources, ["publisherName", "publisher.name"]),
    ),
    publisherId: toStringValue(
      pickFirstValueFromSources(sources, ["publisherId", "publisher.id"]),
    ),
    targetName:
      primaryAttempt?.targetName ??
      toStringValue(pickFirstValueFromSources(sources, ["targetName", "target.name"])),
    targetId:
      primaryAttempt?.targetId ??
      toStringValue(pickFirstValueFromSources(sources, ["targetId", "target.id"])),
    buyerName:
      primaryAttempt?.targetBuyer ??
      toStringValue(pickFirstValueFromSources(sources, ["buyerName", "buyer.name"])),
    buyerId:
      primaryAttempt?.targetBuyerId ??
      toStringValue(pickFirstValueFromSources(sources, ["buyerId", "buyer.id"])),
    bidAmount,
    winningBid,
    bidElapsedMs,
    isZeroBid,
    reasonForReject,
    httpStatusCode,
    errorMessage: primaryAttempt?.errorMessage ?? errorMessage,
    primaryFailureStage: deriveFailureStage(outcome, primaryAttempt),
    primaryTargetName: primaryAttempt?.targetName ?? null,
    primaryTargetId: primaryAttempt?.targetId ?? null,
    primaryBuyerName: primaryAttempt?.targetBuyer ?? null,
    primaryBuyerId: primaryAttempt?.targetBuyerId ?? null,
    primaryErrorCode: primaryAttempt?.errorCode ?? extractErrorCodeFromText(errorMessage),
    primaryErrorMessage: primaryAttempt?.errorMessage ?? errorMessage,
    requestBody: normalizedRequestBody,
    responseBody: normalizedResponseBody,
    rawTraceJson: {
      requestUrl: fetchResult.requestUrl,
      fetchedAt: fetchResult.fetchedAt,
      httpStatusCode: fetchResult.httpStatusCode,
      responseHeaders: fetchResult.responseHeaders,
      transportError: fetchResult.transportError,
      trace: traceJson,
      payload: isRecord(fetchResult.rawBody)
        ? fetchResult.rawBody
        : {
            raw: fetchResult.rawBody,
          },
    },
    relevantEvents,
    targetAttempts,
    outcome,
  };
}
