import type {
  BidEvent,
  BidTargetAttempt,
  ClassificationSource,
  ClassificationWarning,
  FailureStage,
  InvestigationOutcome,
  NormalizationParseStatus,
  NormalizationWarning,
  NormalizedBidData,
  OutcomeReasonCategory,
} from "@/types/bid";
import type { RingbaFetchResult } from "@/lib/ringba/client";
import { isRecord, safeJsonParse, stringifyJson } from "@/lib/utils/json";

const NORMALIZATION_VERSION = "ringba-normalizer-v2";
const KNOWN_EVENT_NAMES = new Set([
  "PingRAWResult",
  "PingTreePingingSummary",
  "CallPlanDetail",
  "ZeroRTBBid",
]);

interface SourceDescriptor {
  label: string;
  value: Record<string, unknown> | undefined;
}

interface ResolvedValue {
  value: unknown;
  path: string;
  sourceLabel: string;
}

interface ErrorCodeCandidate {
  code: number;
  source: string;
  confidence: number;
  rawMatch: string | null;
}

interface ErrorDetails {
  errorCode: number | null;
  errorMessage: string | null;
  errors: string[];
  errorCodeSource: string | null;
  errorCodeConfidence: number | null;
  errorCodeRawMatch: string | null;
  usedTextFallback: boolean;
}

interface ParsedTargetAttempt extends BidTargetAttempt {
  errorCodeSource: string | null;
  errorCodeConfidence: number | null;
  errorCodeRawMatch: string | null;
  usedTextFallback: boolean;
}

interface ParsedRingbaBidDetail {
  body: Record<string, unknown> | undefined;
  record: Record<string, unknown> | undefined;
  sources: SourceDescriptor[];
  relevantEvents: BidEvent[];
  targetAttempts: ParsedTargetAttempt[];
  traceJson: Record<string, unknown> | null;
  schemaVariant: string | null;
  warnings: NormalizationWarning[];
  unknownEventNames: string[];
}

interface BidDisposition {
  outcome: InvestigationOutcome;
  outcomeReasonCategory: OutcomeReasonCategory | null;
  outcomeReasonCode: string | null;
  outcomeReasonMessage: string | null;
  classificationSource: ClassificationSource | null;
  classificationConfidence: number | null;
  classificationWarnings: ClassificationWarning[];
}

function readPath(
  source: Record<string, unknown> | unknown[] | undefined,
  path: string,
): unknown {
  if (!source) {
    return undefined;
  }

  const parts = path.split(".");
  let current: unknown = source;

  for (const part of parts) {
    if (Array.isArray(current)) {
      const index = Number(part);
      if (!Number.isInteger(index) || index < 0 || index >= current.length) {
        return undefined;
      }

      current = current[index];
      continue;
    }

    if (isRecord(current)) {
      current = current[part];
      continue;
    }

    return undefined;
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

function addWarning(
  warnings: NormalizationWarning[],
  warning: NormalizationWarning,
) {
  const exists = warnings.some(
    (entry) =>
      entry.code === warning.code &&
      entry.field === warning.field &&
      entry.message === warning.message,
  );

  if (!exists) {
    warnings.push(warning);
  }
}

function isJsonLikeString(value: string) {
  const trimmed = value.trim();
  return trimmed.startsWith("{") || trimmed.startsWith("[");
}

function parseJsonString(value: string): { ok: true; parsed: unknown } | { ok: false } {
  try {
    return {
      ok: true,
      parsed: JSON.parse(value),
    };
  } catch {
    return {
      ok: false,
    };
  }
}

function parseStructuredValue(
  value: unknown,
  field: string,
  warnings: NormalizationWarning[],
) {
  if (isRecord(value) || Array.isArray(value)) {
    return value;
  }

  if (typeof value !== "string" || !value.trim()) {
    return value ?? null;
  }

  const trimmed = value.trim();
  if (!isJsonLikeString(trimmed)) {
    return value;
  }

  const parsed = parseJsonString(trimmed);
  if (!parsed.ok) {
    addWarning(warnings, {
      code: "json_parse_failed",
      message: `Failed to parse JSON for ${field}.`,
      field,
    });
    return value;
  }

  return parsed.parsed;
}

function buildSourcePath(sourceLabel: string, path: string) {
  return sourceLabel ? `${sourceLabel}.${path}` : path;
}

function pushPathUsage(
  rawPathsUsed: Record<string, string[]>,
  field: string,
  sourcePath: string,
) {
  if (!sourcePath) {
    return;
  }

  const existing = rawPathsUsed[field] ?? [];
  if (!existing.includes(sourcePath)) {
    rawPathsUsed[field] = [...existing, sourcePath];
  }
}

function resolveFirstValue(
  source: Record<string, unknown> | undefined,
  sourceLabel: string,
  paths: string[],
): ResolvedValue | null {
  for (const path of paths) {
    const value = readPath(source, path);
    if (value !== undefined && value !== null && value !== "") {
      return {
        value,
        path,
        sourceLabel,
      };
    }
  }

  return null;
}

function resolveFirstValueFromSources(
  sources: SourceDescriptor[],
  paths: string[],
): ResolvedValue | null {
  for (const source of sources) {
    const resolved = resolveFirstValue(source.value, source.label, paths);
    if (resolved) {
      return resolved;
    }
  }

  return null;
}

function getValueFromSources(
  sources: SourceDescriptor[],
  rawPathsUsed: Record<string, string[]>,
  field: string,
  paths: string[],
) {
  const resolved = resolveFirstValueFromSources(sources, paths);
  if (!resolved) {
    return null;
  }

  pushPathUsage(rawPathsUsed, field, buildSourcePath(resolved.sourceLabel, resolved.path));
  return resolved.value;
}

function cleanIdentifier(value: string) {
  return value
    .toLowerCase()
    .split("_")
    .join("")
    .split("-")
    .join("")
    .split(" ")
    .join("");
}

function isErrorCodeKey(key: string) {
  const cleaned = cleanIdentifier(key);
  return cleaned === "code" || cleaned === "errorcode";
}

function isErrorStatusCodeKey(key: string) {
  return cleanIdentifier(key) === "statuscode";
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

function toBooleanValue(value: unknown) {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true" || normalized === "yes" || normalized === "1") {
      return true;
    }
    if (normalized === "false" || normalized === "no" || normalized === "0") {
      return false;
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

function toLowerString(value: string | null) {
  return value ? value.toLowerCase() : "";
}

function collectTextFragments(value: unknown, fragments: string[], depth = 0) {
  if (depth > 4 || value === null || value === undefined) {
    return;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed) {
      fragments.push(trimmed);
    }
    return;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    fragments.push(String(value));
    return;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      collectTextFragments(entry, fragments, depth + 1);
    }
    return;
  }

  if (isRecord(value)) {
    for (const entry of Object.values(value)) {
      collectTextFragments(entry, fragments, depth + 1);
    }
  }
}

function joinStructuredText(value: unknown) {
  const fragments: string[] = [];
  collectTextFragments(value, fragments);
  return fragments.join(" | ");
}

function getStructuredStatusText(responseRecord: Record<string, unknown> | undefined) {
  return toStringValue(
    pickFirstValue(responseRecord, [
      "status",
      "error",
      "error.status",
      "error.message",
      "message",
      "description",
      "reason",
    ]),
  );
}

function extractReasonCodeFromText(value: string | null) {
  if (!value) {
    return null;
  }

  const candidate = findCodeInText(value, "reason_for_reject_text");
  return candidate ? String(candidate.code) : null;
}

function buildDisposition(input: {
  outcomeReasonCategory: OutcomeReasonCategory;
  outcomeReasonCode?: string | null;
  outcomeReasonMessage?: string | null;
  classificationSource: ClassificationSource;
  classificationConfidence: number;
  classificationWarnings?: ClassificationWarning[];
}) {
  return {
    outcome: deriveLegacyOutcome(input.outcomeReasonCategory),
    outcomeReasonCategory: input.outcomeReasonCategory,
    outcomeReasonCode: input.outcomeReasonCode ?? null,
    outcomeReasonMessage: input.outcomeReasonMessage ?? null,
    classificationSource: input.classificationSource,
    classificationConfidence: roundConfidence(input.classificationConfidence),
    classificationWarnings: input.classificationWarnings ?? [],
  } satisfies BidDisposition;
}

function deriveLegacyOutcome(
  outcomeReasonCategory: OutcomeReasonCategory | null,
): InvestigationOutcome {
  if (outcomeReasonCategory === "accepted") {
    return "accepted";
  }

  if (
    outcomeReasonCategory === "missing_required_field" ||
    outcomeReasonCategory === "missing_caller_id" ||
    outcomeReasonCategory === "request_invalid" ||
    outcomeReasonCategory === "rate_limited"
  ) {
    return "rejected";
  }

  if (
    outcomeReasonCategory === "buyer_returned_zero_bid" ||
    outcomeReasonCategory === "below_minimum_revenue" ||
    outcomeReasonCategory === "tag_filtered_initial" ||
    outcomeReasonCategory === "tag_filtered_final" ||
    outcomeReasonCategory === "no_matching_buyer" ||
    outcomeReasonCategory === "no_capacity" ||
    outcomeReasonCategory === "unknown_no_payable_bid"
  ) {
    return "zero_bid";
  }

  return "unknown";
}

function buildClassificationConflictWarning(
  structuredMessage: string | null,
  reasonForReject: string | null,
): ClassificationWarning | null {
  if (!structuredMessage || !reasonForReject) {
    return null;
  }

  const structuredLower = toLowerString(structuredMessage);
  const rejectLower = toLowerString(reasonForReject);
  if (!structuredLower || !rejectLower || structuredLower === rejectLower) {
    return null;
  }

  if (structuredLower.includes(rejectLower) || rejectLower.includes(structuredLower)) {
    return null;
  }

  return {
    code: "classification_conflict",
    message:
      "Structured response details disagreed with the top-level reject reason, so the structured response was preferred.",
    field: "reasonForReject",
  };
}

function classifyMinimumRevenueDisposition(primaryAttempt: ParsedTargetAttempt | null) {
  if (!primaryAttempt?.summaryReason) {
    return null;
  }

  const summaryReason = primaryAttempt.summaryReason;
  const summaryReasonLower = toLowerString(summaryReason);
  if (!summaryReasonLower.includes("minimum revenue")) {
    return null;
  }

  if ((primaryAttempt.bidAmount ?? 0) <= 0) {
    return null;
  }

  if (primaryAttempt.winning === true || primaryAttempt.accepted === true) {
    return null;
  }

  return buildDisposition({
    outcomeReasonCategory: "below_minimum_revenue",
    outcomeReasonCode: "minimum_revenue",
    outcomeReasonMessage: summaryReason,
    classificationSource: "primary_attempt_structured",
    classificationConfidence: 0.97,
  });
}

function classifyStructuredResponse(input: {
  responseRecord: Record<string, unknown> | undefined;
  reasonForReject: string | null;
  defaultSource: ClassificationSource;
  defaultConfidence: number;
  fallbackCode: string | null;
  fallbackMessage: string | null;
}): BidDisposition | null {
  const responseRecord = input.responseRecord;
  if (!responseRecord) {
    return null;
  }

  const statusText = getStructuredStatusText(responseRecord);
  const responseText = joinStructuredText(responseRecord);
  const responseTextLower = toLowerString(responseText);
  const structuredMessage =
    resolveStructuredErrorMessage(responseRecord) ??
    toStringValue(pickFirstValue(responseRecord, ["rejectReason", "status", "message"])) ??
    input.fallbackMessage;
  const structuredCodeCandidate = collectStructuredErrorCandidates(
    responseRecord,
    input.defaultSource,
  )[0];
  const structuredCode = structuredCodeCandidate
    ? String(structuredCodeCandidate.code)
    : input.fallbackCode;
  const warnings: ClassificationWarning[] = [];
  const conflictWarning = buildClassificationConflictWarning(
    structuredMessage,
    input.reasonForReject,
  );
  if (conflictWarning) {
    warnings.push(conflictWarning);
  }

  if (
    statusText === "caller_id_required" ||
    responseTextLower.includes("caller_id_required") ||
    (responseTextLower.includes("caller_id") && responseTextLower.includes("requires it"))
  ) {
    return buildDisposition({
      outcomeReasonCategory: "missing_caller_id",
      outcomeReasonCode: structuredCode ?? statusText ?? "caller_id_required",
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (statusText === "no_matching_buyer" || responseTextLower.includes("no matching buyers")) {
    return buildDisposition({
      outcomeReasonCategory: "no_matching_buyer",
      outcomeReasonCode: structuredCode ?? statusText,
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (
    responseTextLower.includes("rate limit") ||
    responseTextLower.includes("too many requests") ||
    structuredCode === "1024" ||
    structuredCode === "1025" ||
    structuredCode === "1026"
  ) {
    return buildDisposition({
      outcomeReasonCategory: "rate_limited",
      outcomeReasonCode: structuredCode,
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (
    structuredCode === "3024" ||
    responseTextLower.includes("is required on ping") ||
    responseTextLower.includes("required tag") ||
    (responseTextLower.includes("required") && responseTextLower.includes("zip"))
  ) {
    return buildDisposition({
      outcomeReasonCategory: "missing_required_field",
      outcomeReasonCode: structuredCode ?? statusText,
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (
    (statusText === "422" || responseTextLower.includes("unprocessable")) &&
    (responseTextLower.includes("required") || responseTextLower.includes("invalid"))
  ) {
    return buildDisposition({
      outcomeReasonCategory: "request_invalid",
      outcomeReasonCode: structuredCode ?? statusText,
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (structuredCode === "1002" || responseTextLower.includes("initial tag filter")) {
    return buildDisposition({
      outcomeReasonCategory: "tag_filtered_initial",
      outcomeReasonCode: structuredCode ?? "1002",
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (
    structuredCode === "1005" ||
    structuredCode === "1006" ||
    responseTextLower.includes("final capacity check") ||
    responseTextLower.includes("final tag filter")
  ) {
    return buildDisposition({
      outcomeReasonCategory: "tag_filtered_final",
      outcomeReasonCode: structuredCode ?? "1006",
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (structuredCode === "1003" || responseTextLower.includes("no capacity")) {
    return buildDisposition({
      outcomeReasonCategory: "no_capacity",
      outcomeReasonCode: structuredCode ?? "1003",
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: input.defaultConfidence,
      classificationWarnings: warnings,
    });
  }

  if (
    structuredCode !== null ||
    statusText !== null ||
    responseTextLower.includes("success\":false") ||
    responseTextLower.includes("rejected request")
  ) {
    return buildDisposition({
      outcomeReasonCategory: "request_invalid",
      outcomeReasonCode: structuredCode ?? statusText,
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: Math.min(input.defaultConfidence, 0.82),
      classificationWarnings: warnings,
    });
  }

  const responseBidAmount = toNumberValue(
    pickFirstValue(responseRecord, ["bidAmount", "acceptedBid", "winningBid"]),
  );
  if (
    responseBidAmount === 0 ||
    responseTextLower.includes("zero bid") ||
    responseTextLower.includes("no bid")
  ) {
    return buildDisposition({
      outcomeReasonCategory: "buyer_returned_zero_bid",
      outcomeReasonCode: structuredCode,
      outcomeReasonMessage: structuredMessage,
      classificationSource: input.defaultSource,
      classificationConfidence: Math.min(input.defaultConfidence, 0.84),
      classificationWarnings: warnings,
    });
  }

  return null;
}

function classifyFromPrimaryErrorCode(input: {
  code: number | null;
  message: string | null;
  source: string | null;
}) {
  if (input.code === null) {
    return null;
  }

  const code = String(input.code);
  if (code === "1002") {
    return buildDisposition({
      outcomeReasonCategory: "tag_filtered_initial",
      outcomeReasonCode: code,
      outcomeReasonMessage: input.message,
      classificationSource: "primary_attempt_error_code",
      classificationConfidence: 0.9,
    });
  }

  if (code === "1005" || code === "1006") {
    return buildDisposition({
      outcomeReasonCategory: "tag_filtered_final",
      outcomeReasonCode: code,
      outcomeReasonMessage: input.message,
      classificationSource: input.source === "reasonForReject_text"
        ? "reason_for_reject_text"
        : "primary_attempt_error_code",
      classificationConfidence: input.source === "reasonForReject_text" ? 0.62 : 0.9,
    });
  }

  if (code === "1003") {
    return buildDisposition({
      outcomeReasonCategory: "no_capacity",
      outcomeReasonCode: code,
      outcomeReasonMessage: input.message,
      classificationSource: "primary_attempt_error_code",
      classificationConfidence: 0.9,
    });
  }

  if (code === "1024" || code === "1025" || code === "1026") {
    return buildDisposition({
      outcomeReasonCategory: "rate_limited",
      outcomeReasonCode: code,
      outcomeReasonMessage: input.message,
      classificationSource: "primary_attempt_error_code",
      classificationConfidence: 0.9,
    });
  }

  return null;
}

function classifyFromReasonForReject(reasonForReject: string | null) {
  const lowerReason = toLowerString(reasonForReject);
  if (!lowerReason) {
    return null;
  }

  const reasonCode = extractReasonCodeFromText(reasonForReject);
  if (lowerReason.includes("initial tag filter") || reasonCode === "1002") {
    return buildDisposition({
      outcomeReasonCategory: "tag_filtered_initial",
      outcomeReasonCode: reasonCode ?? "1002",
      outcomeReasonMessage: reasonForReject,
      classificationSource: "reason_for_reject_text",
      classificationConfidence: 0.62,
    });
  }

  if (
    lowerReason.includes("final capacity check") ||
    lowerReason.includes("final tag filter") ||
    reasonCode === "1005" ||
    reasonCode === "1006"
  ) {
    return buildDisposition({
      outcomeReasonCategory: "tag_filtered_final",
      outcomeReasonCode: reasonCode ?? "1006",
      outcomeReasonMessage: reasonForReject,
      classificationSource: "reason_for_reject_text",
      classificationConfidence: 0.6,
    });
  }

  if (lowerReason.includes("zero bid") || lowerReason.includes("no bid")) {
    return buildDisposition({
      outcomeReasonCategory: "buyer_returned_zero_bid",
      outcomeReasonCode: reasonCode,
      outcomeReasonMessage: reasonForReject,
      classificationSource: "reason_for_reject_text",
      classificationConfidence: 0.56,
    });
  }

  return null;
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

function parseTraceValue(
  value: unknown,
  field: string,
  warnings: NormalizationWarning[],
) {
  if (isRecord(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = safeJsonParse(value);
    return isRecord(parsed) ? parsed : null;
  }

  if (value !== null && value !== undefined) {
    addWarning(warnings, {
      code: "trace_parse_failed",
      message: `Failed to parse trace JSON for ${field}.`,
      field,
    });
  }

  return null;
}

function collectStructuredErrorCandidates(
  value: Record<string, unknown> | undefined,
  baseSource: string,
): ErrorCodeCandidate[] {
  if (!value) {
    return [];
  }

  const candidates: ErrorCodeCandidate[] = [];
  const directPaths = [
    { path: "error.code", confidence: 1 },
    { path: "code", confidence: 0.98 },
    { path: "errorCode", confidence: 0.98 },
    { path: "error.errorCode", confidence: 0.98 },
    { path: "error.statusCode", confidence: 0.9 },
  ];

  for (const directPath of directPaths) {
    const resolved = resolveFirstValue(value, baseSource, [directPath.path]);
    const parsed = toNumberValue(resolved?.value ?? null);
    if (resolved && parsed !== null) {
      candidates.push({
        code: parsed,
        source: buildSourcePath(resolved.sourceLabel, resolved.path),
        confidence: directPath.confidence,
        rawMatch: null,
      });
    }
  }

  const errorArrays = ["errors", "error.errors"];

  for (const errorArray of errorArrays) {
    const entries = readArrayPath(value, errorArray);
    if (!entries) {
      continue;
    }

    for (const [index, entry] of entries.entries()) {
      if (!isRecord(entry)) {
        continue;
      }

      for (const [key, nestedValue] of Object.entries(entry)) {
        const parsed = toNumberValue(nestedValue);
        if (parsed === null) {
          continue;
        }

        if (isErrorCodeKey(key) || isErrorStatusCodeKey(key)) {
          candidates.push({
            code: parsed,
            source: `${baseSource}.${errorArray}[${index}].${key}`,
            confidence: 0.94,
            rawMatch: null,
          });
        }
      }
    }
  }

  return candidates;
}

function findCodeInText(value: string | null, source: string): ErrorCodeCandidate | null {
  if (!value) {
    return null;
  }

  const lowerValue = value.toLowerCase();
  const markers = [
    "(code:",
    "code:",
    "code=",
    "\"code\":",
    "'code':",
    "error_code:",
    "error code",
    "status code",
    "status_code:",
  ];
  const skippable = new Set([" ", "\t", "\n", "\r", ":", "=", "\"", "'", "[", "]", "(", ")"]);

  for (const marker of markers) {
    let searchIndex = 0;

    while (searchIndex < lowerValue.length) {
      const markerIndex = lowerValue.indexOf(marker, searchIndex);
      if (markerIndex === -1) {
        break;
      }

      let index = markerIndex + marker.length;
      while (index < value.length && skippable.has(value[index] ?? "")) {
        index += 1;
      }

      let digits = "";
      while (index < value.length) {
        const character = value[index];
        if (!character) {
          break;
        }

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
          return {
            code: parsed,
            source,
            confidence: 0.55,
            rawMatch: value.slice(markerIndex, Math.min(value.length, index + 1)).trim(),
          };
        }
      }

      searchIndex = markerIndex + marker.length;
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

function resolveStructuredErrorMessage(
  responseRecord: Record<string, unknown> | undefined,
) {
  if (!responseRecord) {
    return null;
  }

  const directMessage = toStringValue(
    pickFirstValue(responseRecord, [
      "error.message",
      "message",
      "errorMessage",
      "status",
      "description",
      "reason",
    ]),
  );

  if (directMessage) {
    return directMessage;
  }

  const errorArrays = [
    readArrayPath(responseRecord, "errors"),
    readArrayPath(responseRecord, "error.errors"),
  ];

  for (const entries of errorArrays) {
    if (!entries) {
      continue;
    }

    for (const entry of entries) {
      if (typeof entry === "string" && entry.trim()) {
        return entry.trim();
      }

      if (!isRecord(entry)) {
        continue;
      }

      const message = toStringValue(
        pickFirstValue(entry, ["message", "description", "error", "reason", "status"]),
      );
      if (message) {
        return message;
      }
    }
  }

  const objectErrors = pickFirstValue(responseRecord, ["errors", "error.errors"]);
  if (isRecord(objectErrors)) {
    const joined = joinStructuredText(objectErrors);
    if (joined) {
      return joined;
    }
  }

  return null;
}

function extractAttemptErrorDetails(input: {
  structuredSources: Array<{ label: string; value: Record<string, unknown> | undefined }>;
  rejectReason: string | null;
  errorMessage: string | null;
}): ErrorDetails {
  const responseRecord = input.structuredSources[0]?.value;
  const errors = toStringArray(pickFirstValue(responseRecord, ["errors", "error.errors"]));
  const candidates: ErrorCodeCandidate[] = [];

  for (const source of input.structuredSources) {
    candidates.push(...collectStructuredErrorCandidates(source.value, source.label));
  }

  const textSources = [
    {
      value: input.rejectReason,
      source: "rejectReason_text",
    },
    {
      value: input.errorMessage,
      source: "errorMessage_text",
    },
    ...errors.map((value, index) => ({
      value,
      source: `errors[${index}]_text`,
    })),
  ];

  for (const source of textSources) {
    const candidate = findCodeInText(source.value, source.source);
    if (candidate) {
      candidates.push(candidate);
      break;
    }
  }

  const bestCandidate = candidates[0] ?? null;
  const nestedErrorMessage = resolveStructuredErrorMessage(responseRecord);
  const errorMessage =
    input.errorMessage ??
    nestedErrorMessage ??
    input.rejectReason ??
    errors[0] ??
    null;

  return {
    errorCode: bestCandidate?.code ?? null,
    errorMessage,
    errors,
    errorCodeSource: bestCandidate?.source ?? null,
    errorCodeConfidence: bestCandidate?.confidence ?? null,
    errorCodeRawMatch: bestCandidate?.rawMatch ?? null,
    usedTextFallback: (bestCandidate?.source ?? "").includes("_text"),
  };
}

function parseTargetAttempts(
  record: Record<string, unknown> | undefined,
  warnings: NormalizationWarning[],
): ParsedTargetAttempt[] {
  if (!record) {
    return [];
  }

  const rawEvents = readArrayPath(record, "events");
  if (!rawEvents) {
    return [];
  }

  const attempts: ParsedTargetAttempt[] = [];
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
    const responseBody = toObjectOrString(
      parseStructuredValue(
        eventStrVals.responseBody ?? null,
        `targetAttempts[${attempts.length}].responseBody`,
        warnings,
      ),
    );
    const requestBody = toObjectOrString(
      parseStructuredValue(
        eventStrVals.requestBody ?? null,
        `targetAttempts[${attempts.length}].requestBody`,
        warnings,
      ),
    );
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
      structuredSources: [
        {
          label: `targetAttempts[${attempts.length}].responseBody`,
          value: isRecord(responseBody) ? responseBody : undefined,
        },
      ],
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
      errorCodeSource: parsedErrors.errorCodeSource,
      errorCodeConfidence: parsedErrors.errorCodeConfidence,
      errorCodeRawMatch: parsedErrors.errorCodeRawMatch,
      usedTextFallback: parsedErrors.usedTextFallback,
      requestBody,
      responseBody,
      summaryReason: rejectedSummary?.reason ?? null,
      rawEventJson: rawEvent,
    });
  }

  return attempts;
}

function classifyBidDisposition(input: {
  bidAmount: number | null;
  winningBid: number | null;
  isZeroBid: boolean;
  reasonForReject: string | null;
  httpStatusCode: number | null;
  primaryAttempt: ParsedTargetAttempt | null;
  normalizedResponseBody: Record<string, unknown> | string | null;
  primaryErrorCode: number | null;
  primaryErrorMessage: string | null;
  primaryErrorSource: string | null;
}) {
  const minimumRevenueClassification = classifyMinimumRevenueDisposition(input.primaryAttempt);
  if (minimumRevenueClassification) {
    return minimumRevenueClassification;
  }

  if (
    (input.winningBid ?? 0) > 0 ||
    (input.bidAmount ?? 0) > 0 ||
    input.primaryAttempt?.winning === true ||
    (input.primaryAttempt?.accepted === true && (input.primaryAttempt.bidAmount ?? 0) > 0)
  ) {
    return buildDisposition({
      outcomeReasonCategory: "accepted",
      outcomeReasonCode: null,
      outcomeReasonMessage: null,
      classificationSource: "heuristic",
      classificationConfidence: 0.99,
    });
  }

  const primaryAttemptResponse =
    isRecord(input.primaryAttempt?.responseBody) ? input.primaryAttempt.responseBody : undefined;
  const primaryAttemptFallbackCode =
    input.primaryAttempt && input.primaryAttempt.errorCode !== null
      ? String(input.primaryAttempt.errorCode)
      : null;
  const primaryAttemptClassification = classifyStructuredResponse({
    responseRecord: primaryAttemptResponse,
    reasonForReject: input.reasonForReject,
    defaultSource: "primary_attempt_structured",
    defaultConfidence: 0.98,
    fallbackCode: primaryAttemptFallbackCode,
    fallbackMessage: input.primaryAttempt?.errorMessage ?? null,
  });
  if (primaryAttemptClassification) {
    return primaryAttemptClassification;
  }

  const topLevelResponse =
    isRecord(input.normalizedResponseBody) ? input.normalizedResponseBody : undefined;
  const topLevelClassification = classifyStructuredResponse({
    responseRecord: topLevelResponse,
    reasonForReject: input.reasonForReject,
    defaultSource: "response_body_structured",
    defaultConfidence: 0.94,
    fallbackCode: input.primaryErrorCode !== null ? String(input.primaryErrorCode) : null,
    fallbackMessage: input.primaryErrorMessage,
  });
  if (topLevelClassification) {
    return topLevelClassification;
  }

  const codeClassification = classifyFromPrimaryErrorCode({
    code: input.primaryErrorCode,
    message: input.primaryErrorMessage,
    source: input.primaryErrorSource,
  });
  if (codeClassification) {
    return codeClassification;
  }

  const reasonClassification = classifyFromReasonForReject(input.reasonForReject);
  if (reasonClassification) {
    return reasonClassification;
  }

  if (input.httpStatusCode === 429) {
    return buildDisposition({
      outcomeReasonCategory: "rate_limited",
      outcomeReasonCode: "429",
      outcomeReasonMessage: input.primaryErrorMessage ?? input.reasonForReject,
      classificationSource: "top_level_error",
      classificationConfidence: 0.88,
    });
  }

  if (input.httpStatusCode === 422) {
    return buildDisposition({
      outcomeReasonCategory: "request_invalid",
      outcomeReasonCode: "422",
      outcomeReasonMessage: input.primaryErrorMessage ?? input.reasonForReject,
      classificationSource: "top_level_error",
      classificationConfidence: 0.82,
    });
  }

  if (input.isZeroBid) {
    return buildDisposition({
      outcomeReasonCategory: "unknown_no_payable_bid",
      outcomeReasonCode: null,
      outcomeReasonMessage: input.primaryErrorMessage ?? input.reasonForReject,
      classificationSource: "is_zero_bid_flag",
      classificationConfidence: 0.46,
    });
  }

  if (input.reasonForReject || input.primaryErrorMessage) {
    return buildDisposition({
      outcomeReasonCategory: "unknown_no_payable_bid",
      outcomeReasonCode: extractReasonCodeFromText(input.reasonForReject),
      outcomeReasonMessage: input.primaryErrorMessage ?? input.reasonForReject,
      classificationSource: "heuristic",
      classificationConfidence: 0.35,
    });
  }

  return {
    outcome: "unknown",
    outcomeReasonCategory: null,
    outcomeReasonCode: null,
    outcomeReasonMessage: null,
    classificationSource: null,
    classificationConfidence: null,
    classificationWarnings: [],
  } satisfies BidDisposition;
}

function pickPrimaryAttempt(
  targetAttempts: ParsedTargetAttempt[],
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

function deriveSchemaVariant(
  body: Record<string, unknown> | undefined,
  record: Record<string, unknown> | undefined,
) {
  const reportRecords = readArrayPath(body, "report.records");

  if (reportRecords && record) {
    return "report_records";
  }

  if (body && record === body) {
    return "top_level_record";
  }

  if (body) {
    return "parsed_body";
  }

  return "unknown";
}

function parseFetchBody(
  fetchResult: RingbaFetchResult,
  warnings: NormalizationWarning[],
) {
  if (isRecord(fetchResult.rawBody)) {
    return fetchResult.rawBody;
  }

  if (typeof fetchResult.rawBody !== "string" || !fetchResult.rawBody.trim()) {
    return undefined;
  }

  const parsed = parseStructuredValue(fetchResult.rawBody, "fetchResult.rawBody", warnings);
  return isRecord(parsed) ? parsed : undefined;
}

function extractEventsAndUnknownNames(
  source: Record<string, unknown> | undefined,
) {
  const relevantEvents = extractEvents(source);
  const unknownEventNames = Array.from(
    new Set(
      relevantEvents
        .map((event) => event.eventName)
        .filter((eventName) => !KNOWN_EVENT_NAMES.has(eventName) && !eventName.startsWith("event_")),
    ),
  );

  return {
    relevantEvents,
    unknownEventNames,
  };
}

function parseRawRingbaBidDetail(fetchResult: RingbaFetchResult): ParsedRingbaBidDetail {
  const warnings: NormalizationWarning[] = [];
  const body = parseFetchBody(fetchResult, warnings);
  const record = getPrimaryRecord(body);

  if (readPath(body, "report.partialResult") === true) {
    addWarning(warnings, {
      code: "partial_report",
      message: "Ringba marked this payload as partial.",
      field: "report.partialResult",
    });
  }

  if (!record) {
    addWarning(warnings, {
      code: "shape_unknown",
      message: "Could not locate a primary Ringba record in the payload.",
      field: "rawBody",
    });
  }

  const { relevantEvents, unknownEventNames } = extractEventsAndUnknownNames(record ?? body);
  if (unknownEventNames.length > 0) {
    addWarning(warnings, {
      code: "unknown_event_names",
      message: `Found ${unknownEventNames.length} unknown Ringba event name(s).`,
      field: "events",
    });
  }

  return {
    body,
    record,
    sources: [
      {
        label: "record",
        value: record,
      },
      {
        label: "body",
        value: body,
      },
    ],
    relevantEvents,
    targetAttempts: parseTargetAttempts(record, warnings),
    traceJson: parseTraceValue(readPath(record, "trace"), "record.trace", warnings),
    schemaVariant: deriveSchemaVariant(body, record),
    warnings,
    unknownEventNames,
  };
}

function roundConfidence(value: number) {
  return Math.round(value * 100) / 100;
}

function computeNormalizationConfidence(input: {
  parseStatus: NormalizationParseStatus;
  missingCriticalFields: string[];
  missingOptionalFields: string[];
  warnings: NormalizationWarning[];
}) {
  let confidence = 0.99;

  for (const warning of input.warnings) {
    switch (warning.code) {
      case "json_parse_failed":
        confidence -= 0.16;
        break;
      case "trace_parse_failed":
        confidence -= 0.1;
        break;
      case "partial_report":
        confidence -= 0.15;
        break;
      case "shape_unknown":
        confidence -= 0.28;
        break;
      case "missing_critical_field":
        confidence -= 0.14;
        break;
      case "missing_optional_field":
        confidence -= 0.03;
        break;
      case "unknown_event_names":
        confidence -= 0.06;
        break;
      case "primary_error_code_text_fallback":
        confidence -= 0.12;
        break;
      case "bid_id_from_fetch_context":
        confidence -= 0.08;
        break;
      default:
        confidence -= 0.03;
        break;
    }
  }

  confidence -= Math.max(0, input.missingCriticalFields.length - 1) * 0.04;
  confidence -= Math.max(0, input.missingOptionalFields.length - 2) * 0.01;

  if (input.parseStatus === "partial") {
    confidence = Math.min(confidence, 0.48);
  }

  if (input.parseStatus === "text_fallback") {
    confidence = Math.min(confidence, 0.58);
  }

  if (input.parseStatus === "shape_unknown") {
    confidence = Math.min(confidence, 0.24);
  }

  return roundConfidence(Math.max(0.05, Math.min(1, confidence)));
}

function toPersistedTargetAttempt(attempt: ParsedTargetAttempt): BidTargetAttempt {
  const {
    errorCodeSource: _errorCodeSource,
    errorCodeConfidence: _errorCodeConfidence,
    errorCodeRawMatch: _errorCodeRawMatch,
    usedTextFallback: _usedTextFallback,
    ...normalizedAttempt
  } = attempt;
  return normalizedAttempt;
}

function normalizeParsedRingbaBid(
  parsed: ParsedRingbaBidDetail,
  fetchResult: RingbaFetchResult,
): NormalizedBidData {
  const warnings = [...parsed.warnings];
  const rawPathsUsed: Record<string, string[]> = {};
  const sources = parsed.sources;

  const bidId =
    toStringValue(getValueFromSources(sources, rawPathsUsed, "bidId", ["bidId", "id", "bid.id"])) ??
    fetchResult.bidId;
  if (!(rawPathsUsed.bidId?.length)) {
    addWarning(warnings, {
      code: "bid_id_from_fetch_context",
      message: "Recovered bid ID from fetch context instead of payload.",
      field: "bidId",
    });
  }

  const bidDt = toIsoDateTime(
    getValueFromSources(sources, rawPathsUsed, "bidDt", [
      "bidDt",
      "bidDateTime",
      "timestamp",
      "createdAt",
      "bid.timestamp",
    ]),
  );
  const campaignName = toStringValue(
    getValueFromSources(sources, rawPathsUsed, "campaignName", ["campaignName", "campaign.name"]),
  );
  const campaignId = toStringValue(
    getValueFromSources(sources, rawPathsUsed, "campaignId", ["campaignId", "campaign.id"]),
  );
  const publisherName = toStringValue(
    getValueFromSources(sources, rawPathsUsed, "publisherName", ["publisherName", "publisher.name"]),
  );
  const publisherId = toStringValue(
    getValueFromSources(sources, rawPathsUsed, "publisherId", ["publisherId", "publisher.id"]),
  );
  const bidAmount = toNumberValue(
    getValueFromSources(sources, rawPathsUsed, "bidAmount", [
      "bidAmount",
      "amount",
      "bid.amount",
      "buyerResponse.bidAmount",
    ]),
  );
  const winningBid = toNumberValue(
    getValueFromSources(sources, rawPathsUsed, "winningBid", [
      "winningBid",
      "winningBidAmount",
      "acceptedBid",
      "winning.amount",
    ]),
  );
  const reasonForReject = toStringValue(
    getValueFromSources(sources, rawPathsUsed, "reasonForReject", [
      "reasonForReject",
      "rejectReason",
      "rejection.reason",
      "statusReason",
    ]),
  );
  const httpStatusCode =
    toNumberValue(
      getValueFromSources(sources, rawPathsUsed, "httpStatusCode", [
        "httpStatusCode",
        "response.statusCode",
        "statusCode",
      ]),
    ) ?? fetchResult.httpStatusCode;
  const topLevelErrorMessage =
    toStringValue(
      getValueFromSources(sources, rawPathsUsed, "errorMessage", [
        "errorMessage",
        "error.message",
        "message",
        "response.error",
      ]),
    ) ?? fetchResult.transportError;
  const requestBody =
    toObjectOrString(
      parseStructuredValue(
        getValueFromSources(sources, rawPathsUsed, "requestBody", [
          "requestBody",
          "request.body",
          "requestPayload",
          "payload",
        ]),
        "requestBody",
        warnings,
      ),
    ) ?? null;
  const responseBody =
    toObjectOrString(
      parseStructuredValue(
        getValueFromSources(sources, rawPathsUsed, "responseBody", [
          "responseBody",
          "response.body",
          "buyerResponse",
          "body",
        ]),
        "responseBody",
        warnings,
      ),
    ) ??
    (parsed.record
      ? parsed.record
      : typeof fetchResult.rawBody === "string" || isRecord(fetchResult.rawBody)
        ? fetchResult.rawBody
        : null);
  const bidElapsedMs = toNumberValue(
    getValueFromSources(sources, rawPathsUsed, "bidElapsedMs", [
      "bidElapsedMs",
      "elapsedMs",
      "trace.bidElapsedMs",
    ]),
  );
  const explicitIsZeroBid = toBooleanValue(
    getValueFromSources(sources, rawPathsUsed, "isZeroBid", ["isZeroBid"]),
  );

  const isZeroBid =
    explicitIsZeroBid === true ||
    bidAmount === 0 ||
    winningBid === 0 ||
    String(reasonForReject ?? "").toLowerCase().includes("zero bid") ||
    parsed.relevantEvents.some((event) => event.eventName === "ZeroRTBBid") ||
    (parsed.targetAttempts.length > 0 &&
      parsed.targetAttempts.every((attempt) => {
        const hasExplicitBid = attempt.bidAmount !== null;
        return attempt.accepted !== true && (!hasExplicitBid || attempt.bidAmount === 0);
      }));
  const provisionalOutcome: InvestigationOutcome =
    (winningBid ?? 0) > 0 || (bidAmount ?? 0) > 0 ? "accepted" : "unknown";
  const primaryAttempt = pickPrimaryAttempt(parsed.targetAttempts, provisionalOutcome);

  const normalizedRequestBody = primaryAttempt?.requestBody ?? requestBody;
  const normalizedResponseBody = primaryAttempt?.responseBody ?? responseBody;
  if (primaryAttempt?.requestBody) {
    pushPathUsage(rawPathsUsed, "requestBody", "targetAttempts.primary.requestBody");
  }
  if (primaryAttempt?.responseBody) {
    pushPathUsage(rawPathsUsed, "responseBody", "targetAttempts.primary.responseBody");
  }

  const primaryErrorDetails =
    primaryAttempt && primaryAttempt.errorCode !== null
      ? {
          code: primaryAttempt.errorCode,
          source: primaryAttempt.errorCodeSource,
          confidence: primaryAttempt.errorCodeConfidence,
          rawMatch: primaryAttempt.errorCodeRawMatch,
          message: primaryAttempt.errorMessage,
          usedTextFallback: primaryAttempt.usedTextFallback,
        }
      : (() => {
          const details = extractAttemptErrorDetails({
            structuredSources: [
              {
                label: "responseBody",
                value: isRecord(normalizedResponseBody) ? normalizedResponseBody : undefined,
              },
              {
                label: "record",
                value: parsed.record,
              },
              {
                label: "body",
                value: parsed.body,
              },
            ],
            rejectReason: reasonForReject,
            errorMessage: topLevelErrorMessage,
          });

          return {
            code: details.errorCode,
            source: details.errorCodeSource,
            confidence: details.errorCodeConfidence,
            rawMatch: details.errorCodeRawMatch,
            message: details.errorMessage,
            usedTextFallback: details.usedTextFallback,
          };
        })();

  const disposition = classifyBidDisposition({
    bidAmount,
    winningBid,
    isZeroBid,
    reasonForReject,
    httpStatusCode,
    primaryAttempt,
    normalizedResponseBody,
    primaryErrorCode: primaryErrorDetails.code,
    primaryErrorMessage: primaryErrorDetails.message,
    primaryErrorSource: primaryErrorDetails.source,
  });
  const outcome = disposition.outcome;

  if (primaryAttempt?.targetName) {
    pushPathUsage(rawPathsUsed, "targetName", `targetAttempts.primary.${primaryAttempt.targetName}`);
    pushPathUsage(rawPathsUsed, "primaryTargetName", `targetAttempts.primary.${primaryAttempt.targetName}`);
  }
  if (primaryAttempt?.targetId) {
    pushPathUsage(rawPathsUsed, "targetId", "targetAttempts.primary.targetId");
    pushPathUsage(rawPathsUsed, "primaryTargetId", "targetAttempts.primary.targetId");
  }
  if (primaryAttempt?.targetBuyer) {
    pushPathUsage(rawPathsUsed, "buyerName", "targetAttempts.primary.targetBuyer");
    pushPathUsage(rawPathsUsed, "primaryBuyerName", "targetAttempts.primary.targetBuyer");
  }
  if (primaryAttempt?.targetBuyerId) {
    pushPathUsage(rawPathsUsed, "buyerId", "targetAttempts.primary.targetBuyerId");
    pushPathUsage(rawPathsUsed, "primaryBuyerId", "targetAttempts.primary.targetBuyerId");
  }

  if (primaryErrorDetails.usedTextFallback) {
    addWarning(warnings, {
      code: "primary_error_code_text_fallback",
      message: "Derived primary error code from text fallback instead of a structured field.",
      field: "primaryErrorCode",
    });
  }
  if (primaryErrorDetails.source) {
    pushPathUsage(rawPathsUsed, "primaryErrorCode", primaryErrorDetails.source);
  }

  const primaryFailureStage = deriveFailureStage(outcome, primaryAttempt);
  const missingCriticalFields = [
    ...(bidDt ? [] : ["bidDt"]),
    ...(parsed.targetAttempts.length > 0 ? [] : ["targetAttempts"]),
    ...(outcome === "unknown" ? ["outcome"] : []),
    ...(primaryFailureStage === "unknown" ? ["primaryFailureStage"] : []),
  ];
  const missingOptionalFields = [
    ...(campaignId ? [] : ["campaignId"]),
    ...(publisherId ? [] : ["publisherId"]),
    ...((primaryAttempt?.targetBuyerId ?? null) ? [] : ["buyerId"]),
    ...(normalizedRequestBody ? [] : ["requestBody"]),
    ...(normalizedResponseBody ? [] : ["responseBody"]),
    ...(reasonForReject ? [] : ["reasonForReject"]),
  ];

  for (const field of missingCriticalFields) {
    addWarning(warnings, {
      code: "missing_critical_field",
      message: `Missing critical field: ${field}.`,
      field,
    });
  }
  for (const field of missingOptionalFields) {
    addWarning(warnings, {
      code: "missing_optional_field",
      message: `Missing optional field: ${field}.`,
      field,
    });
  }

  const parseStatus: NormalizationParseStatus =
    parsed.schemaVariant === "unknown"
      ? "shape_unknown"
      : missingCriticalFields.length > 0 || warnings.some((warning) => warning.code === "partial_report")
        ? "partial"
        : primaryErrorDetails.usedTextFallback
          ? "text_fallback"
          : "complete";

  return {
    bidId,
    bidDt,
    campaignName,
    campaignId,
    publisherName,
    publisherId,
    targetName:
      primaryAttempt?.targetName ??
      toStringValue(getValueFromSources(sources, rawPathsUsed, "targetName", ["targetName", "target.name"])),
    targetId:
      primaryAttempt?.targetId ??
      toStringValue(getValueFromSources(sources, rawPathsUsed, "targetId", ["targetId", "target.id"])),
    buyerName:
      primaryAttempt?.targetBuyer ??
      toStringValue(getValueFromSources(sources, rawPathsUsed, "buyerName", ["buyerName", "buyer.name"])),
    buyerId:
      primaryAttempt?.targetBuyerId ??
      toStringValue(getValueFromSources(sources, rawPathsUsed, "buyerId", ["buyerId", "buyer.id"])),
    bidAmount,
    winningBid,
    bidElapsedMs,
    isZeroBid,
    reasonForReject,
    httpStatusCode,
    errorMessage: primaryAttempt?.errorMessage ?? topLevelErrorMessage,
    primaryFailureStage,
    primaryTargetName: primaryAttempt?.targetName ?? null,
    primaryTargetId: primaryAttempt?.targetId ?? null,
    primaryBuyerName: primaryAttempt?.targetBuyer ?? null,
    primaryBuyerId: primaryAttempt?.targetBuyerId ?? null,
    primaryErrorCode: primaryErrorDetails.code,
    primaryErrorMessage:
      primaryAttempt?.errorMessage ??
      primaryErrorDetails.message ??
      topLevelErrorMessage,
    requestBody: normalizedRequestBody,
    responseBody: normalizedResponseBody,
    rawTraceJson: {
      requestUrl: fetchResult.requestUrl,
      fetchedAt: fetchResult.fetchedAt,
      httpStatusCode: fetchResult.httpStatusCode,
      errorKind: fetchResult.errorKind,
      latencyMs: fetchResult.latencyMs,
      attemptCount: fetchResult.attemptCount,
      responseHeaders: fetchResult.responseHeaders,
      transportError: fetchResult.transportError,
      trace: parsed.traceJson,
      payload: isRecord(fetchResult.rawBody)
        ? fetchResult.rawBody
        : {
            raw: fetchResult.rawBody,
          },
    },
    relevantEvents: parsed.relevantEvents,
    targetAttempts: parsed.targetAttempts.map(toPersistedTargetAttempt),
    outcome,
    outcomeReasonCategory: disposition.outcomeReasonCategory,
    outcomeReasonCode: disposition.outcomeReasonCode,
    outcomeReasonMessage: disposition.outcomeReasonMessage,
    classificationSource: disposition.classificationSource,
    classificationConfidence: disposition.classificationConfidence,
    classificationWarnings: disposition.classificationWarnings,
    parseStatus,
    normalizationVersion: NORMALIZATION_VERSION,
    schemaVariant: parsed.schemaVariant,
    normalizationConfidence: computeNormalizationConfidence({
      parseStatus,
      missingCriticalFields,
      missingOptionalFields,
      warnings,
    }),
    normalizationWarnings: warnings,
    missingCriticalFields,
    missingOptionalFields,
    unknownEventNames: parsed.unknownEventNames,
    rawPathsUsed,
    primaryErrorCodeSource: primaryErrorDetails.source,
    primaryErrorCodeConfidence: primaryErrorDetails.confidence,
    primaryErrorCodeRawMatch: primaryErrorDetails.rawMatch,
  };
}

export function normalizeRingbaBidDetail(
  fetchResult: RingbaFetchResult,
): NormalizedBidData {
  return normalizeParsedRingbaBid(parseRawRingbaBidDetail(fetchResult), fetchResult);
}
