import type { DiagnosisEvidence, DiagnosisResult, NormalizedBidData } from "@/types/bid";
import type { ImportSourceRow } from "@/lib/db/schema";

function normalizeText(value: string | null | undefined) {
  return (value ?? "").trim().toLowerCase();
}

function buildEvidence(
  field: string,
  value: DiagnosisEvidence["value"] | undefined,
  description: string,
): DiagnosisEvidence {
  return {
    field,
    value: value === undefined ? null : value,
    description,
  };
}

export function buildCsvDiagnosis(input: {
  normalizedBid: NormalizedBidData;
  sourceRow: ImportSourceRow;
}): DiagnosisResult {
  const reason = normalizeText(input.normalizedBid.reasonForReject);
  const isRejected =
    input.sourceRow.bidRejected === true ||
    Boolean(input.normalizedBid.reasonForReject);

  if (reason.includes("zero bid")) {
    return {
      rootCause: "buyer_returned_zero_bid",
      confidence: 0.7,
      severity: "medium",
      ownerType: "buyer",
      suggestedFix: "Review buyer response rules or floor pricing.",
      explanation: "The CSV indicates a zero bid rejection from the buyer.",
      evidence: [
        buildEvidence("reason_for_reject", input.normalizedBid.reasonForReject, "CSV reason"),
      ],
    };
  }

  if (
    reason.includes("no capacity") ||
    reason.includes("final capacity") ||
    reason.includes("no eligible")
  ) {
    return {
      rootCause: "no_eligible_targets",
      confidence: 0.7,
      severity: "high",
      ownerType: "ringba_config",
      suggestedFix: "Verify eligible targets and capacity settings.",
      explanation: "The CSV rejection reason suggests no eligible targets.",
      evidence: [
        buildEvidence("reason_for_reject", input.normalizedBid.reasonForReject, "CSV reason"),
      ],
    };
  }

  if (reason.includes("rate limit") || reason.includes("rate-limited")) {
    return {
      rootCause: "rate_limited",
      confidence: 0.6,
      severity: "medium",
      ownerType: "system",
      suggestedFix: "Check rate limit policies or reduce request volume.",
      explanation: "The rejection reason indicates rate limiting.",
      evidence: [
        buildEvidence("reason_for_reject", input.normalizedBid.reasonForReject, "CSV reason"),
      ],
    };
  }

  if (reason.includes("timeout")) {
    return {
      rootCause: "timeout",
      confidence: 0.6,
      severity: "medium",
      ownerType: "system",
      suggestedFix: "Inspect timeouts in downstream systems.",
      explanation: "The CSV indicates a timeout during bid processing.",
      evidence: [
        buildEvidence("reason_for_reject", input.normalizedBid.reasonForReject, "CSV reason"),
      ],
    };
  }

  if (reason.includes("payload") || reason.includes("validation")) {
    return {
      rootCause: "payload_validation_error",
      confidence: 0.6,
      severity: "high",
      ownerType: "publisher",
      suggestedFix: "Validate payload fields and required inputs.",
      explanation: "The CSV reason suggests payload validation issues.",
      evidence: [
        buildEvidence("reason_for_reject", input.normalizedBid.reasonForReject, "CSV reason"),
      ],
    };
  }

  if (isRejected) {
    return {
      rootCause: "unknown_needs_review",
      confidence: 0.4,
      severity: "high",
      ownerType: "unknown",
      suggestedFix: "Review CSV fields and downstream rules for more detail.",
      explanation: "The CSV shows a rejection but no known category match.",
      evidence: [
        buildEvidence("reason_for_reject", input.normalizedBid.reasonForReject, "CSV reason"),
      ],
    };
  }

  return {
    rootCause: "unknown_needs_review",
    confidence: 0.3,
    severity: "low",
    ownerType: "unknown",
    suggestedFix: "Review the bid data for a clearer classification.",
    explanation: "The CSV does not provide enough information to classify the outcome.",
    evidence: [
      buildEvidence("bid_amount", input.normalizedBid.bidAmount, "CSV bid amount"),
      buildEvidence("winning_bid", input.normalizedBid.winningBid, "CSV winning bid"),
    ],
  };
}
