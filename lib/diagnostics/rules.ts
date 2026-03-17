import type {
  DiagnosisEvidence,
  DiagnosisResult,
  NormalizedBidData,
} from "@/types/bid";
import { lowercaseText, stringifyJson } from "@/lib/utils/json";

interface RuleMatch {
  matched: boolean;
  result?: DiagnosisResult;
}

function matchesAny(text: string, phrases: string[]) {
  for (const phrase of phrases) {
    if (text.includes(phrase)) {
      return true;
    }
  }

  return false;
}

function pushEvidence(
  evidence: DiagnosisEvidence[],
  field: string,
  value: string | number | boolean | null,
  description: string,
) {
  evidence.push({ field, value, description });
}

function buildContextText(bid: NormalizedBidData) {
  const parts = [
    lowercaseText(bid.errorMessage),
    lowercaseText(bid.reasonForReject),
    lowercaseText(bid.responseBody),
    lowercaseText(bid.requestBody),
    lowercaseText(
      bid.relevantEvents.map((event) => ({
        eventName: event.eventName,
        eventValsJson: event.eventValsJson,
        eventStrValsJson: event.eventStrValsJson,
      })),
    ),
  ];

  return parts.filter(Boolean).join(" | ");
}

function minimumRevenueThreshold() {
  const rawValue = process.env.MINIMUM_REVENUE_THRESHOLD;

  if (!rawValue) {
    return null;
  }

  const parsed = Number(rawValue);
  return Number.isFinite(parsed) ? parsed : null;
}

function acceptedBidNeutralDiagnosis(
  bid: NormalizedBidData,
  evidence: DiagnosisEvidence[],
): DiagnosisResult {
  pushEvidence(
    evidence,
    "outcome",
    bid.outcome,
    "Diagnostics skipped failure heuristics because the bid was accepted.",
  );
  pushEvidence(
    evidence,
    "outcomeReasonCategory",
    bid.outcomeReasonCategory,
    "Normalization already identified this bid as accepted.",
  );

  return {
    rootCause: "unknown_needs_review",
    confidence: 0.99,
    severity: "low",
    ownerType: "unknown",
    suggestedFix:
      "No failure remediation is needed because the bid was accepted. Review the trace only if you need additional context on rejected sibling attempts.",
    explanation:
      "The winning bid was accepted, so diagnostics skipped failure-oriented heuristics that may still appear in rejected sibling attempts.",
    evidence,
  };
}

function derivedClassificationRule(
  bid: NormalizedBidData,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  if (!bid.outcomeReasonCategory || bid.outcomeReasonCategory === "accepted") {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "outcomeReasonCategory",
    bid.outcomeReasonCategory,
    "Structured normalization classified the bid outcome before diagnostics ran.",
  );
  pushEvidence(
    evidence,
    "outcomeReasonCode",
    bid.outcomeReasonCode,
    "Derived classification code from normalization.",
  );
  pushEvidence(
    evidence,
    "classificationSource",
    bid.classificationSource,
    "Where the derived classification came from.",
  );

  const derivedConfidence = Math.max(0.55, Math.min(0.99, bid.classificationConfidence ?? 0.8));

  switch (bid.outcomeReasonCategory) {
    case "missing_caller_id":
      return {
        matched: true,
        result: {
          rootCause: "missing_caller_id",
          confidence: derivedConfidence,
          severity: "high",
          ownerType: "publisher",
          suggestedFix:
            "Confirm the upstream source populates caller_id and that Ringba maps it into the buyer payload.",
          explanation:
            "Structured Ringba response details show the bid failed because caller_id was required but not provided.",
          evidence,
        },
      };
    case "missing_required_field":
      return {
        matched: true,
        result: {
          rootCause: "missing_zip_or_required_payload_field",
          confidence: derivedConfidence,
          severity: "high",
          ownerType: "publisher",
          suggestedFix:
            "Validate the upstream payload and Ringba mappings for zip and other required fields before bidding.",
          explanation:
            "Structured Ringba response details show a required request field was missing before a payable bid could be returned.",
          evidence,
        },
      };
    case "request_invalid":
      return {
        matched: true,
        result: {
          rootCause: "payload_validation_error",
          confidence: derivedConfidence,
          severity: "high",
          ownerType: "publisher",
          suggestedFix:
            "Compare the buyer contract with the generated request payload and correct invalid or missing values.",
          explanation:
            "Structured Ringba response details show the buyer rejected the request as invalid before bidding.",
          evidence,
        },
      };
    case "rate_limited":
      return {
        matched: true,
        result: {
          rootCause: "rate_limited",
          confidence: derivedConfidence,
          severity: "high",
          ownerType: "buyer",
          suggestedFix:
            "Review buyer-side throttling, request pacing, and any per-publisher traffic caps.",
          explanation:
            "Structured Ringba response details show the bid was blocked by rate limiting rather than a normal bid decision.",
          evidence,
        },
      };
    case "buyer_returned_zero_bid":
      return {
        matched: true,
        result: {
          rootCause: "buyer_returned_zero_bid",
          confidence: derivedConfidence,
          severity: "medium",
          ownerType: "buyer",
          suggestedFix:
            "Review buyer capacity, pricing, and targeting to understand why the request produced no payable bid.",
          explanation:
            "Structured Ringba response details indicate the buyer evaluated the request but did not return a payable bid.",
          evidence,
        },
      };
    case "below_minimum_revenue":
      return {
        matched: true,
        result: {
          rootCause: "below_minimum_revenue",
          confidence: derivedConfidence,
          severity: "medium",
          ownerType: "ringba_config",
          suggestedFix:
            "Review the campaign's minimum revenue floor and compare it with the buyer bid amounts being returned.",
          explanation:
            "Structured Ringba attempt details show the buyer returned a bid, but Ring Tree minimum revenue rejected it before a payable outcome.",
          evidence,
        },
      };
    case "tag_filtered_initial":
    case "tag_filtered_final":
    case "no_matching_buyer":
    case "no_capacity":
      return {
        matched: true,
        result: {
          rootCause: "no_eligible_targets",
          confidence: derivedConfidence,
          severity: "medium",
          ownerType: "ringba_config",
          suggestedFix:
            "Review Ringba routing, target filters, buyer eligibility, and capacity settings for this campaign.",
          explanation:
            "Structured Ringba response details show the bid failed during routing, filtering, or buyer matching rather than from a true zero-price bid.",
          evidence,
        },
      };
    case "unknown_no_payable_bid":
      return {
        matched: true,
        result: {
          rootCause: "unknown_needs_review",
          confidence: Math.min(derivedConfidence, 0.6),
          severity: "medium",
          ownerType: "unknown",
          suggestedFix:
            "Inspect the structured response, reject reason, and target attempts to refine this no-payable-bid classification.",
          explanation:
            "Normalization identified a no-payable-bid outcome, but the stored evidence was not specific enough to assign a stronger root cause.",
          evidence,
        },
      };
    default:
      return { matched: false };
  }
}

function rateLimitedRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched =
    bid.httpStatusCode === 429 ||
    matchesAny(contextText, ["rate limit", "too many requests", "throttle"]);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "httpStatusCode",
    bid.httpStatusCode,
    "Ringba or the downstream buyer returned a rate limiting status.",
  );

  return {
    matched: true,
    result: {
      rootCause: "rate_limited",
      confidence: 0.97,
      severity: "high",
      ownerType: "buyer",
      suggestedFix:
        "Review buyer-side throttling, request pacing, and any per-publisher traffic caps.",
      explanation:
        "The request appears to have been rate limited before a normal bid decision could complete.",
      evidence,
    },
  };
}

function missingCallerIdRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched =
    matchesAny(contextText, ["caller_id_required", "caller id required"]) ||
    (contextText.includes("caller_id") && contextText.includes("missing"));

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "requestBody",
    stringifyJson(bid.requestBody),
    "The request payload or response language indicates caller_id was required but absent.",
  );

  return {
    matched: true,
    result: {
      rootCause: "missing_caller_id",
      confidence: 0.98,
      severity: "high",
      ownerType: "publisher",
      suggestedFix:
        "Confirm the upstream source populates caller_id and that Ringba maps it into the buyer payload.",
      explanation:
        "The buyer endpoint rejected the request because caller_id was missing from the payload.",
      evidence,
    },
  };
}

function missingZipRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const mentionsZip = contextText.includes("zip");
  const mentionsMissing =
    contextText.includes("required") || contextText.includes("missing");

  if (!(mentionsZip && mentionsMissing)) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "responseBody",
    stringifyJson(bid.responseBody),
    "The payload validation language points to a missing zip or required field.",
  );

  return {
    matched: true,
    result: {
      rootCause: "missing_zip_or_required_payload_field",
      confidence: 0.93,
      severity: "high",
      ownerType: "publisher",
      suggestedFix:
        "Validate the upstream payload and Ringba mappings for zip and other required fields before bidding.",
      explanation:
        "A required request field appears to be missing, with zip being the strongest signal in the response.",
      evidence,
    },
  };
}

function validationRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched =
    bid.httpStatusCode === 422 ||
    matchesAny(contextText, [
      "validation",
      "invalid field",
      "unprocessable",
      "schema error",
    ]);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "httpStatusCode",
    bid.httpStatusCode,
    "The response looks like a payload validation failure.",
  );

  return {
    matched: true,
    result: {
      rootCause: "payload_validation_error",
      confidence: 0.92,
      severity: "high",
      ownerType: "publisher",
      suggestedFix:
        "Compare the buyer contract with the generated request payload and correct field shapes or values.",
      explanation:
        "The request reached the buyer but failed payload validation before a valid bid could be returned.",
      evidence,
    },
  };
}

function zeroBidRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const capacityLanguage = matchesAny(contextText, [
    "capacity",
    "final capacity",
    "zero bid",
    "no bid",
  ]);
  const matched = bid.isZeroBid || ((bid.bidAmount ?? 0) === 0 && capacityLanguage);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "bidAmount",
    bid.bidAmount,
    "The normalized bid amount resolved to zero or the response explicitly said no bid.",
  );

  return {
    matched: true,
    result: {
      rootCause: "buyer_returned_zero_bid",
      confidence: 0.95,
      severity: "medium",
      ownerType: "buyer",
      suggestedFix:
        "Review buyer capacity, targeting, and any final capacity checks that can zero out the bid.",
      explanation:
        "The buyer evaluated the request but ultimately returned a zero bid or no-bid response.",
      evidence,
    },
  };
}

function minimumRevenueRule(
  bid: NormalizedBidData,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const threshold = minimumRevenueThreshold();
  const amount = bid.winningBid ?? bid.bidAmount;

  if (threshold === null || amount === null || amount >= threshold) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "winningBid",
    amount,
    `The bid came in below the configured minimum revenue threshold of ${threshold}.`,
  );

  return {
    matched: true,
    result: {
      rootCause: "below_minimum_revenue",
      confidence: 0.9,
      severity: "medium",
      ownerType: "ringba_config",
      suggestedFix:
        "Check the campaign's minimum revenue floor and buyer payout strategy.",
      explanation:
        "A bid was returned, but it appears to fall below the configured revenue threshold for acceptance.",
      evidence,
    },
  };
}

function timeoutRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched =
    bid.httpStatusCode === 408 ||
    bid.httpStatusCode === 504 ||
    matchesAny(contextText, ["timeout", "timed out", "ping timeout"]);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "errorMessage",
    bid.errorMessage,
    "The request appears to have timed out during buyer or network processing.",
  );

  return {
    matched: true,
    result: {
      rootCause: "timeout",
      confidence: 0.95,
      severity: "high",
      ownerType: "system",
      suggestedFix:
        "Inspect buyer response times, timeout thresholds, and any recent network instability between Ringba and the buyer.",
      explanation:
        "The bid request did not finish within the allowed time window, so Ringba could not complete the transaction.",
      evidence,
    },
  };
}

function confirmationFailureRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched =
    contextText.includes("confirmation") &&
    matchesAny(contextText, ["failed", "failure", "declined"]);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "responseBody",
    stringifyJson(bid.responseBody),
    "The trace references a confirmation failure after a bid decision.",
  );

  return {
    matched: true,
    result: {
      rootCause: "confirmation_failure",
      confidence: 0.88,
      severity: "high",
      ownerType: "buyer",
      suggestedFix:
        "Review confirmation callback handling and buyer acceptance logic after the initial bid response.",
      explanation:
        "The bid process appears to have advanced, but the final confirmation step failed.",
      evidence,
    },
  };
}

function enrichmentRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched = matchesAny(contextText, [
    "enrichment",
    "passthrough",
    "custom scoring",
    "third party",
    "enrich",
  ]);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "rawTraceJson",
    "see raw trace",
    "The trace suggests an enrichment, passthrough, or custom scoring dependency failed.",
  );

  return {
    matched: true,
    result: {
      rootCause: "third_party_or_enrichment_failure",
      confidence: 0.84,
      severity: "medium",
      ownerType: "system",
      suggestedFix:
        "Check the external enrichment provider, custom scoring config, and Ringba passthrough dependencies.",
      explanation:
        "A third-party enrichment or custom scoring dependency appears to have failed during bid evaluation.",
      evidence,
    },
  };
}

function noEligibleTargetsRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  const matched = matchesAny(contextText, [
    "no eligible targets",
    "no targets",
    "no matching targets",
  ]);

  if (!matched) {
    return { matched: false };
  }

  pushEvidence(
    evidence,
    "targetId",
    bid.targetId,
    "The trace indicates that no target was eligible to accept the request.",
  );

  return {
    matched: true,
    result: {
      rootCause: "no_eligible_targets",
      confidence: 0.9,
      severity: "medium",
      ownerType: "ringba_config",
      suggestedFix:
        "Review campaign routing rules, target availability, and current eligibility filters in Ringba.",
      explanation:
        "Ringba could not route the request because no eligible targets matched at bid time.",
      evidence,
    },
  };
}

export function diagnoseBid(normalizedBid: NormalizedBidData): DiagnosisResult {
  const evidence: DiagnosisEvidence[] = [];

  if (
    normalizedBid.outcome === "accepted" ||
    normalizedBid.outcomeReasonCategory === "accepted"
  ) {
    return acceptedBidNeutralDiagnosis(normalizedBid, evidence);
  }

  const contextText = buildContextText(normalizedBid);

  const ruleRunners = [
    (ruleEvidence: DiagnosisEvidence[]) =>
      derivedClassificationRule(normalizedBid, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      rateLimitedRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      missingCallerIdRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      missingZipRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      validationRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      zeroBidRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) => minimumRevenueRule(normalizedBid, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      timeoutRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      confirmationFailureRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      enrichmentRule(normalizedBid, contextText, ruleEvidence),
    (ruleEvidence: DiagnosisEvidence[]) =>
      noEligibleTargetsRule(normalizedBid, contextText, ruleEvidence),
  ];

  for (const runRule of ruleRunners) {
    const ruleEvidence: DiagnosisEvidence[] = [];
    const rule = runRule(ruleEvidence);
    if (rule.matched && rule.result) {
      return rule.result;
    }
  }

  pushEvidence(
    evidence,
    "httpStatusCode",
    normalizedBid.httpStatusCode,
    "No strong rule matched the current Ringba response.",
  );

  return {
    rootCause: "unknown_needs_review",
    confidence: 0.35,
    severity: "medium",
    ownerType: "unknown",
    suggestedFix:
      "Inspect the raw request, response, and event timeline to refine the diagnosis rule set for this pattern.",
    explanation:
      "The trace did not match a known diagnosis rule strongly enough, so it should be reviewed manually.",
    evidence,
  };
}
