# Diagnostic Engine (`lib/diagnostics/`)

This module contains the business rules for interpreting Ringba bid traces and providing automated root-cause analysis.

## Rule Structure

A diagnostic rule is a function that takes `NormalizedBidData` and returns a `RuleMatch` object:

```typescript
function myNewRule(
  bid: NormalizedBidData,
  contextText: string,
  evidence: DiagnosisEvidence[],
): RuleMatch {
  // 1. Identify a specific pattern in the bid data
  const matched = bid.httpStatusCode === 422 || contextText.includes("validation error");

  if (!matched) {
    return { matched: false };
  }

  // 2. Document the evidence supporting the match
  pushEvidence(evidence, "httpStatusCode", bid.httpStatusCode, "Response indicates a validation failure.");

  // 3. Return a detailed diagnosis result
  return {
    matched: true,
    result: {
      rootCause: "my_custom_root_cause",
      confidence: 0.95,
      severity: "high",
      ownerType: "publisher",
      suggestedFix: "Fix the payload mapping in Ringba.",
      explanation: "The buyer endpoint rejected the bid due to a schema mismatch.",
      evidence,
    },
  };
}
```

## Adding a New Rule

1.  **Define the Rule:** Create a new rule function in `lib/diagnostics/rules.ts` that implements the logic for identifying a specific bid failure pattern.
2.  **Register the Rule:** Add the new rule function to the `rules` array inside the `diagnoseBid` function. The order of rules in the array determines their priority.
3.  **Update Evidence:** Use the `pushEvidence` helper to capture relevant fields and values that support the diagnosis.
4.  **Test:** Add a unit test in `tests/diagnostics.test.ts` to verify the rule matches the expected bid data.

## Guidelines

- **Specificity:** Rules should be as specific as possible to avoid false positives. Use unique HTTP status codes or error message patterns.
- **Confidence:** Assign a confidence score (0.0 to 1.0) based on how certain the rule is about the root cause.
- **Evidence:** Always capture the specific fields and values that triggered the rule match for operator review.
- **Context Text:** Use the `contextText` string, which combines multiple bid fields into a searchable blob, for keyword-based matches.
- **Unknown Match:** If no rules match, the engine returns an `unknown_needs_review` result with low confidence, signaling that a human should investigate and potentially add a new rule.
