# Bid Investigation (`lib/investigations/`)

This module is responsible for analyzing individual bid IDs and determining the root cause of failures.

## Investigation Flow

1.  **Claiming:** Use `claimInvestigationFetch` to ensure that a bid is not already being fetched by another worker.
2.  **Fetching:** Call `fetchRingbaBidDetail` from `lib/ringba/` to retrieve the raw trace for the bid ID.
3.  **Normalization:** Use `normalizeRingbaBidDetail` from `lib/ringba/` to transform the raw response into a standard format (`NormalizedBidData`).
4.  **Diagnostics:** Execute `diagnoseBid` from `lib/diagnostics/` to apply a series of rules to the normalized data and identify a root cause.
5.  **Persistence:** Store the results (raw trace, normalized data, diagnosis) in the `bid_investigations` table using `upsertInvestigation`.

## Key Concepts

- **Enrichment State:** Indicates whether an investigation is `enriched` (full trace retrieved), `not_found` (bid ID not in Ringba), or `failed`.
- **Fetch Status:** Tracks the progress of the Ringba API fetch (`pending`, `completed`, `failed`).
- **Diagnosis:** The outcome of the rule-based engine, including `rootCause`, `confidence`, `severity`, `ownerType`, `suggestedFix`, and `explanation`.
- **Evidence:** A collection of specific fields and values that support the diagnosis.

## Guidelines

- **Force Refresh:** When `forceRefresh` is `true`, a new fetch is always initiated, even if a completed investigation already exists for the bid ID.
- **Poll for Pending:** If an investigation is currently being fetched by another worker, the `investigateBid` function can optionally wait (poll) for it to complete.
- **Error Propagation:** Differentiate between transport errors (network, timeouts) and logical errors (invalid payload, API errors). Log both for troubleshooting.
- **Ringba Retries:** Use the `nextRingbaRetryAt` and `ringbaFailureCount` fields in the database to manage rate-limited or transient API failures.

## Integration with Diagnostics

The `investigations` service is the primary consumer of the `diagnostics` module. It passes normalized data to the diagnostic engine and persists the resulting diagnosis and evidence.
