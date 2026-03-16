# Import Pipeline (`lib/import-runs/`)

This module orchestrates the ingestion of bulk data from various sources into the system.

## Import Sources

- **`manual_bulk`:** User-provided bid IDs pasted into the UI.
- **`csv_import`:** Bid IDs extracted from uploaded CSV files.
- **`csv_direct_import`:** Raw data stored directly in SQLite for reference without immediate Ringba fetches.
- **`ringba_recent_import`:** Recently discovered bid IDs from the Ringba RTB export API.
- **`historical_ringba_backfill`:** Incremental fetch of Ringba details for existing bid IDs.

## Lifecycle of an Import Run

1.  **Creation:** An `importRun` is created in the `queued` status.
2.  **Claiming:** A worker claims the run and sets its status to `processing`.
3.  **Item Claiming:** The worker claims a batch of `importRunItems` for processing.
4.  **Processing:**
    - For Ringba fetches: Orchestrates `investigateBid` for each item.
    - For direct CSV: Normalizes and stores raw rows in `importSourceRows`.
5.  **Finalization:** Once all items are processed (or failed), the run status is updated to `completed`, `completed_with_errors`, or `failed`.

## State Management

- **Concurrency Control:** Use the `claimImportRunProcessing` and `claimImportRunItems` database operations to ensure that only one worker is processing a run or item at a time.
- **Status Transitions:** Only transition statuses forward (e.g., `queued` -> `processing` -> `completed`).
- **Error Handling:** Catch item-level errors and update `importRunItems` status to `failed` with an error message, allowing the rest of the run to continue.
- **Retries:** Failed items can be reset to `queued` for retry using `retryFailedImportRunItems`.

## Integration with Schedules

- Scheduled runs are triggered by `import-schedules`.
- The `syncScheduledRunStatus` function updates the parent schedule's health and operational events based on the run's final state.
- Some source types (e.g., `historical_ringba_backfill`) update a `checkpointSourceKey` upon completion to track progress.

## Guidelines

- **Batch Size:** Keep batches small (e.g., 10-25 items) to minimize the impact of long-running operations and allow for more granular progress tracking.
- **Lease Expiration:** Ensure that `processorLeaseExpiresAt` is updated regularly during long-running imports to prevent other workers from claiming an active run.
- **Metadata:** Use the `sourceMetadata` JSON column in `importRuns` to store source-specific configuration and progress metrics.
