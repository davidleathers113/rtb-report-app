# Ringba Recent Import Validation Plan

## Goal

Verify the real end-to-end `ringba_recent_import` flow against live Ringba data and the local SQLite database.

Also verify that any Ringba data that should already be present but is missing from SQLite is identified and backfilled into the local database.

This validation should confirm:

- Ringba credentials are accepted
- export creation works
- export polling works
- ZIP download and extraction work
- CSV parsing works
- bid IDs are deduped and enqueued
- downstream investigations are fetched and persisted
- SQLite state is updated correctly
- missing Ringba-backed records are backfilled into SQLite
- checkpoints only advance on successful source-stage completion

## Validation Strategy

Use the existing app route instead of a one-off script so the same application code path is exercised.

Start with the smallest practical window first. A 5-minute run makes the results easier to inspect and makes failures easier to localize.

## Step 1: Confirm local prerequisites

Before testing anything, make sure:

- `.env` contains `RINGBA_ACCOUNT_ID`
- `.env` contains `RINGBA_API_TOKEN`
- `.env` contains `RINGBA_API_BASE_URL` only if you intentionally override the default
- `.env` contains `RINGBA_AUTH_SCHEME` only if you intentionally override the default `Token` scheme for Ringba API access tokens
- `.env` contains `BID_CONSOLE_DB_PATH` only if you intentionally override the default SQLite path
- dependencies are installed
- the app is using the local SQLite database

The default local database path is `data/bid-investigation-console.sqlite`.

Run:

```bash
pnpm install
```

## Step 2: Reset and prepare the local database

Start from a clean local state so the outcome is easy to verify.

Run:

```bash
pnpm db:reset
pnpm db:migrate
```

What to confirm:

- both commands succeed
- the SQLite file exists after migration
- the database is ready for local writes

## Step 3: Start the app with real env vars

Launch the app locally with your real Ringba credentials.

Run:

```bash
pnpm dev
```

What to confirm:

- the app boots successfully
- there are no startup errors
- the local SQLite file is writable
- environment validation does not fail

## Step 4: Trigger a minimal live Ringba import

Use a second terminal and trigger the smallest practical recent-import run first.

Use a 5-minute window with no force refresh:

```bash
curl -X POST http://localhost:3000/api/import-runs/ringba-recent \
  -H "Content-Type: application/json" \
  -d '{"windowMinutes":5,"forceRefresh":false}'
```

What to capture:

- HTTP status
- the returned import run identifier
- the initial run status payload

Expected result:

- HTTP `202`
- a JSON response for the created run

## Step 5: Poll the run until it finishes

Poll the run detail endpoint until the run reaches a terminal state:

```bash
curl http://localhost:3000/api/import-runs/<importRunId>
```

Repeat until the run reaches one of:

- `completed`
- `completed_with_errors`
- `failed`

What to verify during polling:

- the run exists
- `sourceStage` progresses in the expected order
- diagnostics populate as the run advances

Expected source-stage progression:

- `creating_export`
- `polling_export`
- `downloading`
- `extracting`
- `parsing`
- `queued`
- `processing`
- terminal state

## Step 6: Verify source-stage success

Once the run has advanced far enough, confirm the Ringba export path worked.

Inspect the run payload for:

- `exportJobId`
- `sourceMetadata.diagnostics`
- `exportReadyLatencyMs`
- `parsedRowCount`
- `extractedBidIdCount`
- `dedupedBidIdCount`
- `insertedItemCount`
- `failedStage` or `sourceStageError` if the run fails

This confirms:

- export job creation worked
- export polling worked
- ZIP download worked
- CSV extraction worked
- bid ID parsing worked

Implementation note:

- the export-create request should use Ringba API access tokens with `Authorization: Token ...`
- the export-create request should not force `formatTimeZone` to `UTC`; omit the field unless you intentionally provide a Ringba-accepted IANA time zone

## Step 7: Verify import-run items were created

Confirm the parsed bid IDs were actually inserted into the downstream queue.

What to verify:

- `items` exist for the run
- `totalItems`, `queuedCount`, `runningCount`, `completedCount`, and `failedCount` look consistent
- `import_run_items` contains deduped bid IDs for the run
- duplicate bid IDs were not inserted twice
- item counts match the deduped parsed result

Success condition:

- the run has queued or processed items, even if the count is small

## Step 8: Verify downstream investigations persist

Now confirm the second half of the flow worked.

Inspect local persistence and verify:

- `bid_investigations` starts filling
- investigation rows correspond to the imported bid IDs
- reused investigations are reused instead of unnecessarily refetched
- related investigation details are persisted

This confirms:

- bid-detail fetches from Ringba are working
- the app is persisting investigations into SQLite
- the downstream processor is functioning

## Step 9: Verify there is no missing Ringba data in SQLite

After the first successful run, confirm SQLite is not missing data that Ringba returned for the validated window.

What to verify:

- the number of deduped Ringba bid IDs for the run matches the number of `import_run_items` created for that run
- every queued or completed item for the run has a corresponding persisted source or investigation record as expected
- there are no obvious gaps where Ringba returned data but SQLite does not contain the matching run, item, source, or investigation rows

If gaps are found:

- record the missing bid IDs or row counts
- identify whether the gap is in source parsing, item insertion, downstream processing, or investigation persistence
- rerun or resume processing until the missing Ringba data is backfilled into SQLite

Success condition:

- all data returned by Ringba for the validated window is represented in SQLite, or any remaining discrepancy is fully explained

## Step 10: Verify checkpoint updates

After a successful run, confirm checkpointing updated correctly.

What to verify:

- `import_source_checkpoints` has a new or updated record
- the checkpoint advanced only after successful recent-import source completion
- the checkpoint timestamp lines up with the imported window

This confirms future recent-import runs can resume correctly.

## Step 11: Verify SQLite state directly

Inspect the local SQLite database after the run.

Tables to verify:

- `import_runs`
- `import_run_items`
- `import_source_files`
- `import_source_rows`
- `bid_investigations`
- `import_source_checkpoints`

Expected behavior:

- `import_runs` contains source-stage progression and diagnostics
- `import_run_items` contains the queued deduped bid IDs
- `import_source_files` and `import_source_rows` reflect the source data that was downloaded and parsed
- `bid_investigations` contains fetched or reused investigations
- any data that was previously missing from SQLite has now been inserted
- `import_source_checkpoints` reflects successful source completion

## Step 12: Verify the UI reflects reality

Open the app in the browser and confirm the same run visually.

What to verify:

- the run appears in the investigations UI
- stage labels are correct
- progress counts are correct
- diagnostics are visible
- imported items and investigations appear as expected
- nothing looks disconnected between API state and UI state

## Step 13: Classify the outcome

### Successful integration

All of these are true:

- Ringba export was created
- the export was polled successfully
- ZIP and CSV processing succeeded
- bid IDs were inserted
- investigations persisted
- no expected Ringba data is missing from SQLite
- checkpoint updated

### Working integration, but no recent bids

All of these are true:

- source stages succeeded
- no rows or no usable bid IDs were returned
- no downstream investigations were created because there was nothing to process

This is not a failure.

### Partial failure

Examples:

- export creation failed
- export polling never completed
- ZIP or CSV parsing failed
- items were parsed but not inserted
- items were inserted but investigations never processed
- Ringba returned data but SQLite still has missing records after processing

This needs targeted debugging.

## Step 14: If it fails, debug in the right order

Use this order so you do not chase the wrong issue.

### If export creation fails

Check:

- `lastError`
- `sourceMetadata.diagnostics`
- Ringba credentials
- account ID
- API base URL override

### If export succeeds but no rows appear

Check:

- whether there were actually bids in the last 5 minutes
- whether this is a real no-recent-bids case
- then retry with a 15-minute window

### If parsing succeeds but no items are inserted

Check:

- bid ID column detection
- dedupe logic
- item insertion logic
- any safety caps or guardrails

### If items exist but investigations do not

Check:

- item claiming
- downstream processing
- the Ringba bid-detail fetch path
- investigation persistence logic

### If Ringba data exists but SQLite is still missing rows

Check:

- whether `import_source_rows` is missing parsed source data
- whether `import_run_items` counts are lower than the deduped Ringba result
- whether processing stopped before all queued items completed
- whether failed or interrupted runs need to be resumed to backfill the missing data

## Step 15: Repeat once with a larger window

If the 5-minute test succeeds or cleanly returns no rows, run one more validation with a larger window.

Use:

- `15` minutes
- optionally `60` minutes if needed

This helps distinguish:

- true integration issues
- low recent volume
- scale or parsing issues
- backfill gaps that only show up when more Ringba data is returned

If the larger-window run returns additional Ringba data that is not already present in SQLite:

- let the run finish completely
- verify the newly returned data is now inserted into SQLite
- if needed, repeat with another safe window until the missing historical slice has been backfilled

## Step 16: Optional schedule verification

Only after manual recent-import works, test the scheduled flow.

What to do:

- create or reuse an existing schedule
- trigger it manually
- confirm it creates and processes a scheduled run correctly

Do not test scheduling first.

## Step 17: Record the results

At the end, write down:

- whether Ringba credentials worked
- whether recent import worked end to end
- whether SQLite persisted everything correctly
- whether any previously missing Ringba data had to be backfilled into SQLite
- whether checkpointing updated
- whether the UI matched backend state
- what failed, if anything
- the smallest reproducible failing step, if applicable

## Commands To Run

### Terminal 1

```bash
pnpm install
pnpm db:reset
pnpm db:migrate
pnpm dev
```

### Terminal 2

```bash
curl -X POST http://localhost:3000/api/import-runs/ringba-recent \
  -H "Content-Type: application/json" \
  -d '{"windowMinutes":5,"forceRefresh":false}'
```

Then poll:

```bash
curl http://localhost:3000/api/import-runs/<importRunId>
```

## What Success Looks Like

You want to see all of this:

- app starts cleanly
- a Ringba import run is created
- `sourceStage` advances correctly
- `exportJobId` is present
- source diagnostics populate
- bid IDs are parsed and inserted
- investigations persist into SQLite
- checkpoint updates on success
- the UI reflects the same state as the API
