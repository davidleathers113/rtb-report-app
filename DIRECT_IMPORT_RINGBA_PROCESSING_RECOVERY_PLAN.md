# Direct Import Ringba Processing Recovery Plan

## Goal

Resume the stalled direct CSV investigation runs safely, keep them progressing without relying on an open browser tab, and respect Ringba's documented account-level API limits while they run.

## Executive Summary

The three CSV imports stopped processing because `csv_direct_import` runs only advance when the UI keeps calling `POST /api/import-runs/[importRunId]/process` for the currently selected `activeRun`.

That means the system currently has:

- durable CSV row staging in SQLite
- durable import run state in SQLite
- no autonomous worker for direct-import investigation processing

When a newer run became the active UI run, the older runs stopped receiving `/process` calls. They remained in `running` / `processing`, but no longer had an active processor lease and no longer advanced.

The best course of action is:

1. Do not try to drive these runs from the browser.
2. Resume them from a single background processor loop.
3. Apply one global Ringba request budget for direct-import investigations.
4. Process only one direct-import run at a time until the budgeted worker exists.

## Exact Stop Cause

### Runtime Evidence

The stalled runs currently look like this in SQLite:

- `3daf6294-4a5b-4a48-a212-b5d2e31bc120` for `3-11-26.csv`
- `0166f544-f630-41d2-b3f8-cbcd56083708` for `3-12-26.csv`
- `775b25a6-a95a-4c5d-a0bf-d5caf3bc165f` for `3-13-26.csv`

All three are still:

- `status = running`
- `source_stage = processing`
- `processor_lease_expires_at = null`
- no `running` items
- almost all items still `queued`

That combination means they are not actively being worked by any processor right now.

### Code Path That Explains It

The direct-import processing loop is UI-driven:

- `components/investigations/bulk-investigation-client.tsx`
  - the polling `useEffect()` only runs for `activeRunId`
  - it repeatedly calls `POST /api/import-runs/[importRunId]/process`
  - switching `activeRun` switches which run is being driven

- `app/api/import-runs/[importRunId]/process/route.ts`
  - only processes a run when explicitly called

- `lib/import-runs/service.ts`
  - `processImportRun()` processes a bounded batch, then returns
  - it does not schedule itself again

- `lib/db/import-runs.ts`
  - `claimImportRunProcessing()` uses a lease, but the lease only matters if something keeps invoking the route

### Why Older Runs Stopped

The UI can only drive one active run at a time. Once newer CSV imports were created, `activeRun` moved to the newer run, and polling for the older runs stopped.

There is no server-side daemon, queue worker, or schedule-backed loop for `csv_direct_import`, so the older runs simply stayed half-done.

## Ringba API Research

### Documented Limits Relevant Here

Ringba's support documentation explicitly lists:

- `Get RTB Bid Log` (`/rtb/bid/{bid_id}`): `150 requests/minute`
- `Get Details about Specific Calls`: `200 requests/minute`
- logs/reporting features: `5 requests/second` or `80 requests/minute`
- for endpoints without explicit limits, Ringba recommends load testing and adding delays as needed

This project fetches bid details from:

- `lib/ringba/client.ts`
- `GET /v2/{accountId}/rtb/bid/{bid_id}`

That maps directly to Ringba's documented `Get RTB Bid Log` limit.

### Current Project Behavior vs Ringba Limits

The project already has a budget gate for historical backfill:

- `lib/ringba/budget.ts`
- default historical budget:
  - concurrency `1`
  - `30 requests/minute`
  - jitter between requests

However, direct CSV imports do **not** use that stricter budget:

- `lib/investigations/service.ts`
  - only `historical_ringba_backfill` gets `budgetProfile: "historical_backfill"`
  - `csv_direct_import` uses `budgetProfile: "default"`

So direct-import investigation processing currently has:

- retries and `Retry-After` handling in `lib/ringba/client.ts`
- no steady-state global pacing for large direct-import runs

## Best Course of Action

### Immediate Operational Recommendation

Resume the runs with a single background processor, one run at a time, outside the browser.

Use this ordering:

1. `3-11-26.csv`
2. `3-12-26.csv`
3. `3-13-26.csv`

Why:

- it is the safest operational model against Ringba's account-level minute bucket
- it avoids multiple runs competing for the same limit
- it removes dependence on browser tabs, rendering, or `activeRun`

### Recommended Safe Throughput

Use a global target of `100-120 requests/minute`, concurrency `1`.

Why not `150/minute` exactly:

- the documented limit is account-wide
- other tabs/users/API activity may consume the same bucket
- staying below the ceiling reduces 429 churn
- the client already respects `Retry-After`, but avoiding 429s is better than recovering from them

### Practical Recovery Target

Recommended starting target:

- concurrency: `1`
- budget: `100 requests/minute`
- jitter: `250-1000 ms`

This is conservative enough to respect Ringba's published `150/minute` limit while still progressing far faster than the historical-backfill default of `30/minute`.

## What To Change In Code

### 1. Decouple Direct-Import Processing From The Browser

Add a background runner for `csv_direct_import` similar in spirit to how schedules drive other import sources.

Preferred options:

- add a small Node script that repeatedly calls `processImportRun()` for a target run list
- or add a server-side worker endpoint / cron-compatible trigger for queued direct-import runs

Important requirement:

- the processor must not depend on `activeRun` in the React client

### 2. Reuse The Existing Ringba Budget Gate For Direct Imports

Extend `lib/investigations/service.ts` so `csv_direct_import` can use a throttled Ringba budget profile too, not just `historical_ringba_backfill`.

Recommended shape:

- rename the current historical budget to a more general Ringba budget utility
- support profiles like:
  - `historical_backfill`
  - `direct_csv_bulk`
  - `default`

Recommended initial `direct_csv_bulk` config:

- concurrency `1`
- `100 requests/minute`
- jitter `250-1000 ms`

### 3. Add A Real Queue-Drain Strategy

For very large imports, the run processor should:

- claim the run
- process bounded batches
- renew progress continuously
- release the lease cleanly
- continue until terminal status without needing the UI

### 4. Surface Processing Ownership In The UI

The UI should show whether a run is:

- browser-driven
- background-driven
- stalled because no processor is attached

That would have made this failure mode obvious immediately.

## Immediate Recovery Plan

### Phase 1. Implement The Safer Processing Path

1. Add a dedicated background processor for direct-import runs.
2. Route direct-import Ringba fetches through a capped budget profile.
3. Make that processor handle one direct-import run at a time.

### Phase 2. Resume Existing Runs

1. Resume `3-11-26.csv`
2. Wait for terminal completion
3. Resume `3-12-26.csv`
4. Wait for terminal completion
5. Resume `3-13-26.csv`

### Phase 3. Monitor For Rate Limiting

Track during execution:

- Ringba HTTP `429`
- `Retry-After` values
- `errorKind = rate_limited`
- average latency
- completed items per minute

If rate limiting appears:

- drop from `100/min` to `80/min`
- keep concurrency at `1`
- continue from the same queue

## Estimated Runtime Once Fixed

If resumed under a single-worker budget:

- at `100/min`:
  - about `2,016` minutes for `201,554` remaining items
  - about `33.6 hours`

- at `120/min`:
  - about `1,680` minutes
  - about `28 hours`

- at the historical-backfill default `30/min`:
  - about `112 hours`
  - about `4.7 days`

So the best safe target is not "as fast as the browser can post `/process`" but "one controlled worker at `100-120/min`".

## Why This Is Better Than Leaving The Current Flow Alone

The current behavior has two failure modes:

1. processing stops when the browser changes focus to another run
2. direct CSV imports can potentially hit Ringba faster than the documented RTB bid-log limit

The recommended worker-plus-budget model fixes both:

- it keeps runs moving without a tab
- it applies one account-safe Ringba throttle

## Recommended File-Level Follow-Up

Primary change targets:

- `components/investigations/bulk-investigation-client.tsx`
  - stop implying the browser is the processor of record for huge runs
- `lib/investigations/service.ts`
  - add a throttled profile for `csv_direct_import`
- `lib/ringba/budget.ts`
  - generalize beyond historical backfill
- `lib/import-runs/service.ts`
  - add a background-friendly run-drain loop for direct imports
- `app/api/import-runs/[importRunId]/process/route.ts`
  - keep for manual stepping, but do not rely on it as the only execution model

## Recommended Decision

The best course of action is:

- implement a single background processor for direct CSV runs
- throttle direct-import Ringba fetches to `100-120 requests/minute`, concurrency `1`
- resume the three stalled runs sequentially

That is the safest way to continue investigations while respecting Ringba's documented API limitations.

## Sources

- [Ringba: Why am I being rate limited?](https://support.ringba.com/hc/en-us/articles/23785949612695-Why-am-I-being-rate-limited)
- [Ringba API Documentation](https://developers.ringba.com/)
- [Ringba: RTB Error Codes](https://support.ringba.com/hc/en-us/articles/17992477665943-RTB-Error-Codes)
