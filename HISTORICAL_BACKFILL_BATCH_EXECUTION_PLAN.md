# Historical Backfill Batch Execution Plan

## Goal

Enrich the remaining `116,437` `csv_only` investigations in SQLite by running the historical Ringba backfill in controlled batches, while reporting progress in three primary outcome buckets:

- `success`
- `not_found`
- `failed`

This plan assumes CSV ingestion is already complete for:

- `3-5-26.csv`
- `3-6-26.csv`
- `3-9-26.csv`

and that these bid IDs are already materialized into `bid_investigations`.

## Current Baseline

Current SQLite state at the start of this plan:

- `csv_direct_source_rows`: `116,745`
- `csv_direct_distinct_bid_ids`: `116,455`
- `bid_investigations`: `116,610`
- `csv_only`: `116,437`
- `enriched`: `173`

Notes:

- `3-5-26.csv` was imported twice, but only has `290` distinct bid IDs, so it should not be imported again.
- The remaining work is API enrichment only.
- Historical backfill now records fetch latency correctly for successful scheduled and manual runs.

## Success Criteria

The backfill is complete when all of the following are true:

1. `csv_only = 0`
2. Every CSV bid ID from the three source files is represented in `bid_investigations`
3. Progress reporting is available for:
   - `success`
   - `not_found`
   - `failed`
4. Rate limiting and failure rates stay within acceptable bounds during execution

## Non-Goals

- Re-importing any CSV files
- Refactoring the historical backfill architecture
- Increasing concurrency or request budgets beyond the existing safe historical backfill controls

## Execution Strategy

### Phase 1: Establish a clean operational baseline

Before running any large backfill:

- confirm there are no active historical runs already in progress
- capture baseline counts for:
  - `csv_only`
  - `enriched`
  - `not_found`
  - `failed`
- capture the current oldest and newest `csv_only` bid timestamps
- verify the current historical schedule configuration:
  - `backfillLimit`
  - `backfillSort`
  - throttle profile
  - pause state

Deliverable:

- a baseline snapshot saved in the run notes or shared as an operator report before batch execution starts

### Phase 2: Run manual controlled batches first

Do not start with a fully automated long-running schedule loop.

Use repeated manual or operator-triggered historical backfill runs with a fixed batch size so progress can be inspected between runs.

Recommended initial batch size:

- `250` if the API remains healthy

Fallback batch sizes:

- `100` if latency or rate-limit behavior degrades
- `50` if upstream instability appears

Run ordering:

- default to `newest_first`

Reason:

- recent bids are more likely to enrich successfully
- this gives faster signal on API health and progress quality
- it reduces the chance of spending early budget on very old unavailable bids

Each batch should:

1. create a historical backfill run against `csv_only` investigations only
2. process the run to completion
3. record the run outcome totals
4. compare the updated global counts before starting the next batch

### Phase 3: Promote to repeated schedule-driven batches

After a few healthy manual batches:

- enable the historical schedule to continue in small controlled chunks
- keep `maxConcurrentRuns = 1`
- keep the existing historical throttle profile
- keep the schedule paused or disabled any time failure patterns worsen

Recommended schedule settings:

- source type: `historical_ringba_backfill`
- sort: `newest_first`
- limit: `250`
- max concurrent runs: `1`
- leave overlap behavior unchanged

Run cadence should remain conservative until real-world rates are established across more of the backlog.

## Reporting Model

Every run should be summarized using these outcome buckets:

- `success`
  Definition: investigations moved to `enriched`

- `not_found`
  Definition: Ringba returned a durable missing-result outcome and the investigation moved to `not_found`

- `failed`
  Definition: transport, server, rate-limit, or other non-terminal failure that did not enrich the bid

For each run, report:

- run id
- trigger type: `manual` or `scheduled`
- candidate count selected
- attempted count
- success count
- not found count
- failed count
- reused count
- rate-limited count
- server error count
- average fetch latency
- latency sample count
- total remaining `csv_only`

For cumulative progress, report:

- initial `csv_only`
- current `csv_only`
- total converted to `enriched`
- total converted to `not_found`
- total currently `failed`
- percent of backlog processed

## Batch Control Rules

### Continue batching when

- `failedCount` remains low
- `rateLimitedCount` remains near zero
- `averageFetchLatencyMs` remains stable
- the majority of attempted items resolve to `success` or `not_found`

### Slow down when

- average latency rises materially across consecutive runs
- `rateLimitedCount` appears repeatedly
- server errors become persistent

Action:

- reduce batch size from `250` to `100`
- if needed, reduce again from `100` to `50`

### Pause immediately when

- repeated `429` responses appear across consecutive batches
- server-side failures dominate a batch
- schedule auto-pause triggers
- import runs stop making progress

Action:

- stop further batch execution
- inspect the most recent run metadata and failure counts
- resume only after confirming the upstream is healthy again

## Recommended Rollout Sequence

### Stage A: Validation batches

Run:

- `3` manual batches of `100`

Review after each:

- `success / not_found / failed`
- latency
- remaining backlog

If stable, move to Stage B.

### Stage B: Steady-state batches

Run:

- repeated batches of `250`

Review every:

- `1,000` attempted items

Checkpoint report should include:

- total attempted
- total success
- total not found
- total failed
- current backlog remaining

### Stage C: Tail cleanup

When backlog drops below `5,000`:

- reduce batch size to `100`

When backlog drops below `1,000`:

- reduce batch size to `50`

Reason:

- smaller tail batches make it easier to inspect anomalies, retries, and any stubborn `failed` records

## Verification Queries

Use these checks between stages.

### Global enrichment counts

```sql
select enrichment_state, count(*)
from bid_investigations
group by enrichment_state
order by enrichment_state;
```

### CSV-only backlog remaining

```sql
select count(*)
from bid_investigations
where enrichment_state = 'csv_only';
```

### Historical backfill run summary

```sql
select id, status, source_type, created_at, completed_at
from import_runs
where source_type = 'historical_ringba_backfill'
order by created_at desc
limit 20;
```

### Oldest remaining csv_only bids

```sql
select bid_id, bid_dt
from bid_investigations
where enrichment_state = 'csv_only'
order by bid_dt asc
limit 20;
```

## Operator Runbook

For each batch cycle:

1. record pre-run counts
2. create the historical backfill run
3. process it to completion
4. capture run metrics from `sourceMetadata.metrics`
5. capture updated global enrichment counts
6. append a progress note with:
   - run id
   - batch size
   - success
   - not found
   - failed
   - remaining backlog
7. decide whether to continue, slow down, or pause

## Expected End State

At the end of execution:

- all CSV bids from `3-5-26.csv`, `3-6-26.csv`, and `3-9-26.csv` will have gone through the API completion path
- remaining unresolved bids will be explicitly categorized as:
  - `enriched`
  - `not_found`
  - `failed`
- no residual historical bids should remain in `csv_only`

## Immediate Next Action

Start with a manual historical backfill run of `100` items and capture the first true backlog report in this format:

- starting backlog
- attempted
- success
- not found
- failed
- rate limited
- server error
- average fetch latency
- remaining backlog
