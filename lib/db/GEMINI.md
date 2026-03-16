# Database Layer (`lib/db/`)

This directory contains the database schema and low-level persistence operations using Drizzle ORM with SQLite.

## Core Schema (`lib/db/schema/index.ts`)

- **`bidInvestigations`:** The primary table for storing analysis of a bid ID. Contains normalized data from the Ringba API and diagnosis results.
- **`importRuns`:** Manages the state and progress of bulk import operations.
- **`importRunItems`:** Individual bid IDs within an `importRun`.
- **`importSchedules`:** Configurations for recurring Ringba API imports.
- **`importSourceRows`:** Raw data from CSV uploads, stored for later reference or direct import.
- **`importOpsEvents`:** Operational logs and events related to scheduled runs and system health.

## Conventions

- **ID Generation:** Use unique string IDs (e.g., UUIDs or ULIDs) for all primary keys.
- **Timestamps:** Use ISO 8601 strings. Always include `createdAt` and `updatedAt`. Use the `nowIso` helper for default values.
- **Foreign Keys:** Use Drizzle's `.references()` and define appropriate `onDelete` behaviors (usually `cascade` or `set null`).
- **Indices:** Always add indices for columns used in filters or join conditions (e.g., `status`, `importRunId`, `bidId`).
- **JSON Columns:** Use `{ mode: "json" }` for storing complex objects or arrays (e.g., `rawTraceJson`, `evidenceJson`).

## Writing Queries

- **Separation of Concerns:** Business logic belongs in the service layer (`lib/`). Database files should focus on efficient CRUD and state transitions.
- **Atomicity:** Use Drizzle transactions (`db.transaction`) for multi-step updates that must succeed or fail together (e.g., claiming a run and its items).
- **Concurrency Control:** Use "claim" patterns (e.g., updating a `status` from `queued` to `processing` in a single SQL statement) to prevent race conditions during concurrent processing.

## Migrations

- Manage schema changes via `drizzle-kit`.
- Generate migrations: `npm run db:generate`
- Run migrations: `npm run db:migrate`
- Use the `scripts/` directory for manual maintenance tasks.
