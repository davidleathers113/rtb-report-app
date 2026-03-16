# RTB Report App (Bid Investigation Console)

This application is a specialized console for investigating Ringba Real-Time Bidding (RTB) traces. It allows operators to analyze bid failures, identify root causes, and manage bulk data imports from various sources.

## Core Tech Stack

- **Framework:** Next.js (App Router, Node.js runtime)
- **Language:** TypeScript
- **Database:** SQLite (via `better-sqlite3`)
- **ORM:** Drizzle ORM
- **Validation:** Zod
- **Styling:** Tailwind CSS (v4)
- **UI Components:** Radix UI primitives
- **Testing:** Vitest

## Architectural Overview

The application follows a service-oriented architecture with a clear separation between the UI, API, and business logic layers:

1.  **API Layer (`app/api/`):** Next.js Route Handlers for external and internal requests.
2.  **Service Layer (`lib/`):** Domain-specific logic (Imports, Investigations, Diagnostics).
3.  **Database Layer (`lib/db/`):** Drizzle schema and low-level persistence operations.
4.  **UI Layer (`components/`):** React components, largely focused on complex state management for bulk operations.

## Engineering Standards

- **Server-Only Enforcement:** Use the `server-only` package in all `lib/` files that interact with the database or sensitive environment variables to prevent accidental leakage to the client.
- **Type Safety:** Always prefer explicit types or Drizzle-inferred types. Avoid `any`.
- **Validation:** All external inputs (API requests, file uploads) must be validated using Zod schemas defined in `lib/validation/`.
- **Concurrency & State:** Bulk operations (Import Runs) are asynchronous and stateful. Use the established claim/lease mechanisms in the database to prevent race conditions.
- **Timestamps:** Use ISO 8601 strings for timestamps. In the database, use the `nowIso` helper: `sql`(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))``.

## Key Modules

- `lib/import-runs/`: Manages the lifecycle of bulk data ingestion.
- `lib/investigations/`: Orchestrates the analysis of individual bid IDs.
- `lib/diagnostics/`: A rule-based engine for automated root-cause analysis.
- `lib/ringba/`: Integration with the Ringba RTB API.

For specific instructions on these modules, refer to the `GEMINI.md` files in their respective directories.
