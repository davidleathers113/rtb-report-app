# Bid Investigation Console

Internal dashboard for investigating Ringba RTB bid outcomes. Monitor processed bid traces, review root causes, and run bulk or single investigations without working in spreadsheets.

## Tech Stack

- **Framework:** [Next.js](https://nextjs.org) 16 (App Router)
- **Database:** SQLite (`better-sqlite3` + Drizzle ORM)
- **Styling:** Tailwind CSS 4
- **Testing:** Vitest

## Getting Started

### Prerequisites

- Node.js 22+
- pnpm (recommended; project uses `pnpm-lock.yaml`)

### Install & Run

```bash
pnpm install
cp .env.example .env
# Edit .env with your Ringba credentials
pnpm db:migrate
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000).

### Database Setup

Apply Drizzle migrations locally:

```bash
pnpm db:generate
pnpm db:migrate
```

The local database file defaults to `data/bid-investigation-console.sqlite`.

To reset local persistence during development:

```bash
pnpm db:reset
pnpm db:migrate
```

## Scripts

| Command       | Description                    |
|---------------|--------------------------------|
| `pnpm dev`    | Start dev server (port 3000)   |
| `pnpm build`  | Production build               |
| `pnpm start`  | Start production server        |
| `pnpm lint`   | Run ESLint                     |
| `pnpm test`   | Run Vitest tests               |
| `pnpm typecheck` | TypeScript check (no emit)  |
| `pnpm db:generate` | Generate Drizzle SQL migration |
| `pnpm db:migrate` | Apply SQLite migrations     |
| `pnpm db:reset` | Remove local SQLite DB files |

## Features

- **Dashboard** (`/`) – Metrics (investigated, accepted, rejected, zero bid), charts, top root causes/campaigns/publishers, recent investigations
- **Investigations** (`/investigations`) – Bulk and single bid investigation, paste Bid IDs, create async import runs, view stored investigations
- **Import sources** – CSV upload, Ringba recent API
- **Scheduled imports** – Configurable schedules for Ringba recent imports (window, overlap, concurrency)
- **Export** – CSV export of investigations

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BID_CONSOLE_DB_PATH` | No | Override the local SQLite file path |
| `RINGBA_ACCOUNT_ID` | Yes | Ringba account ID |
| `RINGBA_API_TOKEN` | Yes | Ringba API token |
| `RINGBA_API_BASE_URL` | No | Default: `https://api.ringba.com` |
| `RINGBA_AUTH_SCHEME` | No | Default: `Token` |
| `MINIMUM_REVENUE_THRESHOLD` | No | Used by diagnostics rules |
| `IMPORT_SCHEDULES_TRIGGER_SECRET` | Prod | Secret for schedule trigger API; if unset in prod, manual trigger UI is disabled |
| `IMPORT_SCHEDULES_SLACK_WEBHOOK_URL` | No | Slack webhook for schedule notifications |

## Project Structure

```
app/
  page.tsx              # Dashboard
  investigations/       # Bulk/single investigation UI
  api/
    investigations/     # CRUD, export, bulk
    import-runs/        # CSV, Ringba recent, process, rerun
    import-schedules/   # Schedules CRUD, trigger
lib/
  db/                   # SQLite client, schema, and persistence queries
  import-schedules/     # Schedule service, trigger auth, notifications
  ringba/               # Ringba API client
  diagnostics/          # Revenue threshold rules
components/
  dashboard/            # Charts panel
  investigations/       # Table, bulk client
  layout/               # App shell, nav
drizzle/                # SQLite migrations
data/                   # Local SQLite database file
tests/                  # Vitest unit tests
```

## Runtime

This repo is configured for local Node.js execution with a local SQLite database file.
