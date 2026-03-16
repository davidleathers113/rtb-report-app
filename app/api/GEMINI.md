# API Routes (`app/api/`)

This directory contains the Next.js Route Handlers for the application.

## Conventions

- **Runtime:** Explicitly set `export const runtime = "nodejs";` in all route files.
- **Methods:** Use standard HTTP methods (GET, POST, PATCH, DELETE) as appropriate.
- **Validation:** Always validate incoming request payloads and query parameters using Zod schemas defined in `lib/validation/`.
- **Responses:** Use `NextResponse.json()` to return structured data and appropriate status codes.

## Error Handling

- **Try-Catch:** Wrap all route logic in a `try...catch` block.
- **Consistent Format:** Return errors in a consistent JSON format: `{ "error": "Message" }`.
- **Status Codes:** Use appropriate HTTP status codes (e.g., 400 for validation failures, 404 for missing resources, 500 for internal server errors).

## Common Route Patterns

- **Bulk Investigations (`app/api/investigations/bulk/`):** POST request to create a new `importRun` for a list of bid IDs. Returns a 202 status code and the created run detail.
- **Import Run Processing (`app/api/import-runs/[id]/process/`):** POST request that triggers the background processing of a specific import run.
- **Resource List/Detail:** GET requests to list or retrieve individual entities (e.g., investigations, schedules, ops events). Use Zod for pagination and filtering.

## Authentication & Security

- **Server-Only:** Ensure that API routes only call services that are marked as `server-only`.
- **Secrets:** Use environment variables for sensitive API keys or trigger secrets. Never log or return these in API responses.
- **Trigger Secret:** Some routes (e.g., `app/api/import-schedules/trigger/`) may require a secret token passed in a header (e.g., `x-import-schedules-trigger-secret`) for automated cron jobs.
