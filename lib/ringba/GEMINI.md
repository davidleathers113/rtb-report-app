# Ringba API Integration (`lib/ringba/`)

This module manages all communication with the external Ringba RTB API.

## Core Components

- **`client.ts`:** The low-level HTTP client for fetching bid details. Handles authentication, retries, timeouts, and error mapping.
- **`normalize.ts`:** Transforms the raw, complex Ringba API response into a simplified, flat `NormalizedBidData` structure used by the rest of the application.
- **`budget.ts`:** Manages rate limiting and concurrency budgets for different types of API requests (e.g., default vs. historical backfill).

## Configuration

The client relies on several environment variables:
- `RINGBA_ACCOUNT_ID`: The unique identifier for the Ringba account.
- `RINGBA_API_TOKEN`: The API access token.
- `RINGBA_AUTH_SCHEME`: The authorization scheme (defaults to `Token`).
- `RINGBA_API_BASE_URL`: The base URL for the Ringba API (defaults to `https://api.ringba.com`).
- `RINGBA_BID_DETAIL_TIMEOUT_MS`: The timeout for bid detail requests (defaults to 15 seconds).

## Error Mapping

The client maps diverse HTTP and transport errors into a standard `errorKind`:
- `none`: Successful request.
- `transport_error`: Network issues or timeouts.
- `not_found`: The bid ID does not exist in Ringba (HTTP 404).
- `rate_limited`: The API is rate-limiting requests (HTTP 429).
- `server_error`: Ringba internal server errors (HTTP 5xx).
- `client_error`: Other client-side errors (HTTP 4xx).

## Guidelines

- **Server-Only:** This module must only be used on the server to protect API credentials.
- **Retries:** Retries are only enabled for `historical_backfill` budget profiles. Manual and recent imports do not retry to ensure fast UI response times.
- **Backoff:** Use exponential backoff with jitter for retries. Respect the `Retry-After` header if provided by the API.
- **Normalization:** Any changes to the Ringba API response structure should be handled in `normalize.ts` to maintain a stable `NormalizedBidData` interface for the diagnostic engine.
