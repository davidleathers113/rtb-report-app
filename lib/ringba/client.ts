import "server-only";

import { withHistoricalRingbaBudget } from "@/lib/ringba/budget";
import { safeJsonParse } from "@/lib/utils/json";

export interface RingbaConfig {
  accountId: string;
  apiToken: string;
  apiBaseUrl: string;
  authScheme: string;
}

export interface RingbaFetchResult {
  bidId: string;
  requestUrl: string;
  fetchedAt: string;
  httpStatusCode: number | null;
  ok: boolean;
  rawBody: Record<string, unknown> | string | null;
  responseHeaders: Record<string, string>;
  transportError: string | null;
  errorKind:
    | "none"
    | "transport_error"
    | "not_found"
    | "rate_limited"
    | "server_error"
    | "client_error";
  latencyMs: number;
  attemptCount: number;
  retryAfterMs: number | null;
}

export interface FetchRingbaBidDetailOptions {
  budgetProfile?: "default" | "historical_backfill";
}

function getRequiredEnv(name: string) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function getAuthScheme() {
  const configured = process.env.RINGBA_AUTH_SCHEME?.trim();

  if (!configured) {
    return "Token";
  }

  // Ringba API access tokens use the Token scheme. Normalize legacy Bearer
  // configs so existing local environments keep working after the auth change.
  if (configured.toLowerCase() === "bearer") {
    return "Token";
  }

  return configured;
}

export function getRingbaConfig(): RingbaConfig {
  return {
    accountId: getRequiredEnv("RINGBA_ACCOUNT_ID"),
    apiToken: getRequiredEnv("RINGBA_API_TOKEN"),
    apiBaseUrl: process.env.RINGBA_API_BASE_URL ?? "https://api.ringba.com",
    authScheme: getAuthScheme(),
  };
}

function mapHeaders(headers: Headers) {
  const entries: Record<string, string> = {};

  headers.forEach((value, key) => {
    entries[key] = value;
  });

  return entries;
}

function readPositiveIntEnv(name: string, fallback: number) {
  const rawValue = process.env[name]?.trim();
  if (!rawValue) {
    return fallback;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }

  return Math.trunc(parsed);
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function randomBetween(minimum: number, maximum: number) {
  if (maximum <= minimum) {
    return minimum;
  }

  return minimum + Math.floor(Math.random() * (maximum - minimum + 1));
}

function parseRetryAfterMs(
  headers: Headers | Record<string, string>,
  referenceTimeMs: number,
) {
  const headerValue =
    headers instanceof Headers
      ? headers.get("retry-after")
      : (headers["retry-after"] ?? headers["Retry-After"] ?? null);

  if (!headerValue) {
    return null;
  }

  const numericValue = Number(headerValue);
  if (Number.isFinite(numericValue) && numericValue >= 0) {
    return Math.trunc(numericValue * 1000);
  }

  const retryDateMs = Date.parse(headerValue);
  if (Number.isNaN(retryDateMs)) {
    return null;
  }

  return Math.max(0, retryDateMs - referenceTimeMs);
}

function mapErrorKind(input: { ok: boolean; httpStatusCode: number | null; transportError: string | null }) {
  if (input.transportError) {
    return "transport_error" as const;
  }

  if (input.ok) {
    return "none" as const;
  }

  if (input.httpStatusCode === 404) {
    return "not_found" as const;
  }

  if (input.httpStatusCode === 429) {
    return "rate_limited" as const;
  }

  if (
    input.httpStatusCode !== null &&
    input.httpStatusCode >= 500 &&
    input.httpStatusCode <= 599
  ) {
    return "server_error" as const;
  }

  return "client_error" as const;
}

function shouldRetryResult(result: RingbaFetchResult) {
  return (
    result.errorKind === "transport_error" ||
    result.errorKind === "rate_limited" ||
    result.errorKind === "server_error"
  );
}

function calculateRetryDelayMs(attemptNumber: number, retryAfterMs: number | null) {
  if (retryAfterMs !== null) {
    return retryAfterMs;
  }

  const exponentialBackoffMs = Math.min(30000, 1000 * 2 ** Math.max(0, attemptNumber - 1));
  return exponentialBackoffMs + randomBetween(250, 750);
}

async function performTimedRingbaFetch(
  requestUrl: string,
  headers: Record<string, string>,
  timeoutMs: number,
) {
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetch(requestUrl, {
      method: "GET",
      headers,
      cache: "no-store",
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function performFetchAttempt(
  bidId: string,
  requestUrl: string,
  headers: Record<string, string>,
  timeoutMs: number,
) {
  const startedAtMs = Date.now();

  try {
    const response = await performTimedRingbaFetch(requestUrl, headers, timeoutMs);
    const responseText = await response.text();
    const parsedBody = responseText ? safeJsonParse(responseText) : null;
    const responseHeaders = mapHeaders(response.headers);
    const fetchedAt = new Date().toISOString();
    const httpStatusCode = response.status;
    const retryAfterMs = parseRetryAfterMs(response.headers, startedAtMs);
    const transportError = null;
    const errorKind = mapErrorKind({
      ok: response.ok,
      httpStatusCode,
      transportError,
    });

    return {
      bidId,
      requestUrl,
      fetchedAt,
      httpStatusCode,
      ok: response.ok,
      rawBody:
        typeof parsedBody === "string" || parsedBody === null
          ? parsedBody
          : (parsedBody as Record<string, unknown>),
      responseHeaders,
      transportError,
      errorKind,
      latencyMs: Date.now() - startedAtMs,
      attemptCount: 1,
      retryAfterMs,
    } satisfies RingbaFetchResult;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown Ringba transport error";
    const fetchedAt = new Date().toISOString();

    return {
      bidId,
      requestUrl,
      fetchedAt,
      httpStatusCode: null,
      ok: false,
      rawBody: {
        error: "ringba_transport_error",
        message,
      },
      responseHeaders: {},
      transportError: message,
      errorKind: "transport_error",
      latencyMs: Date.now() - startedAtMs,
      attemptCount: 1,
      retryAfterMs: null,
    } satisfies RingbaFetchResult;
  }
}

export async function fetchRingbaBidDetail(
  bidId: string,
  options?: FetchRingbaBidDetailOptions,
): Promise<RingbaFetchResult> {
  const config = getRingbaConfig();
  const requestUrl = `${config.apiBaseUrl}/v2/${config.accountId}/rtb/bid/${encodeURIComponent(
    bidId,
  )}`;
  const timeoutMs = readPositiveIntEnv("RINGBA_BID_DETAIL_TIMEOUT_MS", 15000);
  const useHistoricalBudget = options?.budgetProfile === "historical_backfill";
  const maxAttempts = useHistoricalBudget
    ? Math.max(1, 1 + readPositiveIntEnv("RINGBA_BACKFILL_MAX_RETRIES", 3))
    : 1;
  const headers = {
    Accept: "application/json",
    Authorization: `${config.authScheme} ${config.apiToken}`,
  };

  let lastResult: RingbaFetchResult | null = null;

  for (let attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber += 1) {
    const executeFetch = async () => {
      return performFetchAttempt(bidId, requestUrl, headers, timeoutMs);
    };
    const result = useHistoricalBudget
      ? await withHistoricalRingbaBudget(executeFetch)
      : await executeFetch();
    lastResult = {
      ...result,
      attemptCount: attemptNumber,
    };

    if (!useHistoricalBudget || !shouldRetryResult(lastResult) || attemptNumber >= maxAttempts) {
      return lastResult;
    }

    await sleep(calculateRetryDelayMs(attemptNumber, lastResult.retryAfterMs));
  }

  if (!lastResult) {
    throw new Error(`Unable to fetch Ringba bid detail for ${bidId}.`);
  }

  return lastResult;
}
