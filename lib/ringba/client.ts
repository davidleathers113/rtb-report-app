import "server-only";

import { safeJsonParse } from "@/lib/utils/json";

export interface RingbaFetchResult {
  bidId: string;
  requestUrl: string;
  fetchedAt: string;
  httpStatusCode: number | null;
  ok: boolean;
  rawBody: Record<string, unknown> | string | null;
  responseHeaders: Record<string, string>;
  transportError: string | null;
}

function getRequiredEnv(name: string) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function getRingbaConfig() {
  return {
    accountId: getRequiredEnv("RINGBA_ACCOUNT_ID"),
    apiToken: getRequiredEnv("RINGBA_API_TOKEN"),
    apiBaseUrl: process.env.RINGBA_API_BASE_URL ?? "https://api.ringba.com",
    authScheme: process.env.RINGBA_AUTH_SCHEME ?? "Bearer",
  };
}

function mapHeaders(headers: Headers) {
  const entries: Record<string, string> = {};

  headers.forEach((value, key) => {
    entries[key] = value;
  });

  return entries;
}

export async function fetchRingbaBidDetail(
  bidId: string,
): Promise<RingbaFetchResult> {
  const config = getRingbaConfig();
  const requestUrl = `${config.apiBaseUrl}/v2/${config.accountId}/rtb/bid/${encodeURIComponent(
    bidId,
  )}`;

  try {
    const response = await fetch(requestUrl, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `${config.authScheme} ${config.apiToken}`,
      },
      cache: "no-store",
    });

    const responseText = await response.text();
    const parsedBody = responseText ? safeJsonParse(responseText) : null;

    return {
      bidId,
      requestUrl,
      fetchedAt: new Date().toISOString(),
      httpStatusCode: response.status,
      ok: response.ok,
      rawBody:
        typeof parsedBody === "string" || parsedBody === null
          ? parsedBody
          : (parsedBody as Record<string, unknown>),
      responseHeaders: mapHeaders(response.headers),
      transportError: null,
    };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown Ringba transport error";

    return {
      bidId,
      requestUrl,
      fetchedAt: new Date().toISOString(),
      httpStatusCode: null,
      ok: false,
      rawBody: {
        error: "ringba_transport_error",
        message,
      },
      responseHeaders: {},
      transportError: message,
    };
  }
}
