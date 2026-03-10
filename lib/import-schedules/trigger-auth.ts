import "server-only";

import { timingSafeEqual } from "node:crypto";

export interface ImportSchedulesTriggerAuthResult {
  ok: boolean;
  reason?: string;
  authMode: "secret" | "development-open";
  requestSource: string;
}

function readPresentedSecret(request: Request) {
  const explicitHeader = request.headers.get("x-import-schedules-trigger-secret");
  if (explicitHeader) {
    return explicitHeader;
  }

  const authorization = request.headers.get("authorization");
  if (authorization?.startsWith("Bearer ")) {
    return authorization.slice("Bearer ".length).trim();
  }

  return null;
}

function matchesSecret(expected: string, actual: string) {
  const expectedBuffer = Buffer.from(expected);
  const actualBuffer = Buffer.from(actual);

  if (expectedBuffer.length !== actualBuffer.length) {
    return false;
  }

  return timingSafeEqual(expectedBuffer, actualBuffer);
}

export function authorizeImportSchedulesTrigger(
  request: Request,
): ImportSchedulesTriggerAuthResult {
  const configuredSecret = process.env.IMPORT_SCHEDULES_TRIGGER_SECRET?.trim() ?? "";
  const requestSource =
    request.headers.get("x-vercel-cron") ??
    request.headers.get("user-agent") ??
    "unknown";

  if (!configuredSecret) {
    if (process.env.NODE_ENV === "production") {
      return {
        ok: false,
        reason: "Import schedule trigger secret is not configured.",
        authMode: "secret",
        requestSource,
      };
    }

    return {
      ok: true,
      authMode: "development-open",
      requestSource,
    };
  }

  const presentedSecret = readPresentedSecret(request);
  if (!presentedSecret || !matchesSecret(configuredSecret, presentedSecret)) {
    return {
      ok: false,
      reason: "Unauthorized trigger request.",
      authMode: "secret",
      requestSource,
    };
  }

  return {
    ok: true,
    authMode: "secret",
    requestSource,
  };
}
