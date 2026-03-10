import { afterEach, describe, expect, it, vi } from "vitest";

import { authorizeImportSchedulesTrigger } from "@/lib/import-schedules/trigger-auth";

function buildRequest(headers: Record<string, string> = {}) {
  return new Request("http://localhost/api/import-schedules/trigger", {
    method: "POST",
    headers,
  });
}

describe("import schedule trigger auth", () => {
  const originalNodeEnv = process.env.NODE_ENV;
  const originalSecret = process.env.IMPORT_SCHEDULES_TRIGGER_SECRET;

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.stubEnv("NODE_ENV", originalNodeEnv);
    if (originalSecret === undefined) {
      delete process.env.IMPORT_SCHEDULES_TRIGGER_SECRET;
    } else {
      vi.stubEnv("IMPORT_SCHEDULES_TRIGGER_SECRET", originalSecret);
    }
  });

  it("allows local development when no secret is configured", () => {
    vi.stubEnv("NODE_ENV", "development");
    delete process.env.IMPORT_SCHEDULES_TRIGGER_SECRET;

    const result = authorizeImportSchedulesTrigger(buildRequest());

    expect(result.ok).toBe(true);
    expect(result.authMode).toBe("development-open");
  });

  it("rejects production requests when no secret is configured", () => {
    vi.stubEnv("NODE_ENV", "production");
    delete process.env.IMPORT_SCHEDULES_TRIGGER_SECRET;

    const result = authorizeImportSchedulesTrigger(buildRequest());

    expect(result.ok).toBe(false);
    expect(result.reason).toBe("Import schedule trigger secret is not configured.");
  });

  it("accepts a matching explicit trigger header", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("IMPORT_SCHEDULES_TRIGGER_SECRET", "top-secret");

    const result = authorizeImportSchedulesTrigger(
      buildRequest({
        "x-import-schedules-trigger-secret": "top-secret",
      }),
    );

    expect(result.ok).toBe(true);
    expect(result.authMode).toBe("secret");
  });

  it("rejects a mismatched bearer token", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("IMPORT_SCHEDULES_TRIGGER_SECRET", "top-secret");

    const result = authorizeImportSchedulesTrigger(
      buildRequest({
        authorization: "Bearer wrong-secret",
      }),
    );

    expect(result.ok).toBe(false);
    expect(result.reason).toBe("Unauthorized trigger request.");
  });
});
