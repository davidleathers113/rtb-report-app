import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { afterEach, describe, expect, it, vi } from "vitest";
import { zipSync } from "fflate";

import {
  createRtbExportJob,
  createRingbaRecentImportRun,
  extractCsvFromZip,
  parseRtbExportCsv,
  prepareRingbaRecentImportRun,
} from "@/lib/import-runs/ringba-recent";
import { getDb, getSqlite, resetDbClientForTests } from "@/lib/db/client";

describe("Ringba recent import helpers", () => {
  const originalFetch = global.fetch;
  const originalAccountId = process.env.RINGBA_ACCOUNT_ID;
  const originalApiToken = process.env.RINGBA_API_TOKEN;
  const originalApiBaseUrl = process.env.RINGBA_API_BASE_URL;
  const originalAuthScheme = process.env.RINGBA_AUTH_SCHEME;
  const originalDatabasePath = process.env.BID_CONSOLE_DB_PATH;

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    global.fetch = originalFetch;
    resetDbClientForTests();

    if (originalAccountId === undefined) {
      delete process.env.RINGBA_ACCOUNT_ID;
    } else {
      vi.stubEnv("RINGBA_ACCOUNT_ID", originalAccountId);
    }

    if (originalApiToken === undefined) {
      delete process.env.RINGBA_API_TOKEN;
    } else {
      vi.stubEnv("RINGBA_API_TOKEN", originalApiToken);
    }

    if (originalApiBaseUrl === undefined) {
      delete process.env.RINGBA_API_BASE_URL;
    } else {
      vi.stubEnv("RINGBA_API_BASE_URL", originalApiBaseUrl);
    }

    if (originalAuthScheme === undefined) {
      delete process.env.RINGBA_AUTH_SCHEME;
    } else {
      vi.stubEnv("RINGBA_AUTH_SCHEME", originalAuthScheme);
    }

    if (originalDatabasePath === undefined) {
      delete process.env.BID_CONSOLE_DB_PATH;
    } else {
      vi.stubEnv("BID_CONSOLE_DB_PATH", originalDatabasePath);
    }
  });

  function buildJsonResponse(body: unknown, status = 200) {
    return new Response(JSON.stringify(body), {
      status,
      headers: {
        "Content-Type": "application/json",
      },
    });
  }

  function buildZipResponse(csvText: string) {
    const zipBytes = zipSync({
      "ringba-export.csv": new TextEncoder().encode(csvText),
    });

    return new Response(Buffer.from(zipBytes), {
      status: 200,
      headers: {
        "content-length": String(zipBytes.byteLength),
      },
    });
  }

  function setupTestDatabase() {
    const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "ringba-recent-"));
    const databasePath = path.join(tempDirectory, "test.sqlite");
    process.env.BID_CONSOLE_DB_PATH = databasePath;
    resetDbClientForTests();
    migrate(getDb(), {
      migrationsFolder: path.join(process.cwd(), "drizzle"),
    });

    return {
      cleanup() {
        resetDbClientForTests();
        for (const filePath of [databasePath, `${databasePath}-wal`, `${databasePath}-shm`]) {
          if (fs.existsSync(filePath)) {
            fs.rmSync(filePath);
          }
        }
        if (fs.existsSync(tempDirectory)) {
          fs.rmSync(tempDirectory, { recursive: true, force: true });
        }
      },
    };
  }

  it("extracts the CSV file from a Ringba export zip", () => {
    const csvText = "Bid ID,Bid Date\nRTB-1,03/09/2026 11:34:48 PM\n";
    const zipBytes = zipSync({
      "ringba-export.csv": new TextEncoder().encode(csvText),
    });

    const extracted = extractCsvFromZip(zipBytes);

    expect(extracted.fileName).toBe("ringba-export.csv");
    expect(extracted.csvText).toBe(csvText);
  });

  it("parses and dedupes bid ids from the RTB export CSV", () => {
    const result = parseRtbExportCsv(
      [
        "Bid ID,Bid Date,Campaign",
        "RTB-1,03/09/2026 11:34:48 PM,Alpha",
        "RTB-1,03/09/2026 11:34:49 PM,Alpha",
        "RTB-2,03/09/2026 11:35:48 PM,Beta",
      ].join("\n"),
    );

    expect(result.bidIds).toEqual(["RTB-1", "RTB-2"]);
    expect(result.duplicateCount).toBe(1);
    expect(result.rowCount).toBe(3);
    expect(result.latestBidDt).toBe("2026-03-09T23:35:48.000Z");
  });

  it("accepts alternate bid id header variants and BOM-prefixed csv text", () => {
    const result = parseRtbExportCsv(
      [
        "\uFEFFbid_id,bid_dt",
        " RTB-1 ,03/09/2026 11:34:48 PM",
        "RTB-2,03/09/2026 11:35:48 PM",
      ].join("\n"),
    );

    expect(result.bidIds).toEqual(["RTB-1", "RTB-2"]);
    expect(result.dedupedBidIdCount).toBe(2);
  });

  it("returns zero unique bid ids for duplicate-only empty values", () => {
    const result = parseRtbExportCsv(
      [
        "Bid ID,Bid Date",
        " ,03/09/2026 11:34:48 PM",
        " ,03/09/2026 11:35:48 PM",
      ].join("\n"),
    );

    expect(result.bidIds).toEqual([]);
    expect(result.rowCount).toBe(2);
  });

  it("uses token auth and omits formatTimeZone by default", async () => {
    vi.stubEnv("RINGBA_ACCOUNT_ID", "account-1");
    vi.stubEnv("RINGBA_API_TOKEN", "token-1");
    vi.stubEnv("RINGBA_API_BASE_URL", "https://api.ringba.com");
    vi.stubEnv("RINGBA_AUTH_SCHEME", "Bearer");

    const fetchMock = vi.fn().mockResolvedValue(buildJsonResponse({ id: "job-1" }));
    global.fetch = fetchMock as typeof fetch;

    await createRtbExportJob({
      reportStart: "2026-03-10T04:49:58.161Z",
      reportEnd: "2026-03-10T04:54:58.161Z",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [requestUrl, requestInit] = fetchMock.mock.calls[0] as [string, RequestInit];
    const requestBody = JSON.parse(String(requestInit.body));
    const headers = requestInit.headers as Record<string, string>;

    expect(requestUrl).toBe("https://api.ringba.com/account-1/rtb/export/csv");
    expect(headers.Authorization).toBe("Token token-1");
    expect(requestBody.formatTimeZone).toBeUndefined();
  });

  it("forwards explicit formatTimeZone overrides", async () => {
    vi.stubEnv("RINGBA_ACCOUNT_ID", "account-1");
    vi.stubEnv("RINGBA_API_TOKEN", "token-1");
    delete process.env.RINGBA_AUTH_SCHEME;

    const fetchMock = vi.fn().mockResolvedValue(buildJsonResponse({ id: "job-2" }));
    global.fetch = fetchMock as typeof fetch;

    await createRtbExportJob({
      reportStart: "2026-03-10T04:49:58.161Z",
      reportEnd: "2026-03-10T04:54:58.161Z",
      formatTimeZone: "America/New_York",
    });

    const [, requestInit] = fetchMock.mock.calls[0] as [string, RequestInit];
    const requestBody = JSON.parse(String(requestInit.body));
    const headers = requestInit.headers as Record<string, string>;

    expect(headers.Authorization).toBe("Token token-1");
    expect(requestBody.formatTimeZone).toBe("America/New_York");
  });

  it("persists source files and source rows during recent import preparation", async () => {
    const database = setupTestDatabase();
    vi.stubEnv("RINGBA_ACCOUNT_ID", "account-1");
    vi.stubEnv("RINGBA_API_TOKEN", "token-1");
    vi.stubEnv("RINGBA_API_BASE_URL", "https://api.ringba.com");
    delete process.env.RINGBA_AUTH_SCHEME;

    const csvText = [
      "Campaign,Publisher,Campaign ID,Publisher ID,Bid,Winning Bid,Winning Bid - Call Accepted,Winning Bid - Call Rejected,Bid Date,Expiration in Seconds,Bid Expire Date,Bid DID,Bid ID,Bid Rejected,Reason for Rejection",
      "Alpha,Pub A,campaign-a,publisher-a,10.5,12.25,True,False,03/10/2026 05:06:58 AM,30,03/10/2026 05:07:28 AM,18005550101,RTB-1,True,Capacity",
      "Beta,Pub B,campaign-b,publisher-b,5,0,False,True,03/10/2026 05:08:26 AM,45,03/10/2026 05:09:11 AM,18005550102,RTB-2,False,",
    ].join("\n");

    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(buildJsonResponse({ id: "job-1" }))
      .mockResolvedValueOnce(
        buildJsonResponse({
          status: "Ready",
          url: "https://download.test/ringba-export.zip",
        }),
      )
      .mockResolvedValueOnce(buildZipResponse(csvText));
    global.fetch = fetchMock as typeof fetch;

    try {
      const run = await createRingbaRecentImportRun({
        windowMinutes: 5,
        forceRefresh: false,
      });
      const detail = await prepareRingbaRecentImportRun({
        importRunId: run.id,
        sourceMetadata: run.sourceMetadata,
      });
      const sqlite = getSqlite();
      const sourceFiles = sqlite
        .prepare(
          "select file_name as fileName, row_count as rowCount, header_json as headerJson from import_source_files where import_run_id = ?",
        )
        .all(run.id) as Array<{ fileName: string; rowCount: number; headerJson: string }>;
      const sourceRows = sqlite
        .prepare(
          "select row_number as rowNumber, bid_id as bidId, bid_dt as bidDt, campaign_name as campaignName, publisher_name as publisherName, bid_amount as bidAmount, winning_bid as winningBid, bid_rejected as bidRejected, reason_for_reject as reasonForReject from import_source_rows where import_run_id = ? order by row_number",
        )
        .all(run.id) as Array<{
          rowNumber: number;
          bidId: string | null;
          bidDt: string | null;
          campaignName: string | null;
          publisherName: string | null;
          bidAmount: number | null;
          winningBid: number | null;
          bidRejected: number | null;
          reasonForReject: string | null;
        }>;

      expect(detail.totalItems).toBe(2);
      expect(sourceFiles).toHaveLength(1);
      expect(sourceFiles[0]?.fileName).toBe("ringba-export.csv");
      expect(sourceFiles[0]?.rowCount).toBe(2);
      expect(JSON.parse(sourceFiles[0]?.headerJson ?? "[]")).toContain("Bid ID");

      expect(sourceRows).toHaveLength(2);
      expect(sourceRows[0]).toMatchObject({
        rowNumber: 2,
        bidId: "RTB-1",
        bidDt: "2026-03-10T05:06:58.000Z",
        campaignName: "Alpha",
        publisherName: "Pub A",
        bidAmount: 10.5,
        winningBid: 12.25,
        bidRejected: 1,
        reasonForReject: "Capacity",
      });
      expect(sourceRows[1]).toMatchObject({
        rowNumber: 3,
        bidId: "RTB-2",
        bidDt: "2026-03-10T05:08:26.000Z",
        campaignName: "Beta",
        publisherName: "Pub B",
        bidAmount: 5,
        winningBid: 0,
        bidRejected: 0,
      });
    } finally {
      database.cleanup();
    }
  });
});
