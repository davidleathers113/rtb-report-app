import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { eq } from "drizzle-orm";
import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { afterEach, describe, expect, it } from "vitest";

import { getDb, resetDbClientForTests } from "@/lib/db/client";
import {
  bidInvestigations,
  importRunItems,
  importRuns,
  importSchedules,
} from "@/lib/db/schema";

async function setupTestDatabase() {
  const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "bid-console-"));
  const databasePath = path.join(tempDirectory, "test.sqlite");
  process.env.BID_CONSOLE_DB_PATH = databasePath;
  resetDbClientForTests();
  migrate(getDb(), {
    migrationsFolder: path.join(process.cwd(), "drizzle"),
  });

  return { databasePath };
}

function buildNormalizedBid(bidId: string) {
  return {
    bidId,
    bidDt: "2026-03-09T00:00:00.000Z",
    campaignName: "Campaign",
    campaignId: "campaign-1",
    publisherName: "Publisher",
    publisherId: "publisher-1",
    targetName: "Target",
    targetId: "target-1",
    buyerName: "Buyer",
    buyerId: "buyer-1",
    bidAmount: 1.25,
    winningBid: 1.25,
    isZeroBid: false,
    reasonForReject: null,
    httpStatusCode: 200,
    errorMessage: null,
    requestBody: {},
    responseBody: {},
    rawTraceJson: {},
    relevantEvents: [],
    outcome: "accepted" as const,
  };
}

function buildDiagnosis() {
  return {
    rootCause: "unknown_needs_review" as const,
    confidence: 0.5,
    severity: "medium" as const,
    ownerType: "unknown" as const,
    suggestedFix: "Review manually.",
    explanation: "Investigated.",
    evidence: [],
  };
}

afterEach(() => {
  const databasePath = process.env.BID_CONSOLE_DB_PATH;
  resetDbClientForTests();
  if (databasePath) {
    for (const filePath of [databasePath, `${databasePath}-wal`, `${databasePath}-shm`]) {
      if (fs.existsSync(filePath)) {
        fs.rmSync(filePath);
      }
    }

    const directoryPath = path.dirname(databasePath);
    if (fs.existsSync(directoryPath)) {
      fs.rmSync(directoryPath, { recursive: true, force: true });
    }
  }

  delete process.env.BID_CONSOLE_DB_PATH;
});

describe.sequential("sqlite persistence integration", () => {
  it("reuses a fetched investigation and only reacquires stale pending claims", async () => {
    await setupTestDatabase();
    const db = getDb();
    const investigations = await import("@/lib/db/investigations");

    const firstClaim = await investigations.claimInvestigationFetch({
      bidId: "bid-1",
      importRunId: null,
      forceRefresh: false,
      leaseSeconds: 60,
    });
    expect(firstClaim.shouldFetch).toBe(true);

    await investigations.upsertInvestigation({
      importRunId: null,
      normalizedBid: buildNormalizedBid("bid-1"),
      diagnosis: buildDiagnosis(),
    });

    const reusedClaim = await investigations.claimInvestigationFetch({
      bidId: "bid-1",
      importRunId: null,
      forceRefresh: false,
      leaseSeconds: 60,
    });
    expect(reusedClaim.shouldFetch).toBe(false);
    expect(reusedClaim.fetchStatus).toBe("fetched");

    await db
      .update(bidInvestigations)
      .set({
        fetchStatus: "pending",
        fetchedAt: null,
        leaseExpiresAt: "2026-03-09T00:00:00.000Z",
      })
      .where(eq(bidInvestigations.bidId, "bid-1"))
      .run();

    const staleClaim = await investigations.claimInvestigationFetch({
      bidId: "bid-1",
      importRunId: null,
      forceRefresh: false,
      leaseSeconds: 60,
    });
    expect(staleClaim.shouldFetch).toBe(true);
    expect(staleClaim.fetchAttemptCount).toBeGreaterThan(reusedClaim.fetchAttemptCount);
  });

  it("loads investigation list items in chunks beyond SQLite variable limits", async () => {
    await setupTestDatabase();
    const investigations = await import("@/lib/db/investigations");

    const expectedBidIds: string[] = [];

    for (let index = 0; index < 1200; index += 1) {
      const bidId = `bid-${index}`;
      expectedBidIds.push(bidId);

      await investigations.upsertInvestigation({
        importRunId: null,
        normalizedBid: buildNormalizedBid(bidId),
        diagnosis: buildDiagnosis(),
      });
    }

    const ids = expectedBidIds.map((bidId) => `missing-${bidId}`);
    const persisted = await Promise.all(
      expectedBidIds.map((bidId) => investigations.getInvestigationByBidId(bidId)),
    );

    persisted.forEach((row, index) => {
      ids[index] = row?.id ?? ids[index];
    });

    const items = await investigations.getInvestigationListItemsByIds(ids);

    expect(items).toHaveLength(1200);
    expect(items.some((item) => item.bidId === "bid-0")).toBe(true);
    expect(items.some((item) => item.bidId === "bid-1199")).toBe(true);
  });

  it("reclaims stale import run items instead of leaving them stuck running", async () => {
    await setupTestDatabase();
    const db = getDb();
    const runs = await import("@/lib/db/import-runs");

    const runId = await runs.createImportRun({
      sourceType: "manual_bulk",
      bidIds: ["bid-1"],
      forceRefresh: false,
    });

    const firstBatch = await runs.claimImportRunItems({
      importRunId: runId,
      batchSize: 1,
      leaseSeconds: 60,
    });
    expect(firstBatch).toHaveLength(1);

    await db
      .update(importRunItems)
      .set({
        status: "running",
        leaseExpiresAt: "2026-03-09T00:00:00.000Z",
      })
      .where(eq(importRunItems.id, firstBatch[0].id))
      .run();

    const reclaimedBatch = await runs.claimImportRunItems({
      importRunId: runId,
      batchSize: 1,
      leaseSeconds: 60,
    });
    expect(reclaimedBatch).toHaveLength(1);
    expect(reclaimedBatch[0]?.id).toBe(firstBatch[0]?.id);

    const itemRow = db
      .select()
      .from(importRunItems)
      .where(eq(importRunItems.id, firstBatch[0].id))
      .get();
    expect(itemRow?.attemptCount).toBe(2);
  });

  it("reopens failed runs when queued work remains", async () => {
    await setupTestDatabase();
    const db = getDb();
    const runs = await import("@/lib/db/import-runs");

    const runId = await runs.createImportRun({
      sourceType: "csv_direct_import",
      bidIds: ["bid-1", "bid-2"],
      forceRefresh: false,
    });

    await db
      .update(importRuns)
      .set({
        status: "failed",
        sourceStage: "failed",
        lastError: "too many SQL variables",
      })
      .where(eq(importRuns.id, runId))
      .run();

    await db
      .update(importRunItems)
      .set({
        status: "completed",
        completedAt: "2026-03-09T00:00:00.000Z",
      })
      .where(eq(importRunItems.bidId, "bid-1"))
      .run();

    const detail = await runs.resetFailedImportRunItems({
      importRunId: runId,
      forceRefresh: false,
    });

    expect(detail?.status).toBe("queued");
    expect(detail?.sourceStage).toBe("queued");
    expect(detail?.queuedCount).toBe(1);
    expect(detail?.lastError).toBeNull();
  });

  it("claims due schedules while ignoring paused schedules and stale overlap blockers", async () => {
    await setupTestDatabase();
    const db = getDb();
    const schedules = await import("@/lib/db/import-schedules");

    db.insert(importSchedules)
      .values([
        {
          id: "schedule-due",
          name: "Due schedule",
          isEnabled: true,
          accountId: "RA1",
          sourceType: "ringba_recent_import",
          windowMinutes: 15,
          overlapMinutes: 2,
          maxConcurrentRuns: 1,
          createdAt: "2026-03-10T00:00:00.000Z",
          updatedAt: "2026-03-10T00:00:00.000Z",
        },
        {
          id: "schedule-paused",
          name: "Paused schedule",
          isEnabled: true,
          accountId: "RA1",
          sourceType: "ringba_recent_import",
          windowMinutes: 15,
          overlapMinutes: 2,
          maxConcurrentRuns: 1,
          pausedAt: "2026-03-10T00:00:00.000Z",
          createdAt: "2026-03-10T00:00:00.000Z",
          updatedAt: "2026-03-10T00:00:00.000Z",
        },
      ])
      .run();

    db.insert(importRuns)
      .values({
        id: "stale-run",
        sourceType: "ringba_recent_import",
        triggerType: "scheduled",
        scheduleId: "schedule-due",
        sourceStage: "processing",
        status: "running",
        processorLeaseExpiresAt: "2026-03-09T00:00:00.000Z",
        createdAt: "2026-03-09T00:00:00.000Z",
        updatedAt: "2026-03-09T00:00:00.000Z",
      })
      .run();

    const claimed = await schedules.claimDueImportSchedules({
      limit: 10,
      staleAfterMinutes: 30,
    });

    expect(claimed.map((schedule) => schedule.id)).toEqual(["schedule-due"]);
  });

  it("keeps schedule health monotonic when older runs finish after newer ones", async () => {
    await setupTestDatabase();
    const db = getDb();
    const schedules = await import("@/lib/db/import-schedules");

    db.insert(importSchedules)
      .values({
        id: "schedule-1",
        name: "Monotonic schedule",
        isEnabled: true,
        accountId: "RA1",
        sourceType: "ringba_recent_import",
        windowMinutes: 15,
        overlapMinutes: 2,
        maxConcurrentRuns: 1,
        createdAt: "2026-03-10T00:00:00.000Z",
        updatedAt: "2026-03-10T00:00:00.000Z",
      })
      .run();

    await schedules.markImportScheduleRunFailed({
      scheduleId: "schedule-1",
      runCreatedAt: "2026-03-10T02:00:00.000Z",
      occurredAt: "2026-03-10T02:10:00.000Z",
      errorMessage: "newer run failed",
    });

    await schedules.markImportScheduleRunSucceeded({
      scheduleId: "schedule-1",
      runCreatedAt: "2026-03-10T01:00:00.000Z",
      occurredAt: "2026-03-10T01:05:00.000Z",
    });

    const row = db
      .select()
      .from(importSchedules)
      .where(eq(importSchedules.id, "schedule-1"))
      .get();

    expect(row?.lastFailedAt).toBe("2026-03-10T02:10:00.000Z");
    expect(row?.lastSucceededAt).toBeNull();
    expect(row?.consecutiveFailureCount).toBe(1);
  });
});
