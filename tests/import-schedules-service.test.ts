import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db/import-schedules", () => ({
  claimDueImportSchedules: vi.fn(),
  createImportSchedule: vi.fn(),
  getActiveScheduledImportRuns: vi.fn(),
  getImportScheduleDetail: vi.fn(),
  getImportScheduleRunHistory: vi.fn(),
  getImportSchedules: vi.fn(),
  markImportScheduleRunFailed: vi.fn(),
  markImportScheduleTriggerFailure: vi.fn(),
  markImportScheduleTriggered: vi.fn(),
  updateImportScheduleAlertState: vi.fn(),
  updateImportSchedule: vi.fn(),
}));

vi.mock("@/lib/import-runs/ringba-recent", () => ({
  createRingbaRecentImportRun: vi.fn(),
}));

vi.mock("@/lib/import-runs/service", () => ({
  processImportRun: vi.fn(),
}));

vi.mock("@/lib/import-schedules/alerts", () => ({
  evaluateImportScheduleAlerts: vi.fn().mockResolvedValue({ alertsSent: 0 }),
}));

import {
  claimDueImportSchedules,
  getActiveScheduledImportRuns,
  getImportSchedules,
  markImportScheduleTriggerFailure,
  markImportScheduleTriggered,
} from "@/lib/db/import-schedules";
import { evaluateImportScheduleAlerts } from "@/lib/import-schedules/alerts";
import { createRingbaRecentImportRun } from "@/lib/import-runs/ringba-recent";
import { processImportRun } from "@/lib/import-runs/service";
import { processDueImportSchedules } from "@/lib/import-schedules/service";
import type { ImportRunDetail } from "@/types/import-run";

function buildRun(overrides: Partial<ImportRunDetail> = {}): ImportRunDetail {
  return {
    id: "run-1",
    sourceType: "ringba_recent_import",
    triggerType: "scheduled",
    scheduleId: "schedule-1",
    sourceStage: "queued",
    status: "queued",
    forceRefresh: false,
    notes: null,
    lastError: null,
    sourceWindowStart: null,
    sourceWindowEnd: null,
    exportJobId: null,
    exportRowCount: 0,
    exportDownloadStatus: null,
    sourceMetadata: {},
    startedAt: "2026-03-10T00:00:00.000Z",
    completedAt: null,
    createdAt: "2026-03-10T00:00:00.000Z",
    updatedAt: "2026-03-10T00:00:00.000Z",
    totalItems: 0,
    queuedCount: 0,
    runningCount: 0,
    completedCount: 0,
    reusedCount: 0,
    fetchedCount: 0,
    failedCount: 0,
    percentComplete: 0,
    items: [],
    ...overrides,
  };
}

describe("import schedules service", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getImportSchedules).mockResolvedValue([]);
  });

  it("creates due scheduled runs and clears trigger lease", async () => {
    vi.mocked(claimDueImportSchedules).mockResolvedValue([
      {
        id: "schedule-1",
        name: "Every 15",
        isEnabled: true,
        accountId: "RA1",
        sourceType: "ringba_recent_import",
        windowMinutes: 15,
        overlapMinutes: 2,
        maxConcurrentRuns: 1,
        lastTriggeredAt: null,
        lastSucceededAt: null,
        lastFailedAt: null,
        lastError: null,
        consecutiveFailureCount: 0,
      },
    ]);
    vi.mocked(createRingbaRecentImportRun).mockResolvedValue(buildRun());
    vi.mocked(getActiveScheduledImportRuns).mockResolvedValue([]);

    const result = await processDueImportSchedules();

    expect(createRingbaRecentImportRun).toHaveBeenCalledWith({
      windowMinutes: 15,
      forceRefresh: false,
      accountId: "RA1",
      overlapMinutes: 2,
      triggerType: "scheduled",
      scheduleId: "schedule-1",
      scheduleName: "Every 15",
    });
    expect(markImportScheduleTriggered).toHaveBeenCalledWith({
      scheduleId: "schedule-1",
    });
    expect(evaluateImportScheduleAlerts).toHaveBeenCalled();
    expect(result.createdRuns).toHaveLength(1);
  });

  it("processes active scheduled runs and syncs schedule health", async () => {
    vi.mocked(claimDueImportSchedules).mockResolvedValue([]);
    vi.mocked(getActiveScheduledImportRuns).mockResolvedValue([
      buildRun({
        id: "run-2",
      }),
    ]);
    vi.mocked(processImportRun).mockResolvedValue(
      buildRun({
        id: "run-2",
        status: "completed",
        sourceStage: "completed",
      }),
    );

    const result = await processDueImportSchedules({
      processBatchSize: 20,
      processMaxBatches: 5,
    });

    expect(processImportRun).toHaveBeenCalledWith({
      importRunId: "run-2",
      batchSize: 20,
      maxBatches: 5,
    });
    expect(markImportScheduleTriggerFailure).not.toHaveBeenCalled();
    expect(result.processedRuns).toHaveLength(1);
  });

  it("processes existing active runs before claiming new due schedules", async () => {
    vi.mocked(getActiveScheduledImportRuns)
      .mockResolvedValueOnce([buildRun({ id: "run-existing" })])
      .mockResolvedValueOnce([]);
    vi.mocked(processImportRun).mockResolvedValue(
      buildRun({
        id: "run-existing",
        status: "completed",
        sourceStage: "completed",
      }),
    );
    vi.mocked(claimDueImportSchedules).mockResolvedValue([]);

    await processDueImportSchedules();

    expect(processImportRun).toHaveBeenCalledWith({
      importRunId: "run-existing",
      batchSize: 25,
      maxBatches: 10,
    });
    expect(claimDueImportSchedules).toHaveBeenCalledTimes(1);
  });
});
