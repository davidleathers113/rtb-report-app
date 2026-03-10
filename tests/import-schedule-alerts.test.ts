import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db/import-schedules", () => ({
  clearImportScheduleAlertAcknowledgement: vi.fn(),
  getImportScheduleAlertState: vi.fn(),
  updateImportScheduleAlertState: vi.fn(),
}));

vi.mock("@/lib/import-schedules/notifications", () => ({
  notifyImportScheduleAlert: vi.fn().mockResolvedValue({ delivered: true }),
}));

vi.mock("@/lib/db/import-ops-events", () => ({
  createImportOpsEvent: vi.fn(),
}));

import {
  clearImportScheduleAlertAcknowledgement,
  getImportScheduleAlertState,
  updateImportScheduleAlertState,
} from "@/lib/db/import-schedules";
import {
  evaluateImportScheduleAlerts,
  notifyTriggerAuthFailureIfNeeded,
} from "@/lib/import-schedules/alerts";
import { notifyImportScheduleAlert } from "@/lib/import-schedules/notifications";
import type { ImportScheduleDetail } from "@/types/import-schedule";

function buildSchedule(
  overrides: Partial<ImportScheduleDetail> = {},
): ImportScheduleDetail {
  return {
    id: "schedule-1",
    name: "Every 15",
    isEnabled: true,
    accountId: "RA1",
    sourceType: "ringba_recent_import",
    windowMinutes: 15,
    overlapMinutes: 2,
    maxConcurrentRuns: 1,
    lastTriggeredAt: "2026-03-10T00:00:00.000Z",
    lastSucceededAt: "2026-03-10T00:00:00.000Z",
    lastFailedAt: null,
    lastError: null,
    consecutiveFailureCount: 0,
    healthStatus: "healthy",
    healthSummary: "Schedule is healthy.",
    isNoRecentSuccess: false,
    isPaused: false,
    pausedAt: null,
    pauseReason: null,
    currentAlertKey: null,
    currentAlertLabel: null,
    alertAcknowledgedAt: null,
    alertAcknowledgedKey: null,
    isCurrentAlertAcknowledged: false,
    alertSnoozedUntil: null,
    isAlertSnoozed: false,
    createdAt: "2026-03-10T00:00:00.000Z",
    updatedAt: "2026-03-10T00:00:00.000Z",
    activeRun: null,
    recentRuns: [],
    recentRunTotalCount: 0,
    recentOpsEvents: [],
    recentOpsEventTotalCount: 0,
    analytics: {
      recentRunCount: 0,
      successfulRunCount: 0,
      failedRunCount: 0,
      completedWithErrorsCount: 0,
      runningRunCount: 0,
      queuedRunCount: 0,
      staleRunCount: 0,
      averageRunDurationMs: null,
      averageExportReadyLatencyMs: null,
      sourceStageFailureBreakdown: [],
      rootCauseSummary: [],
    },
    ...overrides,
  };
}

describe("import schedule alerts", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("sends repeated failure alerts when threshold is crossed", async () => {
    vi.mocked(getImportScheduleAlertState).mockResolvedValue({});

    await evaluateImportScheduleAlerts([
      buildSchedule({
        consecutiveFailureCount: 3,
        healthStatus: "failing",
        healthSummary: "Schedule has failed 3 times in a row.",
        lastFailedAt: "2026-03-10T01:00:00.000Z",
        lastError: "Export failed.",
        currentAlertKey: "repeated_failures:3",
        currentAlertLabel: "3 consecutive failures",
      }),
    ]);

    expect(notifyImportScheduleAlert).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "repeated_schedule_failures",
        scheduleId: "schedule-1",
      }),
    );
    expect(updateImportScheduleAlertState).toHaveBeenCalled();
  });

  it("dedupes repeated failure alerts when the same failure count was already sent", async () => {
    vi.mocked(getImportScheduleAlertState).mockResolvedValue({
      repeatedFailures: {
        sentAt: new Date().toISOString(),
        consecutiveFailureCount: 3,
      },
    });

    await evaluateImportScheduleAlerts([
      buildSchedule({
        consecutiveFailureCount: 3,
        healthStatus: "failing",
        healthSummary: "Schedule has failed 3 times in a row.",
        currentAlertKey: "repeated_failures:3",
        currentAlertLabel: "3 consecutive failures",
      }),
    ]);

    expect(notifyImportScheduleAlert).not.toHaveBeenCalled();
  });

  it("clears stale alert acknowledgements when the alert key changes", async () => {
    vi.mocked(getImportScheduleAlertState).mockResolvedValue({});

    await evaluateImportScheduleAlerts([
      buildSchedule({
        currentAlertKey: null,
        alertAcknowledgedAt: "2026-03-10T02:00:00.000Z",
        alertAcknowledgedKey: "repeated_failures:3",
      }),
    ]);

    expect(clearImportScheduleAlertAcknowledgement).toHaveBeenCalledWith("schedule-1");
  });

  it("dedupes trigger auth alerts in memory", async () => {
    await notifyTriggerAuthFailureIfNeeded({
      message: "Unauthorized trigger request.",
      authMode: "secret",
      requestSource: "test",
    });
    await notifyTriggerAuthFailureIfNeeded({
      message: "Unauthorized trigger request.",
      authMode: "secret",
      requestSource: "test",
    });

    expect(notifyImportScheduleAlert).toHaveBeenCalledTimes(1);
  });
});
