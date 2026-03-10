import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db/import-schedules", () => ({
  acknowledgeImportScheduleAlert: vi.fn(),
  clearImportScheduleAlertAcknowledgement: vi.fn(),
  clearImportScheduleAlertSnooze: vi.fn(),
  claimDueImportSchedules: vi.fn(),
  createImportSchedule: vi.fn(),
  getActiveScheduledImportRuns: vi.fn(),
  getImportScheduleDetail: vi.fn(),
  getImportScheduleRunHistory: vi.fn(),
  getImportSchedules: vi.fn(),
  markImportScheduleRunFailed: vi.fn(),
  markImportScheduleTriggerFailure: vi.fn(),
  markImportScheduleTriggered: vi.fn(),
  pauseImportSchedule: vi.fn(),
  resumeImportSchedule: vi.fn(),
  snoozeImportScheduleAlerts: vi.fn(),
  updateImportScheduleAlertState: vi.fn(),
  updateImportSchedule: vi.fn(),
}));

vi.mock("@/lib/import-runs/ringba-recent", () => ({
  createRingbaRecentImportRun: vi.fn(),
}));

vi.mock("@/lib/import-runs/service", () => ({
  processImportRun: vi.fn(),
  rerunImportRun: vi.fn(),
  retryFailedImportRunItems: vi.fn(),
}));

vi.mock("@/lib/import-schedules/alerts", () => ({
  evaluateImportScheduleAlerts: vi.fn().mockResolvedValue({ alertsSent: 0 }),
}));

vi.mock("@/lib/db/import-ops-events", () => ({
  createImportOpsEvent: vi.fn(),
  listImportOpsEvents: vi.fn(),
}));

import {
  acknowledgeImportScheduleAlert,
  claimDueImportSchedules,
  clearImportScheduleAlertSnooze,
  getActiveScheduledImportRuns,
  getImportScheduleDetail,
  getImportSchedules,
  markImportScheduleTriggerFailure,
  markImportScheduleTriggered,
  pauseImportSchedule,
  resumeImportSchedule,
  snoozeImportScheduleAlerts,
} from "@/lib/db/import-schedules";
import { evaluateImportScheduleAlerts } from "@/lib/import-schedules/alerts";
import { createRingbaRecentImportRun } from "@/lib/import-runs/ringba-recent";
import {
  processImportRun,
  rerunImportRun,
  retryFailedImportRunItems,
} from "@/lib/import-runs/service";
import {
  performImportScheduleAction,
  processDueImportSchedules,
} from "@/lib/import-schedules/service";
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
      clearError: true,
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

  it("acknowledges the current alert state", async () => {
    vi.mocked(getImportScheduleDetail).mockResolvedValue({
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
      lastError: "Export failed.",
      consecutiveFailureCount: 3,
      healthStatus: "failing",
      healthSummary: "Schedule has failed 3 times in a row.",
      isNoRecentSuccess: false,
      isPaused: false,
      pausedAt: null,
      pauseReason: null,
      currentAlertKey: "repeated_failures:3",
      currentAlertLabel: "3 consecutive failures",
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
      recentOpsEvents: [],
      recentOpsEventTotalCount: 0,
    });

    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "acknowledge_alert",
      actionSource: "manual_ui",
    });

    expect(acknowledgeImportScheduleAlert).toHaveBeenCalledWith({
      scheduleId: "schedule-1",
      alertKey: "repeated_failures:3",
    });
  });

  it("snoozes, pauses, resumes, and remediates runs", async () => {
    vi.mocked(getImportScheduleDetail).mockResolvedValue({
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
      healthStatus: "healthy",
      healthSummary: "Healthy",
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
      recentOpsEvents: [],
      recentOpsEventTotalCount: 0,
    });
    vi.mocked(retryFailedImportRunItems).mockResolvedValue(buildRun({ id: "run-2" }));
    vi.mocked(rerunImportRun).mockResolvedValue(buildRun({ id: "run-3", triggerType: "manual" }));
    vi.mocked(createRingbaRecentImportRun).mockResolvedValue(buildRun({ id: "run-4" }));

    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "snooze_alert",
      snoozedUntil: "2026-03-10T05:00:00.000Z",
      actionSource: "manual_ui",
    });
    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "clear_snooze",
      actionSource: "manual_ui",
    });
    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "pause_schedule",
      reason: "Maintenance",
      actionSource: "manual_ui",
    });
    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "resume_schedule",
      actionSource: "manual_ui",
    });
    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "run_now",
      forceRefresh: true,
      actionSource: "manual_ui",
    });
    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "retry_failed_run",
      importRunId: "11111111-1111-1111-1111-111111111111",
      forceRefresh: false,
      actionSource: "manual_ui",
    });
    await performImportScheduleAction({
      scheduleId: "schedule-1",
      action: "force_refresh_rerun",
      importRunId: "11111111-1111-1111-1111-111111111111",
      actionSource: "manual_ui",
    });

    expect(snoozeImportScheduleAlerts).toHaveBeenCalled();
    expect(clearImportScheduleAlertSnooze).toHaveBeenCalledWith("schedule-1");
    expect(pauseImportSchedule).toHaveBeenCalled();
    expect(resumeImportSchedule).toHaveBeenCalledWith("schedule-1");
    expect(markImportScheduleTriggered).toHaveBeenCalledWith({
      scheduleId: "schedule-1",
      clearError: true,
    });
    expect(retryFailedImportRunItems).toHaveBeenCalled();
    expect(rerunImportRun).toHaveBeenCalled();
  });
});
