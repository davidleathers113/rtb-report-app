import "server-only";

import {
  acknowledgeImportScheduleAlert,
  clearImportScheduleAlertSnooze,
  claimDueImportSchedules,
  createImportSchedule,
  getActiveScheduledImportRuns,
  getImportScheduleDetail,
  getImportScheduleRunHistory,
  getImportSchedules,
  markImportScheduleTriggerFailure,
  markImportScheduleTriggered,
  pauseImportSchedule,
  resumeImportSchedule,
  snoozeImportScheduleAlerts,
  updateImportSchedule,
} from "@/lib/db/import-schedules";
import { createImportOpsEvent, listImportOpsEvents } from "@/lib/db/import-ops-events";
import { evaluateImportScheduleAlerts } from "@/lib/import-schedules/alerts";
import {
  processImportRun,
  rerunImportRun,
  retryFailedImportRunItems,
} from "@/lib/import-runs/service";
import { createRingbaRecentImportRun } from "@/lib/import-runs/ringba-recent";
import type { ImportRunDetail } from "@/types/import-run";
import type {
  ImportOpsEventSeverity,
  ImportOpsEventType,
  ImportOpsEventSource,
} from "@/types/ops-event";

function isScheduleDue(schedule: Awaited<ReturnType<typeof getImportSchedules>>[number]) {
  if (!schedule.isEnabled || schedule.isPaused) {
    return false;
  }

  if (!schedule.lastTriggeredAt) {
    return true;
  }

  const lastTriggeredAt = new Date(schedule.lastTriggeredAt).getTime();
  if (Number.isNaN(lastTriggeredAt)) {
    return true;
  }

  return lastTriggeredAt <= Date.now() - Math.max(schedule.windowMinutes, 1) * 60 * 1000;
}

export async function processDueImportSchedules(input?: {
  scheduleLimit?: number;
  activeRunLimit?: number;
  processBatchSize?: number;
  processMaxBatches?: number;
  staleAfterMinutes?: number;
  actionSource?: ImportOpsEventSource;
}) {
  const actionSource = input?.actionSource ?? "scheduled_trigger";
  const processedRuns: ImportRunDetail[] = [];
  const processedRunIds = new Set<string>();
  const scheduleErrors: Array<{ scheduleId: string; message: string }> = [];

  async function processRuns(runs: ImportRunDetail[]) {
    for (const run of runs) {
      if (processedRunIds.has(run.id)) {
        continue;
      }

      const processed = await processImportRun({
        importRunId: run.id,
        batchSize: input?.processBatchSize ?? 25,
        maxBatches: input?.processMaxBatches ?? 10,
      });

      if (processed) {
        processedRunIds.add(processed.id);
        processedRuns.push(processed);
      }
    }
  }

  const activeRunsBeforeClaim = await getActiveScheduledImportRuns({
    limit: input?.activeRunLimit ?? 10,
  });
  const schedulesBeforeClaim = await getImportSchedules();

  await processRuns(activeRunsBeforeClaim);

  await Promise.all(
    schedulesBeforeClaim
      .filter((schedule) => isScheduleDue(schedule) && schedule.activeRun && !schedule.activeRun.isStale)
      .map((schedule) =>
        createImportOpsEvent({
          eventType: "schedule_skipped_overlap",
          severity: "warning",
          source: actionSource,
          scheduleId: schedule.id,
          importRunId: schedule.activeRun?.id ?? null,
          message: `${schedule.name} skipped trigger because an active run is already in progress.`,
        }).catch(() => undefined),
      ),
  );

  const dueSchedules = await claimDueImportSchedules({
    limit: input?.scheduleLimit ?? 10,
    staleAfterMinutes: input?.staleAfterMinutes,
  });
  const createdRuns: ImportRunDetail[] = [];

  for (const schedule of dueSchedules) {
    try {
      const run = await createRingbaRecentImportRun({
        windowMinutes: schedule.windowMinutes,
        forceRefresh: false,
        accountId: schedule.accountId,
        overlapMinutes: schedule.overlapMinutes,
        triggerType: "scheduled",
        scheduleId: schedule.id,
        scheduleName: schedule.name,
      });

      await markImportScheduleTriggered({
        scheduleId: schedule.id,
        clearError: true,
      });
      await createImportOpsEvent({
        eventType: "schedule_claimed",
        severity: "info",
        source: actionSource,
        scheduleId: schedule.id,
        message: `${schedule.name} claimed for scheduled import processing.`,
      });
      await createImportOpsEvent({
        eventType: "scheduled_run_created",
        severity: "info",
        source: actionSource,
        scheduleId: schedule.id,
        importRunId: run.id,
        message: `${schedule.name} created scheduled run ${run.id}.`,
      });

      createdRuns.push(run);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unexpected schedule trigger error.";

      await markImportScheduleTriggerFailure({
        scheduleId: schedule.id,
        occurredAt: new Date().toISOString(),
        errorMessage: message,
      });
      await createImportOpsEvent({
        eventType: "scheduled_run_failed",
        severity: "error",
        source: actionSource,
        scheduleId: schedule.id,
        message,
      }).catch(() => undefined);

      scheduleErrors.push({
        scheduleId: schedule.id,
        message,
      });
    }
  }

  await processRuns(createdRuns);

  const activeRunsAfterClaim = await getActiveScheduledImportRuns({
    limit: input?.activeRunLimit ?? 10,
  });
  await processRuns(activeRunsAfterClaim);
  const schedules = await getImportSchedules();
  const alertResult = await evaluateImportScheduleAlerts(schedules);

  return {
    activeRunsRecovered: activeRunsBeforeClaim.length,
    dueSchedulesClaimed: dueSchedules.length,
    createdRuns,
    processedRuns,
    scheduleErrors,
    alertsSent: alertResult.alertsSent,
  };
}

export async function getImportScheduleOpsEvents(input: {
  scheduleId: string;
  limit?: number;
  offset?: number;
  eventType?: ImportOpsEventType | "all";
  severity?: ImportOpsEventSeverity | "all";
}) {
  return listImportOpsEvents({
    scheduleId: input.scheduleId,
    limit: input.limit,
    offset: input.offset,
    eventType: input.eventType,
    severity: input.severity,
  });
}

export async function performImportScheduleAction(input:
  | {
      scheduleId: string;
      action: "acknowledge_alert";
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "snooze_alert";
      snoozedUntil: string;
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "clear_snooze";
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "pause_schedule";
      reason?: string;
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "resume_schedule";
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "run_now";
      forceRefresh: boolean;
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "retry_failed_run";
      importRunId: string;
      forceRefresh: boolean;
      actionSource: ImportOpsEventSource;
    }
  | {
      scheduleId: string;
      action: "force_refresh_rerun";
      importRunId: string;
      actionSource: ImportOpsEventSource;
    }) {
  const schedule = await getImportScheduleDetail(input.scheduleId);

  if (!schedule) {
    throw new Error(`Import schedule not found: ${input.scheduleId}`);
  }

  if (input.action === "acknowledge_alert") {
    if (!schedule.currentAlertKey) {
      throw new Error("There is no active alert state to acknowledge.");
    }

    await acknowledgeImportScheduleAlert({
      scheduleId: schedule.id,
      alertKey: schedule.currentAlertKey,
    });
    await createImportOpsEvent({
      eventType: "alert_acknowledged",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      message: `${schedule.name} alert acknowledged.`,
      metadataJson: {
        alertKey: schedule.currentAlertKey,
      },
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run: null,
    };
  }

  if (input.action === "snooze_alert") {
    await snoozeImportScheduleAlerts({
      scheduleId: schedule.id,
      snoozedUntil: input.snoozedUntil,
    });
    await createImportOpsEvent({
      eventType: "alert_snoozed",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      message: `${schedule.name} alerts snoozed until ${input.snoozedUntil}.`,
      metadataJson: {
        snoozedUntil: input.snoozedUntil,
      },
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run: null,
    };
  }

  if (input.action === "clear_snooze") {
    await clearImportScheduleAlertSnooze(schedule.id);
    await createImportOpsEvent({
      eventType: "alert_snooze_cleared",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      message: `${schedule.name} alert snooze cleared.`,
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run: null,
    };
  }

  if (input.action === "pause_schedule") {
    await pauseImportSchedule({
      scheduleId: schedule.id,
      reason: input.reason,
    });
    await createImportOpsEvent({
      eventType: "schedule_paused",
      severity: "warning",
      source: input.actionSource,
      scheduleId: schedule.id,
      message: `${schedule.name} paused.`,
      metadataJson: {
        reason: input.reason ?? null,
      },
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run: null,
    };
  }

  if (input.action === "resume_schedule") {
    await resumeImportSchedule(schedule.id);
    await createImportOpsEvent({
      eventType: "schedule_resumed",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      message: `${schedule.name} resumed.`,
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run: null,
    };
  }

  if (input.action === "run_now") {
    const run = await createRingbaRecentImportRun({
      windowMinutes: schedule.windowMinutes,
      forceRefresh: input.forceRefresh,
      accountId: schedule.accountId,
      overlapMinutes: schedule.overlapMinutes,
      triggerType: "scheduled",
      scheduleId: schedule.id,
      scheduleName: schedule.name,
    });
    await markImportScheduleTriggered({
      scheduleId: schedule.id,
      clearError: true,
    });
    await createImportOpsEvent({
      eventType: "scheduled_run_created",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      importRunId: run.id,
      message: `${schedule.name} created run ${run.id} from operator action.`,
    });
    await createImportOpsEvent({
      eventType: "operator_run_now",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      importRunId: run.id,
      message: `${schedule.name} triggered manually.`,
      metadataJson: {
        forceRefresh: input.forceRefresh,
      },
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run,
    };
  }

  if (input.action === "retry_failed_run") {
    const run = await retryFailedImportRunItems({
      importRunId: input.importRunId,
      forceRefresh: input.forceRefresh,
    });
    await createImportOpsEvent({
      eventType: "operator_retry_failed_run",
      severity: "info",
      source: input.actionSource,
      scheduleId: schedule.id,
      importRunId: input.importRunId,
      message: `${schedule.name} retried failed items for run ${input.importRunId}.`,
      metadataJson: {
        forceRefresh: input.forceRefresh,
      },
    });

    return {
      schedule: await getImportScheduleDetail(schedule.id),
      run,
    };
  }

  const run = await rerunImportRun({
    importRunId: input.importRunId,
    forceRefresh: true,
  });
  await createImportOpsEvent({
    eventType: "operator_force_refresh_rerun",
    severity: "info",
    source: input.actionSource,
    scheduleId: schedule.id,
    importRunId: input.importRunId,
    message: `${schedule.name} force-refresh reran run ${input.importRunId}.`,
  });

  return {
    schedule: await getImportScheduleDetail(schedule.id),
    run,
  };
}

export {
  createImportSchedule,
  getImportScheduleDetail,
  getImportScheduleRunHistory,
  getImportSchedules,
  updateImportSchedule,
};
