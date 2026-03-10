import "server-only";

import {
  claimDueImportSchedules,
  createImportSchedule,
  getActiveScheduledImportRuns,
  getImportScheduleDetail,
  getImportScheduleRunHistory,
  getImportSchedules,
  markImportScheduleTriggerFailure,
  markImportScheduleTriggered,
  updateImportSchedule,
} from "@/lib/db/import-schedules";
import { evaluateImportScheduleAlerts } from "@/lib/import-schedules/alerts";
import { processImportRun } from "@/lib/import-runs/service";
import { createRingbaRecentImportRun } from "@/lib/import-runs/ringba-recent";
import type { ImportRunDetail } from "@/types/import-run";

export async function processDueImportSchedules(input?: {
  scheduleLimit?: number;
  activeRunLimit?: number;
  processBatchSize?: number;
  processMaxBatches?: number;
  staleAfterMinutes?: number;
}) {
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

  await processRuns(activeRunsBeforeClaim);

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

export {
  createImportSchedule,
  getImportScheduleDetail,
  getImportScheduleRunHistory,
  getImportSchedules,
  updateImportSchedule,
};
