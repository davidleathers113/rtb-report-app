import "server-only";

import {
  getImportScheduleAlertState,
  updateImportScheduleAlertState,
} from "@/lib/db/import-schedules";
import { notifyImportScheduleAlert } from "@/lib/import-schedules/notifications";
import type { ImportScheduleDetail } from "@/types/import-schedule";
import type { ImportScheduleAlertState } from "@/lib/db/import-schedules";

const REPEATED_FAILURE_ALERT_THRESHOLD = 3;
const ALERT_COOLDOWN_MINUTES = 60;
const SYSTEM_ALERT_COOLDOWN_MINUTES = 15;

const inMemorySystemAlertState = new Map<string, number>();

function cloneAlertState(value: ImportScheduleAlertState | null) {
  return { ...(value ?? {}) } as Record<string, unknown>;
}

function canSendAgain(sentAt: string | undefined, cooldownMinutes: number) {
  if (!sentAt) {
    return true;
  }

  const sentAtMs = new Date(sentAt).getTime();
  if (Number.isNaN(sentAtMs)) {
    return true;
  }

  return sentAtMs <= Date.now() - cooldownMinutes * 60 * 1000;
}

function getLatestSourceStageFailureRun(schedule: ImportScheduleDetail) {
  return (
    schedule.recentRuns.find((run) => {
      return (
        (run.status === "failed" || run.status === "completed_with_errors") &&
        Boolean(run.failedStage || run.failureReason)
      );
    }) ?? null
  );
}

async function maybeSendScheduleAlert(input: {
  schedule: ImportScheduleDetail;
  alertState: Record<string, unknown>;
  alertKey: string;
  shouldSend: boolean;
  nextAlertState: Record<string, unknown>;
  event: Parameters<typeof notifyImportScheduleAlert>[0];
}) {
  if (!input.shouldSend) {
    return false;
  }

  let result: Awaited<ReturnType<typeof notifyImportScheduleAlert>>;
  try {
    result = await notifyImportScheduleAlert(input.event);
  } catch (error) {
    console.error("import-schedules.alert-delivery-failed", {
      scheduleId: input.schedule.id,
      kind: input.event.kind,
      error: error instanceof Error ? error.message : String(error),
    });
    return false;
  }

  if (!result.delivered) {
    return false;
  }

  input.nextAlertState[input.alertKey] = {
    ...(typeof input.alertState[input.alertKey] === "object" &&
    input.alertState[input.alertKey] !== null &&
    !Array.isArray(input.alertState[input.alertKey])
      ? (input.alertState[input.alertKey] as Record<string, unknown>)
      : {}),
    sentAt: new Date().toISOString(),
  };

  return true;
}

export async function evaluateImportScheduleAlerts(schedules: ImportScheduleDetail[]) {
  let alertsSent = 0;

  for (const schedule of schedules) {
    const currentAlertState = await getImportScheduleAlertState(schedule.id);
    if (currentAlertState == null) {
      continue;
    }

    const nextAlertState = cloneAlertState(currentAlertState);
    const repeatedFailuresState =
      typeof currentAlertState.repeatedFailures === "object" &&
      currentAlertState.repeatedFailures !== null &&
      !Array.isArray(currentAlertState.repeatedFailures)
        ? (currentAlertState.repeatedFailures as Record<string, unknown>)
        : {};
    const staleState =
      typeof currentAlertState.stale === "object" &&
      currentAlertState.stale !== null &&
      !Array.isArray(currentAlertState.stale)
        ? (currentAlertState.stale as Record<string, unknown>)
        : {};
    const noRecentSuccessState =
      typeof currentAlertState.noRecentSuccess === "object" &&
      currentAlertState.noRecentSuccess !== null &&
      !Array.isArray(currentAlertState.noRecentSuccess)
        ? (currentAlertState.noRecentSuccess as Record<string, unknown>)
        : {};
    const sourceStageHardFailureState =
      typeof currentAlertState.sourceStageHardFailure === "object" &&
      currentAlertState.sourceStageHardFailure !== null &&
      !Array.isArray(currentAlertState.sourceStageHardFailure)
        ? (currentAlertState.sourceStageHardFailure as Record<string, unknown>)
        : {};

    const latestSourceStageFailure = getLatestSourceStageFailureRun(schedule);

    const repeatedFailuresSent = await maybeSendScheduleAlert({
      schedule,
      alertState: currentAlertState as Record<string, unknown>,
      alertKey: "repeatedFailures",
      nextAlertState,
      shouldSend:
        schedule.consecutiveFailureCount >= REPEATED_FAILURE_ALERT_THRESHOLD &&
        ((typeof repeatedFailuresState.consecutiveFailureCount === "number" &&
          repeatedFailuresState.consecutiveFailureCount < schedule.consecutiveFailureCount) ||
          canSendAgain(
            typeof repeatedFailuresState.sentAt === "string"
              ? repeatedFailuresState.sentAt
              : undefined,
            ALERT_COOLDOWN_MINUTES,
          )),
      event: {
        scheduleId: schedule.id,
        scheduleName: schedule.name,
        kind: "repeated_schedule_failures",
        message: `${schedule.name} has failed ${schedule.consecutiveFailureCount} times in a row.`,
        metadata: {
          consecutiveFailureCount: schedule.consecutiveFailureCount,
          healthStatus: schedule.healthStatus,
        },
      },
    });

    if (repeatedFailuresSent) {
      const repeatedState =
        (nextAlertState.repeatedFailures as Record<string, unknown> | undefined) ?? {};
      repeatedState.consecutiveFailureCount = schedule.consecutiveFailureCount;
      nextAlertState.repeatedFailures = repeatedState;
      alertsSent += 1;
    }

    const staleSent = await maybeSendScheduleAlert({
      schedule,
      alertState: currentAlertState as Record<string, unknown>,
      alertKey: "stale",
      nextAlertState,
      shouldSend:
        Boolean(schedule.activeRun?.isStale) &&
        (((typeof staleState.runId === "string" ? staleState.runId : null) !==
          schedule.activeRun?.id) ||
          canSendAgain(
            typeof staleState.sentAt === "string" ? staleState.sentAt : undefined,
            ALERT_COOLDOWN_MINUTES,
          )),
      event: {
        scheduleId: schedule.id,
        scheduleName: schedule.name,
        runId: schedule.activeRun?.id,
        kind: "scheduled_run_stale",
        message: `${schedule.name} has a stale active run.`,
        metadata: {
          runStatus: schedule.activeRun?.status ?? null,
          sourceStage: schedule.activeRun?.sourceStage ?? null,
        },
      },
    });

    if (staleSent) {
      const staleNext = (nextAlertState.stale as Record<string, unknown> | undefined) ?? {};
      staleNext.runId = schedule.activeRun?.id ?? null;
      nextAlertState.stale = staleNext;
      alertsSent += 1;
    }

    const noRecentSuccessSent = await maybeSendScheduleAlert({
      schedule,
      alertState: currentAlertState as Record<string, unknown>,
      alertKey: "noRecentSuccess",
      nextAlertState,
      shouldSend:
        schedule.isNoRecentSuccess &&
        (((typeof noRecentSuccessState.referenceAt === "string"
          ? noRecentSuccessState.referenceAt
          : null) !== schedule.lastSucceededAt) ||
          canSendAgain(
            typeof noRecentSuccessState.sentAt === "string"
              ? noRecentSuccessState.sentAt
              : undefined,
            ALERT_COOLDOWN_MINUTES,
          )),
      event: {
        scheduleId: schedule.id,
        scheduleName: schedule.name,
        kind: "no_recent_success",
        message: `${schedule.name} has not succeeded within its expected cadence.`,
        metadata: {
          lastSucceededAt: schedule.lastSucceededAt ?? null,
          windowMinutes: schedule.windowMinutes,
        },
      },
    });

    if (noRecentSuccessSent) {
      const noRecentSuccessNext =
        (nextAlertState.noRecentSuccess as Record<string, unknown> | undefined) ?? {};
      noRecentSuccessNext.referenceAt = schedule.lastSucceededAt ?? "never";
      nextAlertState.noRecentSuccess = noRecentSuccessNext;
      alertsSent += 1;
    }

    const sourceStageHardFailureSent = await maybeSendScheduleAlert({
      schedule,
      alertState: currentAlertState as Record<string, unknown>,
      alertKey: "sourceStageHardFailure",
      nextAlertState,
      shouldSend:
        latestSourceStageFailure !== null &&
        (latestSourceStageFailure.sourceStage === "failed" ||
          latestSourceStageFailure.failedStage !== null) &&
        (((typeof sourceStageHardFailureState.runId === "string"
          ? sourceStageHardFailureState.runId
          : null) !== latestSourceStageFailure.id) ||
          canSendAgain(
            typeof sourceStageHardFailureState.sentAt === "string"
              ? sourceStageHardFailureState.sentAt
              : undefined,
            ALERT_COOLDOWN_MINUTES,
          )),
      event: {
        scheduleId: schedule.id,
        scheduleName: schedule.name,
        runId: latestSourceStageFailure?.id,
        kind: "source_stage_hard_failed",
        message:
          latestSourceStageFailure?.failureReason ??
          `${schedule.name} source stage failed.`,
        metadata: {
          failedStage: latestSourceStageFailure?.failedStage ?? latestSourceStageFailure?.sourceStage,
        },
      },
    });

    if (sourceStageHardFailureSent) {
      const sourceStageNext =
        (nextAlertState.sourceStageHardFailure as Record<string, unknown> | undefined) ?? {};
      sourceStageNext.runId = latestSourceStageFailure?.id ?? null;
      sourceStageNext.failedStage =
        latestSourceStageFailure?.failedStage ?? latestSourceStageFailure?.sourceStage ?? null;
      nextAlertState.sourceStageHardFailure = sourceStageNext;
      alertsSent += 1;
    }

    if (JSON.stringify(nextAlertState) !== JSON.stringify(currentAlertState)) {
      await updateImportScheduleAlertState({
        scheduleId: schedule.id,
        alertState: nextAlertState as Record<string, unknown>,
      });
    }
  }

  return { alertsSent };
}

export async function notifyTriggerAuthFailureIfNeeded(input: {
  message: string;
  authMode: string;
  requestSource: string;
}) {
  const key = `${input.authMode}:${input.message}`;
  const lastSentAt = inMemorySystemAlertState.get(key) ?? 0;

  if (lastSentAt > Date.now() - SYSTEM_ALERT_COOLDOWN_MINUTES * 60 * 1000) {
    return { delivered: false };
  }

  let result: Awaited<ReturnType<typeof notifyImportScheduleAlert>>;
  try {
    result = await notifyImportScheduleAlert({
      kind: "trigger_auth_failed",
      message: input.message,
      metadata: {
        authMode: input.authMode,
        requestSource: input.requestSource,
      },
    });
  } catch (error) {
    console.error("import-schedules.auth-alert-delivery-failed", {
      authMode: input.authMode,
      error: error instanceof Error ? error.message : String(error),
    });
    return { delivered: false };
  }

  if (result.delivered) {
    inMemorySystemAlertState.set(key, Date.now());
  }

  return result;
}
