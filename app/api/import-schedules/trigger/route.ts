export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { notifyTriggerAuthFailureIfNeeded } from "@/lib/import-schedules/alerts";
import { createImportOpsEvent } from "@/lib/db/import-ops-events";
import { processDueImportSchedules } from "@/lib/import-schedules/service";
import { authorizeImportSchedulesTrigger } from "@/lib/import-schedules/trigger-auth";
import { triggerImportSchedulesSchema } from "@/lib/validation/import-schedules";

export async function POST(request: Request) {
  const startedAt = Date.now();

  try {
    const auth = authorizeImportSchedulesTrigger(request);
    const actionSource = request.headers.get("x-vercel-cron") ? "cron" : "api";
    await createImportOpsEvent({
      eventType: "trigger_attempted",
      severity: "info",
      source: actionSource,
      message: "Import schedule trigger route invoked.",
      metadataJson: {
        authMode: auth.authMode,
        requestSource: auth.requestSource,
      },
    }).catch(() => undefined);
    if (!auth.ok) {
      await createImportOpsEvent({
        eventType: "trigger_auth_failed",
        severity: "warning",
        source: actionSource,
        message: auth.reason ?? "Unauthorized trigger request.",
        metadataJson: {
          authMode: auth.authMode,
          requestSource: auth.requestSource,
        },
      }).catch(() => undefined);
      await notifyTriggerAuthFailureIfNeeded({
        message: auth.reason ?? "Unauthorized trigger request.",
        authMode: auth.authMode,
        requestSource: auth.requestSource,
      }).catch(() => undefined);
      const status =
        auth.reason === "Import schedule trigger secret is not configured." ? 503 : 401;
      return NextResponse.json(
        {
          error: auth.reason,
          authMode: auth.authMode,
        },
        { status },
      );
    }

    const json = await request.json().catch(() => ({}));
    const parsed = triggerImportSchedulesSchema.parse(json);
    const result = await processDueImportSchedules({
      ...parsed,
      actionSource,
    });

    console.info("import-schedules.trigger", {
      authMode: auth.authMode,
      requestSource: auth.requestSource,
      dueSchedulesClaimed: result.dueSchedulesClaimed,
      createdRuns: result.createdRuns.length,
      processedRuns: result.processedRuns.length,
    });

    return NextResponse.json({
      ...result,
      authMode: auth.authMode,
      requestSource: auth.requestSource,
      durationMs: Date.now() - startedAt,
      triggeredAt: new Date().toISOString(),
    });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to trigger import schedules.",
      },
      { status: 400 },
    );
  }
}
