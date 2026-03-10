import "server-only";

export interface ImportScheduleAlertEvent {
  scheduleId?: string;
  scheduleName?: string;
  runId?: string;
  kind:
    | "schedule_trigger_failed"
    | "scheduled_run_failed"
    | "scheduled_run_stale"
    | "repeated_schedule_failures"
    | "no_recent_success"
    | "source_stage_hard_failed"
    | "trigger_auth_failed";
  message: string;
  metadata?: Record<string, string | number | boolean | null | undefined>;
}

function getSlackWebhookUrl() {
  const value = process.env.IMPORT_SCHEDULES_SLACK_WEBHOOK_URL?.trim();
  return value && value.length > 0 ? value : null;
}

function buildSlackLines(event: ImportScheduleAlertEvent) {
  const lines = [
    `*Bid Investigation Console Alert*`,
    `Kind: ${event.kind}`,
    event.scheduleName || event.scheduleId
      ? `Schedule: ${event.scheduleName ?? event.scheduleId}`
      : null,
    event.scheduleId ? `Schedule ID: ${event.scheduleId}` : null,
    event.runId ? `Run ID: ${event.runId}` : null,
    `Message: ${event.message}`,
  ];

  if (event.metadata) {
    for (const [key, value] of Object.entries(event.metadata)) {
      if (value === undefined || value === null || value === "") {
        continue;
      }

      lines.push(`${key}: ${String(value)}`);
    }
  }

  return lines.filter((line): line is string => Boolean(line));
}

export async function notifyImportScheduleAlert(event: ImportScheduleAlertEvent) {
  const webhookUrl = getSlackWebhookUrl();
  if (!webhookUrl) {
    return { delivered: false };
  }

  const response = await fetch(webhookUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      text: buildSlackLines(event).join("\n"),
    }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `Unable to deliver import schedule Slack alert: ${response.status} ${text}`.trim(),
    );
  }

  return { delivered: true };
}
