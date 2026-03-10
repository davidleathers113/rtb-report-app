export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { listImportOpsEvents } from "@/lib/db/import-ops-events";
import { importScheduleOpsEventsQuerySchema } from "@/lib/validation/import-schedules";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const parsed = importScheduleOpsEventsQuerySchema.parse({
      limit: searchParams.get("limit") ?? undefined,
      offset: searchParams.get("offset") ?? undefined,
      eventType: searchParams.get("eventType") ?? undefined,
      severity: searchParams.get("severity") ?? undefined,
    });

    const events = await listImportOpsEvents({
      limit: parsed.limit,
      offset: parsed.offset,
      eventType: parsed.eventType,
      severity: parsed.severity,
    });

    return NextResponse.json(events);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to fetch ops events.",
      },
      { status: 400 },
    );
  }
}
