export const runtime = "nodejs";

import { NextResponse } from "next/server";

import {
  getImportScheduleDetail,
  getImportScheduleOpsEvents,
  getImportScheduleRunHistory,
  performImportScheduleAction,
  updateImportSchedule,
} from "@/lib/import-schedules/service";
import {
  importScheduleActionSchema,
  importScheduleOpsEventsQuerySchema,
  importScheduleRunHistoryQuerySchema,
  updateImportScheduleSchema,
} from "@/lib/validation/import-schedules";

export async function GET(
  request: Request,
  context: { params: Promise<{ scheduleId: string }> },
) {
  try {
    const { scheduleId } = await context.params;
    const { searchParams } = new URL(request.url);
    const view = searchParams.get("view");

    if (view === "history") {
      const parsed = importScheduleRunHistoryQuerySchema.parse({
        limit: searchParams.get("limit") ?? undefined,
        offset: searchParams.get("offset") ?? undefined,
        status: searchParams.get("status") ?? undefined,
      });

      const history = await getImportScheduleRunHistory({
        scheduleId,
        limit: parsed.limit,
        offset: parsed.offset,
        statusFilter: parsed.status,
      });

      return NextResponse.json(history);
    }

    if (view === "events") {
      const parsed = importScheduleOpsEventsQuerySchema.parse({
        limit: searchParams.get("limit") ?? undefined,
        offset: searchParams.get("offset") ?? undefined,
        eventType: searchParams.get("eventType") ?? undefined,
        severity: searchParams.get("severity") ?? undefined,
      });

      const events = await getImportScheduleOpsEvents({
        scheduleId,
        limit: parsed.limit,
        offset: parsed.offset,
        eventType: parsed.eventType,
        severity: parsed.severity,
      });

      return NextResponse.json(events);
    }

    const schedule = await getImportScheduleDetail(scheduleId);

    if (!schedule) {
      return NextResponse.json({ error: "Import schedule not found." }, { status: 404 });
    }

    return NextResponse.json(schedule);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to fetch import schedule.",
      },
      { status: 400 },
    );
  }
}

export async function POST(
  request: Request,
  context: { params: Promise<{ scheduleId: string }> },
) {
  try {
    const { scheduleId } = await context.params;
    const json = await request.json();
    const parsed = importScheduleActionSchema.parse(json);
    const result = await performImportScheduleAction({
      scheduleId,
      ...parsed,
    });

    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to perform schedule action.",
      },
      { status: 400 },
    );
  }
}

export async function PATCH(
  request: Request,
  context: { params: Promise<{ scheduleId: string }> },
) {
  try {
    const { scheduleId } = await context.params;
    const json = await request.json();
    const parsed = updateImportScheduleSchema.parse(json);
    const schedule = await updateImportSchedule({
      scheduleId,
      ...parsed,
    });

    return NextResponse.json(schedule);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to update import schedule.",
      },
      { status: 400 },
    );
  }
}
