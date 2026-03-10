import { NextResponse } from "next/server";

import {
  createImportSchedule,
  getImportSchedules,
} from "@/lib/import-schedules/service";
import { createImportScheduleSchema } from "@/lib/validation/import-schedules";

export async function GET() {
  try {
    const schedules = await getImportSchedules();
    return NextResponse.json(schedules);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to fetch import schedules.",
      },
      { status: 400 },
    );
  }
}

export async function POST(request: Request) {
  try {
    const json = await request.json();
    const parsed = createImportScheduleSchema.parse(json);
    const schedule = await createImportSchedule(parsed);

    return NextResponse.json(schedule, { status: 201 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to create import schedule.",
      },
      { status: 400 },
    );
  }
}
