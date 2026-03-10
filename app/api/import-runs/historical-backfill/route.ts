export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { createHistoricalRingbaBackfillRun } from "@/lib/import-runs/historical-backfill";
import { createHistoricalBackfillRunSchema } from "@/lib/validation/import-runs";

export async function POST(request: Request) {
  try {
    const json = await request.json();
    const parsed = createHistoricalBackfillRunSchema.parse(json);
    const importRun = await createHistoricalRingbaBackfillRun(parsed);

    return NextResponse.json(importRun, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to create historical Ringba backfill run.",
      },
      { status: 400 },
    );
  }
}
