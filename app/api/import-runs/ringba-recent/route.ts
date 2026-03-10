export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { createRingbaRecentImportRun } from "@/lib/import-runs/ringba-recent";
import { createRingbaRecentImportRunSchema } from "@/lib/validation/import-runs";

export async function POST(request: Request) {
  try {
    const json = await request.json();
    const parsed = createRingbaRecentImportRunSchema.parse(json);
    const importRun = await createRingbaRecentImportRun({
      windowMinutes: parsed.windowMinutes,
      forceRefresh: parsed.forceRefresh,
    });

    return NextResponse.json(importRun, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to create Ringba recent import run.",
      },
      { status: 400 },
    );
  }
}
