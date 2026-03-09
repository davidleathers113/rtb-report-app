import { NextResponse } from "next/server";

import { createAsyncImportRun } from "@/lib/import-runs/service";
import { bulkInvestigateSchema } from "@/lib/validation/investigations";

export async function POST(request: Request) {
  try {
    const json = await request.json();
    const parsed = bulkInvestigateSchema.parse(json);
    const importRun = await createAsyncImportRun({
      bidIds: parsed.bidIds,
      forceRefresh: parsed.forceRefresh,
      sourceType: "manual_bulk",
      notes: `Manual bulk investigation for ${parsed.bidIds.length} bid ids.`,
    });

    return NextResponse.json(importRun, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to create import run.",
      },
      { status: 400 },
    );
  }
}
