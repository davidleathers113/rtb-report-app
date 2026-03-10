export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { processImportRun } from "@/lib/import-runs/service";
import { processImportRunSchema } from "@/lib/validation/import-runs";

export async function POST(
  request: Request,
  context: { params: Promise<{ importRunId: string }> },
) {
  try {
    const { importRunId } = await context.params;
    const json = await request.json().catch(() => ({}));
    const parsed = processImportRunSchema.parse(json);
    const detail = await processImportRun({
      importRunId,
      batchSize: parsed.batchSize,
      maxBatches: parsed.maxBatches,
    });

    return NextResponse.json(detail);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to process import run.",
      },
      { status: 400 },
    );
  }
}
