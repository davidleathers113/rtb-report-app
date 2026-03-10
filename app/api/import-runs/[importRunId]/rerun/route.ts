export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { rerunImportRun } from "@/lib/import-runs/service";
import { rerunImportRunSchema } from "@/lib/validation/import-runs";

export async function POST(
  request: Request,
  context: { params: Promise<{ importRunId: string }> },
) {
  try {
    const { importRunId } = await context.params;
    const json = await request.json().catch(() => ({}));
    const parsed = rerunImportRunSchema.parse(json);
    const detail = await rerunImportRun({
      importRunId,
      forceRefresh: parsed.forceRefresh,
    });

    return NextResponse.json(detail, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to rerun import run.",
      },
      { status: 400 },
    );
  }
}
