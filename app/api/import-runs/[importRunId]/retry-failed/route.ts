import { NextResponse } from "next/server";

import { retryFailedImportRunItems } from "@/lib/import-runs/service";
import { retryImportRunSchema } from "@/lib/validation/import-runs";

export async function POST(
  request: Request,
  context: { params: Promise<{ importRunId: string }> },
) {
  try {
    const { importRunId } = await context.params;
    const json = await request.json().catch(() => ({}));
    const parsed = retryImportRunSchema.parse(json);
    const detail = await retryFailedImportRunItems({
      importRunId,
      forceRefresh: parsed.forceRefresh,
    });

    return NextResponse.json(detail);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to retry failed import run items.",
      },
      { status: 400 },
    );
  }
}
