export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { recoverCsvDirectImportRuns } from "@/lib/import-runs/service";
import { recoverCsvDirectImportRunsSchema } from "@/lib/validation/import-runs";

export async function POST(request: Request) {
  try {
    const json = await request.json().catch(() => ({}));
    const parsed = recoverCsvDirectImportRunsSchema.parse(json);
    const result = await recoverCsvDirectImportRuns(parsed);

    return NextResponse.json(result, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to recover direct CSV import runs.",
      },
      { status: 400 },
    );
  }
}
