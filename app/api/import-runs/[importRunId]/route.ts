export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { getImportRunDetail } from "@/lib/db/import-runs";

export async function GET(
  _: Request,
  context: { params: Promise<{ importRunId: string }> },
) {
  try {
    const { importRunId } = await context.params;
    const detail = await getImportRunDetail(importRunId);

    if (!detail) {
      return NextResponse.json(
        {
          error: "Import run not found.",
        },
        { status: 404 },
      );
    }

    return NextResponse.json(detail);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to fetch import run.",
      },
      { status: 400 },
    );
  }
}
