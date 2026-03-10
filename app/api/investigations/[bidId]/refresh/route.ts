export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { investigateBid } from "@/lib/investigations/service";

export async function POST(
  _: Request,
  context: { params: Promise<{ bidId: string }> },
) {
  try {
    const { bidId } = await context.params;
    const result = await investigateBid(decodeURIComponent(bidId), {
      importRunId: null,
      forceRefresh: true,
    });

    return NextResponse.json(result.investigation);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to refresh investigation.",
      },
      { status: 400 },
    );
  }
}
