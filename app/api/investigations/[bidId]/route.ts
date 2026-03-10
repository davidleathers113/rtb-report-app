export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { getInvestigationByBidId } from "@/lib/db/investigations";

export async function GET(
  _: Request,
  context: { params: Promise<{ bidId: string }> },
) {
  try {
    const { bidId } = await context.params;
    const result = await getInvestigationByBidId(decodeURIComponent(bidId));

    if (!result) {
      return NextResponse.json(
        {
          error: "Investigation not found.",
        },
        { status: 404 },
      );
    }

    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to fetch investigation detail.",
      },
      { status: 400 },
    );
  }
}
