export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { investigateBid } from "@/lib/investigations/service";
import { fetchOneBidSchema } from "@/lib/validation/investigations";

export async function POST(request: Request) {
  try {
    const json = await request.json();
    const parsed = fetchOneBidSchema.parse(json);
    const result = await investigateBid(parsed.bidId, {
      importRunId: null,
      forceRefresh: parsed.forceRefresh,
    });

    return NextResponse.json(result.investigation);
  } catch (error) {
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Unable to fetch bid.",
      },
      { status: 400 },
    );
  }
}
