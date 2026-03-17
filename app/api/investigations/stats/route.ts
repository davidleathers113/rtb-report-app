export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { getDashboardStats } from "@/lib/db/investigations";
import { investigationsQuerySchema } from "@/lib/validation/investigations";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    
    // We reuse the same schema but ignore page/pageSize
    const parsed = investigationsQuerySchema.parse({
      rootCause: searchParams.get("rootCause") ?? undefined,
      ownerType: searchParams.get("ownerType") ?? undefined,
      search: searchParams.get("search") ?? undefined,
      startDate: searchParams.get("startDate") ?? undefined,
      endDate: searchParams.get("endDate") ?? undefined,
      publisherName: searchParams.get("publisherName") ?? undefined,
      campaignName: searchParams.get("campaignName") ?? undefined,
      outcome: searchParams.get("outcome") ?? undefined,
    });

    const result = await getDashboardStats(parsed);

    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to fetch dashboard stats.",
      },
      { status: 400 },
    );
  }
}
