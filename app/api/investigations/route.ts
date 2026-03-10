export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { getInvestigations } from "@/lib/db/investigations";
import { investigationsQuerySchema } from "@/lib/validation/investigations";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const parsed = investigationsQuerySchema.parse({
      page: searchParams.get("page") ?? undefined,
      pageSize: searchParams.get("pageSize") ?? undefined,
      rootCause: searchParams.get("rootCause") ?? undefined,
      ownerType: searchParams.get("ownerType") ?? undefined,
      search: searchParams.get("search") ?? undefined,
    });

    const result = await getInvestigations(parsed);

    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to list investigations.",
      },
      { status: 400 },
    );
  }
}
