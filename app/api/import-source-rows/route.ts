export const runtime = "nodejs";

import { NextResponse } from "next/server";

import {
  listImportSourceFiles,
  listImportSourceRows,
} from "@/lib/db/import-sources";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

function parseLimit(value: string | null) {
  if (!value) {
    return DEFAULT_LIMIT;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return DEFAULT_LIMIT;
  }

  return Math.min(MAX_LIMIT, Math.trunc(parsed));
}

function parseOffset(value: string | null) {
  if (!value) {
    return 0;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return Math.trunc(parsed);
}

function normalizeDateInput(value: string | null, isEnd: boolean) {
  if (!value) {
    return null;
  }

  if (value.includes("T")) {
    return value;
  }

  if (value.length === 10 && value[4] === "-" && value[7] === "-") {
    return `${value}T${isEnd ? "23:59:59.999Z" : "00:00:00.000Z"}`;
  }

  return null;
}

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const search = url.searchParams;
    const fileName = search.get("fileName") || undefined;
    const bidId = search.get("bidId") || undefined;
    const limit = parseLimit(search.get("limit"));
    const offset = parseOffset(search.get("offset"));
    const startBidDt = normalizeDateInput(search.get("startBidDt"), false);
    const endBidDt = normalizeDateInput(search.get("endBidDt"), true);

    const [files, rowsResult] = await Promise.all([
      listImportSourceFiles(),
      listImportSourceRows({
        fileName,
        bidId,
        startBidDt: startBidDt ?? undefined,
        endBidDt: endBidDt ?? undefined,
        limit,
        offset,
      }),
    ]);

    return NextResponse.json({
      items: rowsResult.items,
      total: rowsResult.total,
      limit,
      offset,
      files,
    });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to fetch import source rows.",
      },
      { status: 400 },
    );
  }
}
