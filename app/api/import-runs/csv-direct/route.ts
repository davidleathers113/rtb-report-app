export const runtime = "nodejs";

import { NextResponse } from "next/server";

import {
  createImportRunFromCsvDirectUpload,
  isCsvDirectImportError,
} from "@/lib/import-runs/csv-direct";

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");
    const forceRefreshValue = formData.get("forceRefresh");
    const allowDuplicateValue = formData.get("allowDuplicate");

    if (!(file instanceof File)) {
      return NextResponse.json(
        {
          error: "Upload a CSV file before creating a direct import run.",
          code: "csv_direct_missing_file",
        },
        { status: 422 },
      );
    }

    const result = await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: forceRefreshValue === "true",
      allowDuplicate: allowDuplicateValue === "true",
    });

    return NextResponse.json(result, { status: 202 });
  } catch (error) {
    const payload = {
      error:
        error instanceof Error
          ? error.message
          : "Unable to create an import run from direct CSV upload.",
      code: isCsvDirectImportError(error) ? error.code : undefined,
      details: isCsvDirectImportError(error) ? error.details : undefined,
    };

    return NextResponse.json(
      payload,
      { status: isCsvDirectImportError(error) ? error.status : 500 },
    );
  }
}
