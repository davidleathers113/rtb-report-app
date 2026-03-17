export const runtime = "nodejs";

import { NextResponse } from "next/server";

import {
  isCsvDirectImportError,
  previewCsvDirectUpload,
} from "@/lib/import-runs/csv-direct";

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");

    if (!(file instanceof File)) {
      return NextResponse.json(
        {
          error: "Upload a CSV file before previewing a direct import.",
          code: "csv_direct_missing_file",
        },
        { status: 422 },
      );
    }

    const preview = await previewCsvDirectUpload({ file });

    return NextResponse.json(preview);
  } catch (error) {
    const payload = {
      error:
        error instanceof Error ? error.message : "Unable to preview direct CSV import.",
      code: isCsvDirectImportError(error) ? error.code : undefined,
      details: isCsvDirectImportError(error) ? error.details : undefined,
    };

    return NextResponse.json(
      payload,
      { status: isCsvDirectImportError(error) ? error.status : 500 },
    );
  }
}
