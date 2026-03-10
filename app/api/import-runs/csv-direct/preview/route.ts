export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { previewCsvDirectUpload } from "@/lib/import-runs/csv-direct";

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");

    if (!(file instanceof File)) {
      throw new Error("Upload a CSV file before previewing a direct import.");
    }

    const preview = await previewCsvDirectUpload({ file });

    return NextResponse.json(preview);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to preview direct CSV import.",
      },
      { status: 400 },
    );
  }
}
