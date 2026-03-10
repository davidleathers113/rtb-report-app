export const runtime = "nodejs";

import { NextResponse } from "next/server";

import { createImportRunFromCsvDirectUpload } from "@/lib/import-runs/csv-direct";

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");
    const forceRefreshValue = formData.get("forceRefresh");

    if (!(file instanceof File)) {
      throw new Error("Upload a CSV file before creating a direct import run.");
    }

    const result = await createImportRunFromCsvDirectUpload({
      file,
      forceRefresh: forceRefreshValue === "true",
    });

    return NextResponse.json(result.importRun, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to create an import run from direct CSV upload.",
      },
      { status: 400 },
    );
  }
}
