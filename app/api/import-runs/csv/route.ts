import { NextResponse } from "next/server";

import { createImportRunFromCsvUpload } from "@/lib/import-runs/csv";

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");
    const selectedColumnKey = formData.get("selectedColumnKey");
    const forceRefreshValue = formData.get("forceRefresh");

    if (!(file instanceof File)) {
      throw new Error("Upload a CSV file before creating an import run.");
    }

    const result = await createImportRunFromCsvUpload({
      file,
      selectedColumnKey:
        typeof selectedColumnKey === "string" ? selectedColumnKey : undefined,
      forceRefresh: forceRefreshValue === "true",
    });

    return NextResponse.json(result.importRun, { status: 202 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to create an import run from CSV.",
      },
      { status: 400 },
    );
  }
}
