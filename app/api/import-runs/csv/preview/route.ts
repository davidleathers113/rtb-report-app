import { NextResponse } from "next/server";

import { previewCsvUpload } from "@/lib/import-runs/csv";

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");
    const selectedColumnKey = formData.get("selectedColumnKey");

    if (!(file instanceof File)) {
      throw new Error("Upload a CSV file before previewing.");
    }

    const preview = await previewCsvUpload({
      file,
      selectedColumnKey:
        typeof selectedColumnKey === "string" ? selectedColumnKey : undefined,
    });

    return NextResponse.json(preview);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to preview the uploaded CSV.",
      },
      { status: 400 },
    );
  }
}
