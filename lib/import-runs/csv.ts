import "server-only";

import Papa from "papaparse";

import {
  MAX_CSV_BID_IDS,
  MAX_CSV_UPLOAD_BYTES,
} from "@/lib/import-runs/csv-constants";
import { createAsyncImportRun } from "@/lib/import-runs/service";
import { isValidBidId } from "@/lib/utils/bid-id";
import type { CsvPreviewColumnOption, CsvPreviewResult } from "@/types/import-run";
const HEADER_ALIASES = ["bidid", "bid_id", "bid id"] as const;

function normalizeHeaderValue(value: string) {
  let normalized = "";

  for (const character of value.trim().toLowerCase()) {
    if (character === " " || character === "_" || character === "-") {
      continue;
    }

    normalized += character;
  }

  return normalized;
}

function buildColumnKey(index: number) {
  return `column_${index}`;
}

function readStringCell(row: unknown[], index: number) {
  if (index < 0 || index >= row.length) {
    return "";
  }

  const value = row[index];

  if (typeof value === "string") {
    return value.trim();
  }

  if (value === null || value === undefined) {
    return "";
  }

  return String(value).trim();
}

function detectHeaderIndex(row: string[]) {
  const matchingIndices: number[] = [];

  row.forEach((value, index) => {
    const normalized = normalizeHeaderValue(value);
    if (HEADER_ALIASES.includes(normalized as (typeof HEADER_ALIASES)[number])) {
      matchingIndices.push(index);
    }
  });

  return matchingIndices;
}

function valueContainsDigit(value: string) {
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code >= 48 && code <= 57) {
      return true;
    }
  }

  return false;
}

function countValuesWithDigits(row: string[]) {
  let count = 0;

  for (const value of row) {
    if (valueContainsDigit(value)) {
      count += 1;
    }
  }

  return count;
}

function autoDetectColumnIndex(rows: string[][], startRowIndex: number) {
  const firstDataRow = rows[startRowIndex] ?? [];
  let bestIndex = 0;
  let bestScore = -1;

  for (let columnIndex = 0; columnIndex < firstDataRow.length; columnIndex += 1) {
    let score = 0;

    for (
      let rowIndex = startRowIndex;
      rowIndex < rows.length && rowIndex < startRowIndex + 20;
      rowIndex += 1
    ) {
      const value = readStringCell(rows[rowIndex], columnIndex);
      if (value && isValidBidId(value)) {
        score += 1;
      }
    }

    if (score > bestScore) {
      bestScore = score;
      bestIndex = columnIndex;
    }
  }

  return {
    index: bestIndex,
    score: bestScore,
  };
}

function buildColumnOptions(row: string[]) {
  return row.map((value, index) => ({
    key: buildColumnKey(index),
    label: value.trim() || `Column ${index + 1}`,
    index,
  }));
}

function dedupeBidIds(values: string[]) {
  const bidIds: string[] = [];
  const seen = new Set<string>();
  let duplicateCount = 0;

  for (const value of values) {
    if (seen.has(value)) {
      duplicateCount += 1;
      continue;
    }

    seen.add(value);
    bidIds.push(value);
  }

  return {
    bidIds,
    duplicateCount,
  };
}

function parseCsvRows(csvText: string) {
  const result = Papa.parse<unknown[]>(csvText, {
    skipEmptyLines: true,
  });

  const meaningfulErrors = result.errors.filter((error) => {
    return error.type !== "Delimiter";
  });

  if (meaningfulErrors.length > 0) {
    const firstError = meaningfulErrors[0];
    const rowNumber =
      typeof firstError.row === "number" ? firstError.row + 1 : 1;
    throw new Error(
      `Malformed CSV near row ${rowNumber}: ${firstError.message}`,
    );
  }

  return result.data.map((row) => row.map((cell) => String(cell ?? "")));
}

function resolveSelectedColumnIndex(input: {
  rows: string[][];
  selectedColumnKey?: string;
}) {
  const firstRow = input.rows[0] ?? [];
  const secondRow = input.rows[1] ?? [];
  const detectedHeaderIndices = detectHeaderIndex(firstRow);
  const firstRowLikelyHeader =
    (Boolean(input.selectedColumnKey) && firstRow.length > 1) ||
    detectedHeaderIndices.length > 0 ||
    (firstRow.length > 1 &&
      input.rows.length > 1 &&
      countValuesWithDigits(firstRow) < countValuesWithDigits(secondRow));
  const headerDetected = firstRowLikelyHeader;
  const columnOptions: CsvPreviewColumnOption[] = headerDetected
    ? buildColumnOptions(firstRow)
    : [
        {
          key: buildColumnKey(0),
          label: "Column 1",
          index: 0,
        },
      ];

  if (headerDetected && input.selectedColumnKey) {
    const selected = columnOptions.find(
      (option) => option.key === input.selectedColumnKey,
    );

    if (!selected) {
      throw new Error("Selected CSV column was not found in the uploaded file.");
    }

    return {
      headerDetected,
      columnOptions,
      selectedColumnKey: selected.key,
      selectedColumnIndex: selected.index,
    };
  }

  if (headerDetected) {
    const selectedIndex =
      detectedHeaderIndices[0] ?? autoDetectColumnIndex(input.rows, 1).index;
    return {
      headerDetected,
      columnOptions,
      selectedColumnKey: buildColumnKey(selectedIndex),
      selectedColumnIndex: selectedIndex,
    };
  }

  return {
    headerDetected: false,
    columnOptions,
    selectedColumnKey: buildColumnKey(0),
    selectedColumnIndex: 0,
  };
}

export function extractBidIdsFromCsv(input: {
  csvText: string;
  fileName: string;
  selectedColumnKey?: string;
}): CsvPreviewResult & { bidIds: string[] } {
  const rows = parseCsvRows(input.csvText);

  if (rows.length === 0) {
    throw new Error("The uploaded CSV file is empty.");
  }

  if (rows.length > MAX_CSV_BID_IDS + 1) {
    throw new Error(
      `The uploaded CSV exceeds the ${MAX_CSV_BID_IDS} row limit for one import.`,
    );
  }

  const selection = resolveSelectedColumnIndex({
    rows,
    selectedColumnKey: input.selectedColumnKey,
  });
  const dataRows = selection.headerDetected ? rows.slice(1) : rows;

  if (dataRows.length === 0) {
    throw new Error("The uploaded CSV does not contain any data rows.");
  }

  const rawBidIds: string[] = [];
  const invalidRows: Array<{
    rowNumber: number;
    value: string;
    message: string;
  }> = [];

  dataRows.forEach((row, dataIndex) => {
    const value = readStringCell(row, selection.selectedColumnIndex);
    const rowNumber = selection.headerDetected ? dataIndex + 2 : dataIndex + 1;

    if (!value) {
      return;
    }

    if (!isValidBidId(value)) {
      invalidRows.push({
        rowNumber,
        value,
        message: "This value does not look like a valid Bid ID.",
      });
      return;
    }

    rawBidIds.push(value);
  });

  const { bidIds, duplicateCount } = dedupeBidIds(rawBidIds);

  if (bidIds.length === 0) {
    if (invalidRows.length > 0) {
      throw new Error(
        "No valid Bid IDs were found in the uploaded CSV. Review the detected column and row values.",
      );
    }

    throw new Error("No Bid IDs were found in the uploaded CSV.");
  }

  return {
    fileName: input.fileName,
    totalRows: dataRows.length,
    validBidIdCount: bidIds.length,
    duplicateCount,
    invalidRowCount: invalidRows.length,
    selectedColumnKey: selection.selectedColumnKey,
    headerDetected: selection.headerDetected,
    columnOptions: selection.columnOptions,
    previewBidIds: bidIds.slice(0, 10),
    invalidRows: invalidRows.slice(0, 20),
    bidIds,
  };
}

export async function previewCsvUpload(input: {
  file: File;
  selectedColumnKey?: string;
}) {
  if (input.file.size === 0) {
    throw new Error("The uploaded CSV file is empty.");
  }

  if (input.file.size > MAX_CSV_UPLOAD_BYTES) {
    throw new Error(
      `The uploaded CSV exceeds the ${Math.floor(
        MAX_CSV_UPLOAD_BYTES / (1024 * 1024),
      )} MB file size limit.`,
    );
  }

  const csvText = await input.file.text();

  return extractBidIdsFromCsv({
    csvText,
    fileName: input.file.name,
    selectedColumnKey: input.selectedColumnKey,
  });
}

export async function createImportRunFromCsvUpload(input: {
  file: File;
  selectedColumnKey?: string;
  forceRefresh: boolean;
}) {
  const preview = await previewCsvUpload({
    file: input.file,
    selectedColumnKey: input.selectedColumnKey,
  });

  const importRun = await createAsyncImportRun({
    bidIds: preview.bidIds,
    forceRefresh: input.forceRefresh,
    sourceType: "csv_upload",
    notes: `CSV upload import from ${preview.fileName}.`,
  });

  return {
    preview,
    importRun,
  };
}
