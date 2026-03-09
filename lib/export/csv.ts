import { stringifyJson } from "@/lib/utils/json";

function escapeCsvValue(value: unknown) {
  if (value === null || value === undefined) {
    return "";
  }

  const stringValue =
    typeof value === "string" ? value : stringifyJson(value);

  const needsQuotes =
    stringValue.includes(",") ||
    stringValue.includes('"') ||
    stringValue.includes("\n");

  if (!needsQuotes) {
    return stringValue;
  }

  return `"${stringValue.split('"').join('""')}"`;
}

export function buildCsv<T extends Record<string, unknown>>(
  rows: T[],
  columns: Array<keyof T>,
) {
  const header = columns.join(",");
  const lines = rows.map((row) =>
    columns.map((column) => escapeCsvValue(row[column])).join(","),
  );

  return [header, ...lines].join("\n");
}
