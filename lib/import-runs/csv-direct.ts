import "server-only";

import Papa from "papaparse";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { isValid, parse } from "date-fns";

import {
  CSV_DIRECT_CHUNK_SIZE,
  MAX_CSV_DIRECT_ROWS,
  MAX_CSV_DIRECT_UPLOAD_BYTES,
} from "@/lib/import-runs/csv-direct-constants";
import {
  createImportRun,
  getImportRunDetail,
  markImportRunFailed,
  updateImportRunSourceState,
  updateImportRunTotals,
} from "@/lib/db/import-runs";
import {
  createImportSourceFile,
  findLatestImportSourceFileByContentHash,
  getImportSourceRowForBidId,
  insertImportSourceRowsWithRunItemsBatch,
  updateImportSourceFile,
} from "@/lib/db/import-sources";
import { buildCsvDiagnosis } from "@/lib/diagnostics/csv-direct";
import { getInvestigationByBidId, upsertInvestigation } from "@/lib/db/investigations";
import { isValidBidId } from "@/lib/utils/bid-id";
import type {
  CsvDirectDuplicateImportMatch,
  CsvDirectHeaderMappingEntry,
  CsvDirectImportSummary,
  CsvDirectPreviewResult,
  CsvDirectSourceMetadata,
} from "@/types/import-run";
import type { NormalizedBidData } from "@/types/bid";

const HEADER_ALIASES = {
  bidId: ["bidid", "bid_id", "bid id"],
  bidDate: ["biddate", "bid date", "bid_dt"],
  campaign: ["campaign"],
  publisher: ["publisher"],
  campaignId: ["campaignid", "campaign id"],
  publisherId: ["publisherid", "publisher id"],
  bidAmount: ["bid"],
  winningBid: ["winningbid", "winning bid"],
  bidRejected: ["bidrejected", "bid rejected"],
  reasonForReject: ["reasonforrejection", "reason for rejection"],
  bidDid: ["biddid", "bid did"],
  bidExpireDate: ["bidexpiredate", "bid expire date"],
  expirationSeconds: ["expirationinseconds", "expiration in seconds"],
  winningBidCallAccepted: [
    "winningbidcallaccepted",
    "winning bid - call accepted",
    "winning bid call accepted",
  ],
  winningBidCallRejected: [
    "winningbidcallrejected",
    "winning bid - call rejected",
    "winning bid call rejected",
  ],
  bidElapsedMs: ["bidelapsedms", "bid elapsed ms"],
} as const;

const BID_DATE_FORMATS = ["MM/dd/yyyy hh:mm:ss a", "M/d/yyyy h:mm:ss a"];
const METADATA_UPDATE_INTERVAL = 5000;

const CSV_DIRECT_ERROR_CODES = {
  emptyFile: "csv_direct_empty_file",
  fileTooLarge: "csv_direct_file_too_large",
  malformedCsv: "csv_direct_malformed_csv",
  missingHeaderRow: "csv_direct_missing_header_row",
  missingBidIdColumn: "csv_direct_missing_bid_id_column",
  missingDataRows: "csv_direct_missing_data_rows",
  rowLimitExceeded: "csv_direct_row_limit_exceeded",
  duplicateUpload: "csv_direct_duplicate_upload",
  processingFailed: "csv_direct_processing_failed",
} as const;

type CsvDirectErrorCode =
  (typeof CSV_DIRECT_ERROR_CODES)[keyof typeof CSV_DIRECT_ERROR_CODES];

class CsvDirectImportError extends Error {
  status: number;
  code: CsvDirectErrorCode;
  details: Record<string, unknown>;

  constructor(input: {
    message: string;
    status: number;
    code: CsvDirectErrorCode;
    details?: Record<string, unknown>;
  }) {
    super(input.message);
    this.name = "CsvDirectImportError";
    this.status = input.status;
    this.code = input.code;
    this.details = input.details ?? {};
  }
}

export function isCsvDirectImportError(error: unknown): error is CsvDirectImportError {
  return error instanceof CsvDirectImportError;
}

function createCsvDirectImportError(input: {
  message: string;
  status: number;
  code: CsvDirectErrorCode;
  details?: Record<string, unknown>;
}) {
  return new CsvDirectImportError(input);
}

function createMalformedCsvError(rowNumber: number, message: string) {
  return createCsvDirectImportError({
    message: `Malformed CSV near row ${rowNumber}: ${message}`,
    status: 422,
    code: CSV_DIRECT_ERROR_CODES.malformedCsv,
    details: {
      rowNumber,
    },
  });
}

function createDuplicateUploadError(match: CsvDirectDuplicateImportMatch) {
  return createCsvDirectImportError({
    message: `This CSV was already imported on ${match.createdAt} in run ${match.importRunId}.`,
    status: 409,
    code: CSV_DIRECT_ERROR_CODES.duplicateUpload,
    details: {
      duplicateImport: match,
    },
  });
}

function validateCsvDirectFile(file: File) {
  if (file.size === 0) {
    throw createCsvDirectImportError({
      message: "The uploaded CSV file is empty.",
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.emptyFile,
    });
  }

  if (file.size > MAX_CSV_DIRECT_UPLOAD_BYTES) {
    throw createCsvDirectImportError({
      message: `The uploaded CSV exceeds the ${Math.floor(
        MAX_CSV_DIRECT_UPLOAD_BYTES / (1024 * 1024),
      )} MB file size limit.`,
      status: 413,
      code: CSV_DIRECT_ERROR_CODES.fileTooLarge,
      details: {
        maxBytes: MAX_CSV_DIRECT_UPLOAD_BYTES,
        fileSizeBytes: file.size,
      },
    });
  }
}

function stripLeadingBom(value: string) {
  if (value.length === 0) {
    return value;
  }

  return value.charCodeAt(0) === 65279 ? value.slice(1) : value;
}

function mapDuplicateMatch(
  match: Awaited<ReturnType<typeof findLatestImportSourceFileByContentHash>>,
): CsvDirectDuplicateImportMatch | null {
  if (!match || !match.contentHash) {
    return null;
  }

  return {
    importRunId: match.importRunId,
    sourceFileId: match.id,
    fileName: match.fileName,
    rowCount: match.rowCount,
    createdAt: match.createdAt,
    contentHash: match.contentHash,
  };
}

async function hashFileContent(file: File) {
  const buffer = Buffer.from(await file.arrayBuffer());
  return createHash("sha256").update(buffer).digest("hex");
}

async function findDuplicateCsvDirectUpload(contentHash: string) {
  const match = await findLatestImportSourceFileByContentHash({
    sourceType: "csv_direct_import",
    contentHash,
  });

  return mapDuplicateMatch(match);
}

function normalizeHeaderValue(value: string) {
  let normalized = "";

  for (const character of stripLeadingBom(value).trim().toLowerCase()) {
    if (character === " " || character === "_" || character === "-") {
      continue;
    }

    normalized += character;
  }

  return normalized;
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
    throw createMalformedCsvError(rowNumber, firstError.message);
  }

  return result.data.map((row) => row.map((cell) => String(cell ?? "")));
}

function findMappedFieldForHeader(normalizedHeader: string) {
  for (const [fieldName, aliases] of Object.entries(HEADER_ALIASES)) {
    for (const alias of aliases) {
      if (normalizeHeaderValue(alias) === normalizedHeader) {
        return fieldName;
      }
    }
  }

  return null;
}

function buildStoredHeaderKey(input: {
  header: string;
  columnIndex: number;
  duplicateIndex: number;
}) {
  const trimmedHeader = stripLeadingBom(input.header).trim();
  const baseKey = trimmedHeader || `Column ${input.columnIndex + 1}`;

  if (input.duplicateIndex === 1) {
    return baseKey;
  }

  return `${baseKey} (${input.duplicateIndex})`;
}

function buildHeaderMapping(headers: string[]): CsvDirectHeaderMappingEntry[] {
  const seenHeaders = new Map<string, number>();

  return headers.map((header, columnIndex) => {
    const normalizedHeader = normalizeHeaderValue(header);
    const seenCount = (seenHeaders.get(normalizedHeader) ?? 0) + 1;
    seenHeaders.set(normalizedHeader, seenCount);

    return {
      columnIndex,
      sourceHeader: stripLeadingBom(header),
      normalizedHeader,
      storedKey: buildStoredHeaderKey({
        header,
        columnIndex,
        duplicateIndex: seenCount,
      }),
      mappedField: findMappedFieldForHeader(normalizedHeader),
      duplicateIndex: seenCount,
    };
  });
}

function buildHeaderIndexMap(headers: string[]) {
  const normalizedHeaders = headers.map((value) => normalizeHeaderValue(value));
  const indexMap = new Map<string, number>();

  normalizedHeaders.forEach((value, index) => {
    if (!indexMap.has(value)) {
      indexMap.set(value, index);
    }
  });

  function findIndex(aliases: readonly string[]) {
    for (const alias of aliases) {
      const normalized = normalizeHeaderValue(alias);
      const index = indexMap.get(normalized);
      if (typeof index === "number") {
        return index;
      }
    }

    return null;
  }

  return {
    bidId: findIndex(HEADER_ALIASES.bidId),
    bidDate: findIndex(HEADER_ALIASES.bidDate),
    campaign: findIndex(HEADER_ALIASES.campaign),
    publisher: findIndex(HEADER_ALIASES.publisher),
    campaignId: findIndex(HEADER_ALIASES.campaignId),
    publisherId: findIndex(HEADER_ALIASES.publisherId),
    bidAmount: findIndex(HEADER_ALIASES.bidAmount),
    winningBid: findIndex(HEADER_ALIASES.winningBid),
    bidRejected: findIndex(HEADER_ALIASES.bidRejected),
    reasonForReject: findIndex(HEADER_ALIASES.reasonForReject),
    bidDid: findIndex(HEADER_ALIASES.bidDid),
    bidExpireDate: findIndex(HEADER_ALIASES.bidExpireDate),
    expirationSeconds: findIndex(HEADER_ALIASES.expirationSeconds),
    winningBidCallAccepted: findIndex(HEADER_ALIASES.winningBidCallAccepted),
    winningBidCallRejected: findIndex(HEADER_ALIASES.winningBidCallRejected),
    bidElapsedMs: findIndex(HEADER_ALIASES.bidElapsedMs),
  };
}

function readStringCell(row: string[], index: number | null) {
  if (index === null || index < 0 || index >= row.length) {
    return "";
  }

  return row[index]?.trim() ?? "";
}

function parseNumber(value: string) {
  if (!value) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseInteger(value: string) {
  const parsed = parseNumber(value);
  return parsed === null ? null : Math.trunc(parsed);
}

function parseBoolean(value: string) {
  if (!value) {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === "true") {
    return true;
  }

  if (normalized === "false") {
    return false;
  }

  return null;
}

function parseBidDate(value: string) {
  if (!value) {
    return null;
  }

  for (const format of BID_DATE_FORMATS) {
    const parsed = parse(value, format, new Date());
    if (isValid(parsed)) {
      return parsed.toISOString();
    }
  }

  return null;
}

function buildRowObject(headerMapping: CsvDirectHeaderMappingEntry[], row: string[]) {
  const result: Record<string, string> = {};

  headerMapping.forEach((mapping, index) => {
    result[mapping.storedKey] = row[index]?.trim() ?? "";
  });

  return result;
}

function updateBidDateBounds(input: {
  earliestBidDt: string | null;
  latestBidDt: string | null;
  bidDt: string | null;
}) {
  if (!input.bidDt) {
    return {
      earliestBidDt: input.earliestBidDt,
      latestBidDt: input.latestBidDt,
    };
  }

  return {
    earliestBidDt: input.earliestBidDt
      ? input.earliestBidDt < input.bidDt
        ? input.earliestBidDt
        : input.bidDt
      : input.bidDt,
    latestBidDt: input.latestBidDt
      ? input.latestBidDt > input.bidDt
        ? input.latestBidDt
        : input.bidDt
      : input.bidDt,
  };
}

export interface CsvDirectParsedRow {
  rowNumber: number;
  bidId: string | null;
  bidIdValid: boolean;
  bidDt: string | null;
  bidDtRaw: string;
  campaignName: string | null;
  campaignId: string | null;
  publisherName: string | null;
  publisherId: string | null;
  bidAmount: number | null;
  winningBid: number | null;
  bidRejected: boolean | null;
  reasonForReject: string | null;
  bidDid: string | null;
  bidExpireDate: string | null;
  expirationSeconds: number | null;
  winningBidCallAccepted: boolean | null;
  winningBidCallRejected: boolean | null;
  bidElapsedMs: number | null;
  ingestStatus: string;
  ingestErrorCode: string | null;
  ingestErrorMessage: string | null;
  rowJson: Record<string, string>;
}

function buildParsedRow(input: {
  row: string[];
  rowNumber: number;
  headerMap: ReturnType<typeof buildHeaderIndexMap>;
  headerMapping: CsvDirectHeaderMappingEntry[];
}): CsvDirectParsedRow {
  const bidIdValue = readStringCell(input.row, input.headerMap.bidId);
  const bidIdValid = Boolean(bidIdValue) && isValidBidId(bidIdValue);
  const bidDateRaw = readStringCell(input.row, input.headerMap.bidDate);
  const rowJson = buildRowObject(input.headerMapping, input.row);

  return {
    rowNumber: input.rowNumber,
    bidId: bidIdValid ? bidIdValue : bidIdValue || null,
    bidIdValid,
    bidDt: parseBidDate(bidDateRaw),
    bidDtRaw: bidDateRaw,
    campaignName: readStringCell(input.row, input.headerMap.campaign) || null,
    campaignId: readStringCell(input.row, input.headerMap.campaignId) || null,
    publisherName: readStringCell(input.row, input.headerMap.publisher) || null,
    publisherId: readStringCell(input.row, input.headerMap.publisherId) || null,
    bidAmount: parseNumber(readStringCell(input.row, input.headerMap.bidAmount)),
    winningBid: parseNumber(readStringCell(input.row, input.headerMap.winningBid)),
    bidRejected: parseBoolean(readStringCell(input.row, input.headerMap.bidRejected)),
    reasonForReject: readStringCell(input.row, input.headerMap.reasonForReject) || null,
    bidDid: readStringCell(input.row, input.headerMap.bidDid) || null,
    bidExpireDate: readStringCell(input.row, input.headerMap.bidExpireDate) || null,
    expirationSeconds: parseInteger(readStringCell(input.row, input.headerMap.expirationSeconds)),
    winningBidCallAccepted: parseBoolean(
      readStringCell(input.row, input.headerMap.winningBidCallAccepted),
    ),
    winningBidCallRejected: parseBoolean(
      readStringCell(input.row, input.headerMap.winningBidCallRejected),
    ),
    bidElapsedMs: parseInteger(readStringCell(input.row, input.headerMap.bidElapsedMs)),
    ingestStatus: "queued",
    ingestErrorCode: null,
    ingestErrorMessage: null,
    rowJson,
  };
}

function parseCsvDirectRows(csvText: string) {
  const rows = parseCsvRows(csvText);

  if (rows.length === 0) {
    throw createCsvDirectImportError({
      message: "The uploaded CSV file is empty.",
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.emptyFile,
    });
  }

  if (rows.length > MAX_CSV_DIRECT_ROWS + 1) {
    throw createCsvDirectImportError({
      message: `The uploaded CSV exceeds the ${MAX_CSV_DIRECT_ROWS} row limit for direct import.`,
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.rowLimitExceeded,
      details: {
        maxRows: MAX_CSV_DIRECT_ROWS,
      },
    });
  }

  const headerRow = rows[0] ?? [];

  if (headerRow.length === 0) {
    throw createCsvDirectImportError({
      message: "The uploaded CSV file does not contain a header row.",
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.missingHeaderRow,
    });
  }

  const headerMap = buildHeaderIndexMap(headerRow);
  const headerMapping = buildHeaderMapping(headerRow);

  if (headerMap.bidId === null) {
    throw createCsvDirectImportError({
      message: "The uploaded CSV file does not include a Bid ID column.",
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.missingBidIdColumn,
    });
  }

  const dataRows = rows.slice(1);

  if (dataRows.length === 0) {
    throw createCsvDirectImportError({
      message: "The uploaded CSV does not contain any data rows.",
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.missingDataRows,
    });
  }

  const parsedRows: CsvDirectParsedRow[] = [];

  dataRows.forEach((row, index) => {
    parsedRows.push(
      buildParsedRow({
        row,
        rowNumber: index + 2,
        headerMap,
        headerMapping,
      }),
    );
  });

  return {
    headers: headerRow,
    headerMapping,
    parsedRows,
  };
}

function summarizeParsedRows(parsedRows: CsvDirectParsedRow[]) {
  const seen = new Set<string>();
  let duplicateBidIdCount = 0;
  let missingBidIdCount = 0;
  let invalidBidIdCount = 0;
  let earliestBidDt: string | null = null;
  let latestBidDt: string | null = null;

  const invalidRows: CsvDirectPreviewResult["invalidRows"] = [];

  for (const row of parsedRows) {
    if (!row.bidId) {
      missingBidIdCount += 1;
      continue;
    }

    if (!row.bidIdValid) {
      invalidBidIdCount += 1;
      invalidRows.push({
        rowNumber: row.rowNumber,
        value: row.bidId,
        message: "This value does not look like a valid Bid ID.",
      });
      continue;
    }

    if (seen.has(row.bidId)) {
      duplicateBidIdCount += 1;
    } else {
      seen.add(row.bidId);
    }

    const nextBounds = updateBidDateBounds({
      earliestBidDt,
      latestBidDt,
      bidDt: row.bidDt,
    });
    earliestBidDt = nextBounds.earliestBidDt;
    latestBidDt = nextBounds.latestBidDt;
  }

  return {
    queuedItemCount: seen.size,
    rejectedRowCount: missingBidIdCount + invalidBidIdCount,
    skippedDuplicateRowCount: duplicateBidIdCount,
    duplicateBidIdCount,
    missingBidIdCount,
    invalidBidIdCount,
    earliestBidDt,
    latestBidDt,
    invalidRows: invalidRows.slice(0, 20),
  };
}

function buildCsvDirectImportSummary(input: {
  fileName: string;
  contentHash: string;
  parsedRowCount: number;
  queuedItemCount: number;
  validBidIdCount: number;
  duplicateBidIdCount: number;
  missingBidIdCount: number;
  invalidBidIdCount: number;
  earliestBidDt: string | null;
  latestBidDt: string | null;
}): CsvDirectImportSummary {
  return {
    fileName: input.fileName,
    contentHash: input.contentHash,
    storedRowCount: input.parsedRowCount,
    parsedRowCount: input.parsedRowCount,
    queuedItemCount: input.queuedItemCount,
    rejectedRowCount: input.missingBidIdCount + input.invalidBidIdCount,
    skippedDuplicateRowCount: input.duplicateBidIdCount,
    validBidIdCount: input.validBidIdCount,
    duplicateBidIdCount: input.duplicateBidIdCount,
    missingBidIdCount: input.missingBidIdCount,
    invalidBidIdCount: input.invalidBidIdCount,
    earliestBidDt: input.earliestBidDt,
    latestBidDt: input.latestBidDt,
  };
}

function markRowRejected(
  row: CsvDirectParsedRow,
  input: {
    code: string;
    message: string;
  },
) {
  row.ingestStatus = "rejected";
  row.ingestErrorCode = input.code;
  row.ingestErrorMessage = input.message;
}

function markRowSkippedDuplicate(row: CsvDirectParsedRow) {
  row.ingestStatus = "skipped_duplicate";
  row.ingestErrorCode = "duplicate_bid_id";
  row.ingestErrorMessage = "A previous row already queued this Bid ID for import.";
}

function createCsvDirectPreviewAccumulator(fileName: string, contentHash: string) {
  let headers: string[] = [];
  let duplicateImport: CsvDirectDuplicateImportMatch | null = null;
  let totalRows = 0;
  let validBidIdCount = 0;
  let missingBidIdCount = 0;
  let duplicateBidIdCount = 0;
  let invalidBidIdCount = 0;
  let earliestBidDt: string | null = null;
  let latestBidDt: string | null = null;
  const seen = new Set<string>();
  const sampleRows: CsvDirectPreviewResult["sampleRows"] = [];
  const invalidRows: CsvDirectPreviewResult["invalidRows"] = [];

  return {
    setHeaders(nextHeaders: string[]) {
      headers = nextHeaders;
    },
    setDuplicateImport(match: CsvDirectDuplicateImportMatch | null) {
      duplicateImport = match;
    },
    addRow(row: CsvDirectParsedRow) {
      totalRows += 1;

      if (sampleRows.length < 10) {
        sampleRows.push({
          rowNumber: row.rowNumber,
          bidId: row.bidId,
          bidDt: row.bidDt,
          campaignName: row.campaignName,
          publisherName: row.publisherName,
          bidAmount: row.bidAmount,
          reasonForReject: row.reasonForReject,
        });
      }

      const nextBounds = updateBidDateBounds({
        earliestBidDt,
        latestBidDt,
        bidDt: row.bidDt,
      });
      earliestBidDt = nextBounds.earliestBidDt;
      latestBidDt = nextBounds.latestBidDt;

      if (!row.bidId) {
        missingBidIdCount += 1;
        return;
      }

      if (!row.bidIdValid) {
        invalidBidIdCount += 1;
        if (invalidRows.length < 20) {
          invalidRows.push({
            rowNumber: row.rowNumber,
            value: row.bidId,
            message: "This value does not look like a valid Bid ID.",
          });
        }
        return;
      }

      validBidIdCount += 1;

      if (seen.has(row.bidId)) {
        duplicateBidIdCount += 1;
        return;
      }

      seen.add(row.bidId);
    },
    buildPreview(): CsvDirectPreviewResult {
      return {
        fileName,
        contentHash,
        totalRows,
        validBidIdCount,
        queuedItemCount: seen.size,
        rejectedRowCount: missingBidIdCount + invalidBidIdCount,
        skippedDuplicateRowCount: duplicateBidIdCount,
        missingBidIdCount,
        duplicateBidIdCount,
        invalidBidIdCount,
        earliestBidDt,
        latestBidDt,
        headers,
        duplicateImport,
        sampleRows,
        invalidRows,
      };
    },
  };
}

function buildCsvDirectSourceMetadata(input: {
  fileName: string;
  contentHash: string;
  parsedRowCount: number;
  validBidIdCount: number;
  queuedItemCount: number;
  duplicateBidIdCount: number;
  missingBidIdCount: number;
  invalidBidIdCount: number;
  earliestBidDt: string | null;
  latestBidDt: string | null;
  headerRow: string[];
  headerMapping: CsvDirectHeaderMappingEntry[];
  duplicateImport: CsvDirectDuplicateImportMatch | null;
  warnings?: string[];
  lastError?: string;
}): CsvDirectSourceMetadata {
  return {
    ...buildCsvDirectImportSummary({
      fileName: input.fileName,
      contentHash: input.contentHash,
      parsedRowCount: input.parsedRowCount,
      queuedItemCount: input.queuedItemCount,
      validBidIdCount: input.validBidIdCount,
      duplicateBidIdCount: input.duplicateBidIdCount,
      missingBidIdCount: input.missingBidIdCount,
      invalidBidIdCount: input.invalidBidIdCount,
      earliestBidDt: input.earliestBidDt,
      latestBidDt: input.latestBidDt,
    }),
    parsedHeaders: input.headerRow,
    headerMapping: input.headerMapping,
    duplicateImport: input.duplicateImport,
    warnings: input.warnings ?? [],
    lastError: input.lastError,
  };
}

export function buildCsvDirectPreview(input: {
  csvText: string;
  fileName: string;
}): CsvDirectPreviewResult {
  const contentHash = createHash("sha256").update(input.csvText).digest("hex");
  const { headers, parsedRows } = parseCsvDirectRows(input.csvText);
  const summary = summarizeParsedRows(parsedRows);
  const sampleRows = parsedRows.slice(0, 10).map((row) => ({
    rowNumber: row.rowNumber,
    bidId: row.bidId,
    bidDt: row.bidDt,
    campaignName: row.campaignName,
    publisherName: row.publisherName,
    bidAmount: row.bidAmount,
    reasonForReject: row.reasonForReject,
  }));

  const validBidIdCount = parsedRows.filter((row) => row.bidIdValid).length;

  return {
    fileName: input.fileName,
    contentHash,
    totalRows: parsedRows.length,
    validBidIdCount,
    queuedItemCount: summary.queuedItemCount,
    rejectedRowCount: summary.rejectedRowCount,
    skippedDuplicateRowCount: summary.skippedDuplicateRowCount,
    missingBidIdCount: summary.missingBidIdCount,
    duplicateBidIdCount: summary.duplicateBidIdCount,
    invalidBidIdCount: summary.invalidBidIdCount,
    earliestBidDt: summary.earliestBidDt,
    latestBidDt: summary.latestBidDt,
    headers,
    duplicateImport: null,
    sampleRows,
    invalidRows: summary.invalidRows,
  };
}

export async function previewCsvDirectUpload(input: { file: File }) {
  validateCsvDirectFile(input.file);

  const contentHash = await hashFileContent(input.file);
  const duplicateImport = await findDuplicateCsvDirectUpload(contentHash);
  const previewAccumulator = createCsvDirectPreviewAccumulator(input.file.name, contentHash);
  previewAccumulator.setDuplicateImport(duplicateImport);
  let hasDataRows = false;
  let parsedRowCount = 0;

  await streamCsvRows({
    file: input.file,
    onHeader: (header) => {
      previewAccumulator.setHeaders(header);
    },
    onRow: async (row, rowNumber, _headerRow, headerMap, headerMapping) => {
      parsedRowCount += 1;
      if (parsedRowCount > MAX_CSV_DIRECT_ROWS) {
        throw createCsvDirectImportError({
          message: `The uploaded CSV exceeds the ${MAX_CSV_DIRECT_ROWS} row limit for direct import.`,
          status: 422,
          code: CSV_DIRECT_ERROR_CODES.rowLimitExceeded,
          details: {
            maxRows: MAX_CSV_DIRECT_ROWS,
          },
        });
      }

      hasDataRows = true;
      previewAccumulator.addRow(
        buildParsedRow({
          row,
          rowNumber,
          headerMap,
          headerMapping,
        }),
      );
    },
  });

  if (!hasDataRows) {
    throw createCsvDirectImportError({
      message: "The uploaded CSV does not contain any data rows.",
      status: 422,
      code: CSV_DIRECT_ERROR_CODES.missingDataRows,
    });
  }

  return previewAccumulator.buildPreview();
}

async function streamCsvRows(input: {
  file: File;
  onHeader: (header: string[], headerMapping: CsvDirectHeaderMappingEntry[]) => void;
  onRow: (
    row: string[],
    rowNumber: number,
    header: string[],
    headerMap: ReturnType<typeof buildHeaderIndexMap>,
    headerMapping: CsvDirectHeaderMappingEntry[],
  ) => Promise<void>;
}) {
  const stream = Readable.fromWeb(
    input.file.stream() as unknown as NodeReadableStream<Uint8Array>,
  );
  const parser = Papa.parse(Papa.NODE_STREAM_INPUT, {
    skipEmptyLines: true,
  });

  let headerRow: string[] | null = null;
  let headerMap: ReturnType<typeof buildHeaderIndexMap> | null = null;
  let headerMapping: CsvDirectHeaderMappingEntry[] | null = null;
  let rowNumber = 0;
  let pendingRows = 0;
  let streamEnded = false;
  let settled = false;

  return new Promise<void>((resolve, reject) => {
    function settleWithError(error: Error) {
      if (settled) {
        return;
      }
      settled = true;
      stream.unpipe(parser);
      parser.destroy();
      stream.destroy();
      reject(error);
    }

    function settleIfDone() {
      if (settled) {
        return;
      }
      if (streamEnded && pendingRows === 0) {
        settled = true;
        resolve();
      }
    }

    parser.on("data", (data: unknown) => {
      parser.pause();
      const row = Array.isArray(data)
        ? data
        : Array.isArray((data as { data?: unknown }).data)
          ? ((data as { data?: unknown }).data as string[])
          : null;
      if (!row) {
        if (!settled) {
          parser.resume();
        }
        return;
      }
      rowNumber += 1;
      if (!headerRow) {
        headerRow = row.map((value, index) => {
          const text = String(value ?? "");
          return index === 0 ? stripLeadingBom(text) : text;
        });
        if (headerRow.length === 0) {
          settleWithError(
            createCsvDirectImportError({
              message: "The uploaded CSV file does not contain a header row.",
              status: 422,
              code: CSV_DIRECT_ERROR_CODES.missingHeaderRow,
            }),
          );
          return;
        }
        headerMap = buildHeaderIndexMap(headerRow);
        headerMapping = buildHeaderMapping(headerRow);
        if (headerMap.bidId === null) {
          settleWithError(
            createCsvDirectImportError({
              message: "The uploaded CSV file does not include a Bid ID column.",
              status: 422,
              code: CSV_DIRECT_ERROR_CODES.missingBidIdColumn,
            }),
          );
          return;
        }
        input.onHeader(headerRow, headerMapping);
        if (!settled) {
          parser.resume();
        }
        return;
      }
      const map = headerMap;
      const mapping = headerMapping;
      if (!map || !headerRow || !mapping) {
        settleWithError(
          createCsvDirectImportError({
            message: "Unable to read CSV header row.",
            status: 500,
            code: CSV_DIRECT_ERROR_CODES.processingFailed,
          }),
        );
        return;
      }
      pendingRows += 1;
      void input
        .onRow(row.map((value) => String(value ?? "")), rowNumber, headerRow, map, mapping)
        .then(() => {
          pendingRows -= 1;
          if (!settled) {
            parser.resume();
          }
          settleIfDone();
        })
        .catch((error) => {
          settleWithError(error instanceof Error ? error : new Error(String(error)));
        });
    });
    parser.on("error", (error: Error) => {
      settleWithError(createMalformedCsvError(rowNumber || 1, error.message));
    });
    parser.on("end", () => {
      streamEnded = true;
      settleIfDone();
    });
    stream.pipe(parser);
  });
}

function buildNormalizedBid(row: CsvDirectParsedRow): NormalizedBidData {
  const bidAmount = row.bidAmount;
  const winningBid = row.winningBid;
  const lowerRejectReason = String(row.reasonForReject ?? "").toLowerCase();
  const isZeroBid =
    bidAmount === 0 ||
    winningBid === 0 ||
    lowerRejectReason.includes("zero bid");
  const outcome =
    row.bidRejected === true || row.reasonForReject
      ? "rejected"
      : isZeroBid
        ? "zero_bid"
        : bidAmount !== null || winningBid !== null
          ? "accepted"
          : "unknown";
  const outcomeReasonCategory =
    outcome === "accepted"
      ? "accepted"
      : lowerRejectReason.includes("caller_id_required")
        ? "missing_caller_id"
        : lowerRejectReason.includes("initial tag filter") || lowerRejectReason.includes("1002")
          ? "tag_filtered_initial"
          : lowerRejectReason.includes("final capacity check") ||
              lowerRejectReason.includes("final tag filter") ||
              lowerRejectReason.includes("1006")
            ? "tag_filtered_final"
            : lowerRejectReason.includes("zero bid")
              ? "buyer_returned_zero_bid"
              : row.reasonForReject
                ? "unknown_no_payable_bid"
                : null;
  const classificationSource =
    outcomeReasonCategory === "accepted"
      ? "heuristic"
      : row.reasonForReject
        ? "reason_for_reject_text"
        : null;

  return {
    bidId: row.bidId ?? "",
    bidDt: row.bidDt,
    campaignName: row.campaignName,
    campaignId: row.campaignId,
    publisherName: row.publisherName,
    publisherId: row.publisherId,
    targetName: null,
    targetId: null,
    buyerName: null,
    buyerId: null,
    bidAmount,
    winningBid,
    bidElapsedMs: row.bidElapsedMs,
    isZeroBid,
    reasonForReject: row.reasonForReject,
    httpStatusCode: null,
    errorMessage: null,
    primaryFailureStage:
      outcome === "accepted"
        ? "accepted"
        : outcome === "zero_bid"
          ? "zero_bid"
          : outcome === "rejected"
            ? "target_rejected"
            : "unknown",
    primaryTargetName: null,
    primaryTargetId: null,
    primaryBuyerName: null,
    primaryBuyerId: null,
    primaryErrorCode: null,
    primaryErrorMessage: row.reasonForReject,
    requestBody: null,
    responseBody: null,
    rawTraceJson: {
      sourceType: "csv_direct_import",
      sourceCsvRow: row.rowJson,
      bidExpireDate: row.bidExpireDate,
      expirationSeconds: row.expirationSeconds,
      bidDid: row.bidDid,
      winningBidCallAccepted: row.winningBidCallAccepted,
      winningBidCallRejected: row.winningBidCallRejected,
      bidElapsedMs: row.bidElapsedMs,
    },
    relevantEvents: [],
    targetAttempts: [],
    outcome,
    outcomeReasonCategory,
    outcomeReasonCode: row.reasonForReject && lowerRejectReason.includes("1006")
      ? "1006"
      : row.reasonForReject && lowerRejectReason.includes("1002")
        ? "1002"
        : null,
    outcomeReasonMessage: row.reasonForReject,
    classificationSource,
    classificationConfidence: classificationSource === null ? null : 0.72,
    classificationWarnings: [],
    parseStatus: "complete",
    normalizationVersion: "csv-direct-v1",
    schemaVariant: "csv_direct_row",
    normalizationConfidence: 1,
    normalizationWarnings: [],
    missingCriticalFields: [],
    missingOptionalFields: [],
    unknownEventNames: [],
    rawPathsUsed: {
      bidId: ["csv.bidId"],
      bidDt: ["csv.bidDt"],
      campaignId: ["csv.campaignId"],
      publisherId: ["csv.publisherId"],
      reasonForReject: ["csv.reasonForReject"],
    },
    primaryErrorCodeSource: null,
    primaryErrorCodeConfidence: null,
    primaryErrorCodeRawMatch: null,
  };
}

export async function createImportRunFromCsvDirectUpload(input: {
  file: File;
  forceRefresh: boolean;
  allowDuplicate?: boolean;
}) {
  validateCsvDirectFile(input.file);

  const contentHash = await hashFileContent(input.file);
  const duplicateImport = await findDuplicateCsvDirectUpload(contentHash);

  if (duplicateImport && !input.allowDuplicate) {
    throw createDuplicateUploadError(duplicateImport);
  }

  let importRunId: string | null = null;
  let sourceFileId: string | null = null;
  let headerRow: string[] = [];
  let headerMapping: CsvDirectHeaderMappingEntry[] = [];
  let parsedRowCount = 0;
  let validBidIdCount = 0;
  let duplicateBidIdCount = 0;
  let missingBidIdCount = 0;
  let invalidBidIdCount = 0;
  let earliestBidDt: string | null = null;
  let latestBidDt: string | null = null;
  const seenBidIds = new Set<string>();
  let pendingRows: CsvDirectParsedRow[] = [];
  let pendingBidIds: string[] = [];
  let nextPosition = 1;

  function buildCurrentSourceMetadata(lastError?: string) {
    return buildCsvDirectSourceMetadata({
      fileName: input.file.name,
      contentHash,
      parsedRowCount,
      queuedItemCount: seenBidIds.size,
      validBidIdCount,
      duplicateBidIdCount,
      missingBidIdCount,
      invalidBidIdCount,
      earliestBidDt,
      latestBidDt,
      headerRow,
      headerMapping,
      duplicateImport,
      warnings: duplicateImport ? ["duplicate_upload_detected"] : [],
      lastError,
    });
  }

  async function flushPending() {
    if (!importRunId || !sourceFileId) {
      throw new Error("Unable to flush CSV import rows before the run is initialized.");
    }

    if (pendingRows.length === 0 && pendingBidIds.length === 0) {
      return;
    }

    const insertResult = await insertImportSourceRowsWithRunItemsBatch({
      importRunId,
      importSourceFileId: sourceFileId,
      rows: pendingRows.map((row) => ({
        rowNumber: row.rowNumber,
        bidId: row.bidId,
        bidDt: row.bidDt,
        campaignName: row.campaignName,
        campaignId: row.campaignId,
        publisherName: row.publisherName,
        publisherId: row.publisherId,
        bidAmount: row.bidAmount,
        winningBid: row.winningBid,
        bidRejected: row.bidRejected,
        reasonForReject: row.reasonForReject,
        bidDid: row.bidDid,
        bidExpireDate: row.bidExpireDate,
        expirationSeconds: row.expirationSeconds,
        winningBidCallAccepted: row.winningBidCallAccepted,
        winningBidCallRejected: row.winningBidCallRejected,
        bidElapsedMs: row.bidElapsedMs,
        ingestStatus: row.ingestStatus,
        ingestErrorCode: row.ingestErrorCode,
        ingestErrorMessage: row.ingestErrorMessage,
        rowJson: row.rowJson,
      })),
      bidIds: pendingBidIds,
      startPosition: nextPosition,
    });

    nextPosition = insertResult.nextPosition;
    pendingRows = [];
    pendingBidIds = [];
  }

  try {
    importRunId = await createImportRun({
      sourceType: "csv_direct_import",
      bidIds: [],
      forceRefresh: input.forceRefresh,
      notes: `Direct CSV import from ${input.file.name}.`,
      sourceMetadata: {},
      sourceStage: "parsing",
    });

    const sourceFile = await createImportSourceFile({
      importRunId,
      sourceType: "csv_direct_import",
      fileName: input.file.name,
      contentHash,
      rowCount: 0,
      headerJson: [],
      headerMappingJson: [],
      sourceMetadata: buildCurrentSourceMetadata(),
    });
    sourceFileId = sourceFile.id;

    await streamCsvRows({
      file: input.file,
      onHeader: (header, nextHeaderMapping) => {
        headerRow = header;
        headerMapping = nextHeaderMapping;
      },
      onRow: async (row, rowNumber, _header, headerMap, nextHeaderMapping) => {
        parsedRowCount += 1;
        if (parsedRowCount > MAX_CSV_DIRECT_ROWS) {
          throw createCsvDirectImportError({
            message: `The uploaded CSV exceeds the ${MAX_CSV_DIRECT_ROWS} row limit for direct import.`,
            status: 422,
            code: CSV_DIRECT_ERROR_CODES.rowLimitExceeded,
            details: {
              maxRows: MAX_CSV_DIRECT_ROWS,
            },
          });
        }

        const parsedRow = buildParsedRow({
          row,
          rowNumber,
          headerMap,
          headerMapping: nextHeaderMapping,
        });

        if (!parsedRow.bidId) {
          missingBidIdCount += 1;
          markRowRejected(parsedRow, {
            code: "missing_bid_id",
            message: "This row does not include a Bid ID.",
          });
        } else if (!parsedRow.bidIdValid) {
          invalidBidIdCount += 1;
          markRowRejected(parsedRow, {
            code: "invalid_bid_id",
            message: "This value does not look like a valid Bid ID.",
          });
        } else {
          validBidIdCount += 1;
          if (seenBidIds.has(parsedRow.bidId)) {
            duplicateBidIdCount += 1;
            markRowSkippedDuplicate(parsedRow);
          } else {
            seenBidIds.add(parsedRow.bidId);
            pendingBidIds.push(parsedRow.bidId);
          }
        }

        const nextBounds = updateBidDateBounds({
          earliestBidDt,
          latestBidDt,
          bidDt: parsedRow.bidDt,
        });
        earliestBidDt = nextBounds.earliestBidDt;
        latestBidDt = nextBounds.latestBidDt;

        pendingRows.push(parsedRow);

        if (
          pendingRows.length >= CSV_DIRECT_CHUNK_SIZE ||
          pendingBidIds.length >= CSV_DIRECT_CHUNK_SIZE
        ) {
          await flushPending();
        }

        if (parsedRowCount % METADATA_UPDATE_INTERVAL === 0 && importRunId) {
          await updateImportRunSourceState({
            importRunId,
            exportRowCount: parsedRowCount,
            sourceMetadata: buildCurrentSourceMetadata(),
          });
        }
      },
    });

    if (parsedRowCount === 0) {
      throw createCsvDirectImportError({
        message: "The uploaded CSV does not contain any data rows.",
        status: 422,
        code: CSV_DIRECT_ERROR_CODES.missingDataRows,
      });
    }

    await flushPending();

    await updateImportSourceFile({
      id: sourceFile.id,
      contentHash,
      rowCount: parsedRowCount,
      headerJson: headerRow,
      headerMappingJson: headerMapping,
      sourceMetadata: buildCurrentSourceMetadata(),
    });

    await updateImportRunTotals({
      importRunId,
      totalFound: seenBidIds.size,
    });

    await updateImportRunSourceState({
      importRunId,
      exportRowCount: parsedRowCount,
      exportDownloadStatus: "parsed",
      sourceStage: "queued",
      sourceMetadata: buildCurrentSourceMetadata(),
    });

    const detail = await getImportRunDetail(importRunId);

    if (!detail) {
      throw createCsvDirectImportError({
        message: `Unable to load import run detail after creation: ${importRunId}`,
        status: 500,
        code: CSV_DIRECT_ERROR_CODES.processingFailed,
      });
    }

    const summary = buildCsvDirectImportSummary({
      fileName: input.file.name,
      contentHash,
      parsedRowCount,
      queuedItemCount: seenBidIds.size,
      validBidIdCount,
      duplicateBidIdCount,
      missingBidIdCount,
      invalidBidIdCount,
      earliestBidDt,
      latestBidDt,
    });

    return {
      preview: {
        fileName: input.file.name,
        contentHash,
        totalRows: parsedRowCount,
        validBidIdCount,
        queuedItemCount: summary.queuedItemCount,
        rejectedRowCount: summary.rejectedRowCount,
        skippedDuplicateRowCount: summary.skippedDuplicateRowCount,
        missingBidIdCount,
        duplicateBidIdCount,
        invalidBidIdCount,
        earliestBidDt,
        latestBidDt,
        headers: headerRow,
        duplicateImport,
        sampleRows: [],
        invalidRows: [],
      },
      summary,
      importRun: detail,
    };
  } catch (error) {
    const message = error instanceof Error
      ? error.message
      : "Unable to create an import run from direct CSV upload.";
    const normalizedError = isCsvDirectImportError(error)
      ? error
      : createCsvDirectImportError({
          message,
          status: 500,
          code: CSV_DIRECT_ERROR_CODES.processingFailed,
        });

    if (sourceFileId) {
      await updateImportSourceFile({
        id: sourceFileId,
        contentHash,
        rowCount: parsedRowCount,
        headerJson: headerRow,
        headerMappingJson: headerMapping,
        sourceMetadata: buildCurrentSourceMetadata(message),
      }).catch(() => undefined);
    }

    if (importRunId) {
      await updateImportRunSourceState({
        importRunId,
        exportRowCount: parsedRowCount,
        exportDownloadStatus: "failed",
        sourceStage: "failed",
        lastError: message,
        sourceMetadata: buildCurrentSourceMetadata(message),
      }).catch(() => undefined);

      await markImportRunFailed(importRunId, message).catch(() => undefined);
    }

    throw normalizedError;
  }
}

export async function processCsvDirectImportItem(input: {
  importRunId: string;
  bidId: string;
}) {
  const row = await getImportSourceRowForBidId({
    importRunId: input.importRunId,
    bidId: input.bidId,
  });

  if (!row) {
    throw new Error(`No CSV source row found for bid ${input.bidId}.`);
  }

  const normalizedBid = buildNormalizedBid({
    rowNumber: row.rowNumber,
    bidId: row.bidId ?? null,
    bidIdValid: Boolean(row.bidId),
    bidDt: row.bidDt,
    bidDtRaw: "",
    campaignName: row.campaignName ?? null,
    campaignId: row.campaignId ?? null,
    publisherName: row.publisherName ?? null,
    publisherId: row.publisherId ?? null,
    bidAmount: row.bidAmount ?? null,
    winningBid: row.winningBid ?? null,
    bidRejected: row.bidRejected ?? null,
    reasonForReject: row.reasonForReject ?? null,
    bidDid: row.bidDid ?? null,
    bidExpireDate: row.bidExpireDate ?? null,
    expirationSeconds: row.expirationSeconds ?? null,
    winningBidCallAccepted: row.winningBidCallAccepted ?? null,
    winningBidCallRejected: row.winningBidCallRejected ?? null,
    bidElapsedMs: row.bidElapsedMs ?? null,
    ingestStatus: row.ingestStatus,
    ingestErrorCode: row.ingestErrorCode,
    ingestErrorMessage: row.ingestErrorMessage,
    rowJson: (row.rowJson ?? {}) as Record<string, string>,
  });

  if (!normalizedBid.bidId) {
    throw new Error(`CSV source row missing bid id for run ${input.importRunId}.`);
  }

  const existing = await getInvestigationByBidId(normalizedBid.bidId);
  if (existing?.enrichmentState === "enriched") {
    return {
      investigationId: existing.id,
      resolution: "reused" as const,
    };
  }

  const diagnosis = buildCsvDiagnosis({
    normalizedBid,
    sourceRow: row,
  });

  const investigation = await upsertInvestigation({
    importRunId: input.importRunId,
    normalizedBid,
    diagnosis,
    persistence: {
      detailSource: "csv_direct",
      enrichmentState: "csv_only",
      sourceImportRunId: input.importRunId,
      sourceImportSourceFileId: row.importSourceFileId,
      sourceImportSourceRowId: row.id,
      lastRingbaAttemptAt: existing?.lastRingbaAttemptAt ?? null,
      lastRingbaFetchAt: existing?.lastRingbaFetchAt ?? null,
      ringbaFailureCount: existing?.ringbaFailureCount ?? 0,
      nextRingbaRetryAt: existing?.nextRingbaRetryAt ?? null,
    },
  });

  if (!investigation) {
    throw new Error(`Unable to persist investigation for bid ${input.bidId}.`);
  }

  return {
    investigationId: investigation.id,
    resolution: "fetched" as const,
  };
}
