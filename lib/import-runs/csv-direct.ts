import "server-only";

import Papa from "papaparse";
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
  getImportSourceRowForBidId,
  insertImportSourceRowsWithRunItemsBatch,
  updateImportSourceFile,
} from "@/lib/db/import-sources";
import { buildCsvDiagnosis } from "@/lib/diagnostics/csv-direct";
import { upsertInvestigation } from "@/lib/db/investigations";
import { isValidBidId } from "@/lib/utils/bid-id";
import type { CsvDirectPreviewResult } from "@/types/import-run";
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

function buildRowObject(headers: string[], row: string[]) {
  const result: Record<string, string> = {};

  headers.forEach((header, index) => {
    result[header] = row[index]?.trim() ?? "";
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
  rowJson: Record<string, string>;
}

function buildParsedRow(input: {
  row: string[];
  rowNumber: number;
  headerRow: string[];
  headerMap: ReturnType<typeof buildHeaderIndexMap>;
}): CsvDirectParsedRow {
  const bidIdValue = readStringCell(input.row, input.headerMap.bidId);
  const bidIdValid = Boolean(bidIdValue) && isValidBidId(bidIdValue);
  const bidDateRaw = readStringCell(input.row, input.headerMap.bidDate);
  const rowJson = buildRowObject(input.headerRow, input.row);

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
    rowJson,
  };
}

function parseCsvDirectRows(csvText: string) {
  const rows = parseCsvRows(csvText);

  if (rows.length === 0) {
    throw new Error("The uploaded CSV file is empty.");
  }

  if (rows.length > MAX_CSV_DIRECT_ROWS + 1) {
    throw new Error(
      `The uploaded CSV exceeds the ${MAX_CSV_DIRECT_ROWS} row limit for direct import.`,
    );
  }

  const headerRow = rows[0] ?? [];

  if (headerRow.length === 0) {
    throw new Error("The uploaded CSV file does not contain a header row.");
  }

  const headerMap = buildHeaderIndexMap(headerRow);

  if (headerMap.bidId === null) {
    throw new Error("The uploaded CSV file does not include a Bid ID column.");
  }

  const dataRows = rows.slice(1);

  if (dataRows.length === 0) {
    throw new Error("The uploaded CSV does not contain any data rows.");
  }

  const parsedRows: CsvDirectParsedRow[] = [];

  dataRows.forEach((row, index) => {
    parsedRows.push(
      buildParsedRow({
        row,
        rowNumber: index + 2,
        headerRow,
        headerMap,
      }),
    );
  });

  return {
    headers: headerRow,
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
    duplicateBidIdCount,
    missingBidIdCount,
    invalidBidIdCount,
    earliestBidDt,
    latestBidDt,
    invalidRows: invalidRows.slice(0, 20),
  };
}

function createCsvDirectPreviewAccumulator(fileName: string) {
  let headers: string[] = [];
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
        totalRows,
        validBidIdCount,
        missingBidIdCount,
        duplicateBidIdCount,
        invalidBidIdCount,
        earliestBidDt,
        latestBidDt,
        headers,
        sampleRows,
        invalidRows,
      };
    },
  };
}

function buildCsvDirectSourceMetadata(input: {
  fileName: string;
  parsedRowCount: number;
  validBidIdCount: number;
  dedupedBidIdCount: number;
  duplicateBidIdCount: number;
  missingBidIdCount: number;
  invalidBidIdCount: number;
  earliestBidDt: string | null;
  latestBidDt: string | null;
  headerRow: string[];
}) {
  return {
    sourceFileName: input.fileName,
    parsedRowCount: input.parsedRowCount,
    validBidIdCount: input.validBidIdCount,
    dedupedBidIdCount: input.dedupedBidIdCount,
    duplicateBidIdCount: input.duplicateBidIdCount,
    missingBidIdCount: input.missingBidIdCount,
    invalidBidIdCount: input.invalidBidIdCount,
    earliestBidDt: input.earliestBidDt,
    latestBidDt: input.latestBidDt,
    parsedHeaders: input.headerRow,
  };
}

export function buildCsvDirectPreview(input: {
  csvText: string;
  fileName: string;
}): CsvDirectPreviewResult {
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
    totalRows: parsedRows.length,
    validBidIdCount,
    missingBidIdCount: summary.missingBidIdCount,
    duplicateBidIdCount: summary.duplicateBidIdCount,
    invalidBidIdCount: summary.invalidBidIdCount,
    earliestBidDt: summary.earliestBidDt,
    latestBidDt: summary.latestBidDt,
    headers,
    sampleRows,
    invalidRows: summary.invalidRows,
  };
}

export async function previewCsvDirectUpload(input: { file: File }) {
  if (input.file.size === 0) {
    throw new Error("The uploaded CSV file is empty.");
  }

  if (input.file.size > MAX_CSV_DIRECT_UPLOAD_BYTES) {
    throw new Error(
      `The uploaded CSV exceeds the ${Math.floor(
        MAX_CSV_DIRECT_UPLOAD_BYTES / (1024 * 1024),
      )} MB file size limit.`,
    );
  }

  const previewAccumulator = createCsvDirectPreviewAccumulator(input.file.name);
  let hasDataRows = false;
  let parsedRowCount = 0;

  await streamCsvRows({
    file: input.file,
    onHeader: (header) => {
      previewAccumulator.setHeaders(header);
    },
    onRow: async (row, rowNumber, headerRow, headerMap) => {
      parsedRowCount += 1;
      if (parsedRowCount > MAX_CSV_DIRECT_ROWS) {
        throw new Error(
          `The uploaded CSV exceeds the ${MAX_CSV_DIRECT_ROWS} row limit for direct import.`,
        );
      }

      hasDataRows = true;
      previewAccumulator.addRow(
        buildParsedRow({
          row,
          rowNumber,
          headerRow,
          headerMap,
        }),
      );
    },
  });

  if (!hasDataRows) {
    throw new Error("The uploaded CSV does not contain any data rows.");
  }

  return previewAccumulator.buildPreview();
}

async function streamCsvRows(input: {
  file: File;
  onHeader: (header: string[]) => void;
  onRow: (
    row: string[],
    rowNumber: number,
    header: string[],
    headerMap: ReturnType<typeof buildHeaderIndexMap>,
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
        headerRow = row.map((value) => String(value ?? ""));
        if (headerRow.length === 0) {
          settleWithError(new Error("The uploaded CSV file does not contain a header row."));
          return;
        }
        headerMap = buildHeaderIndexMap(headerRow);
        if (headerMap.bidId === null) {
          settleWithError(new Error("The uploaded CSV file does not include a Bid ID column."));
          return;
        }
        input.onHeader(headerRow);
        if (!settled) {
          parser.resume();
        }
        return;
      }
      const map = headerMap;
      if (!map || !headerRow) {
        settleWithError(new Error("Unable to read CSV header row."));
        return;
      }
      pendingRows += 1;
      void input
        .onRow(row.map((value) => String(value ?? "")), rowNumber, headerRow, map)
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
      settleWithError(error);
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
  const isZeroBid =
    bidAmount === 0 ||
    winningBid === 0 ||
    String(row.reasonForReject ?? "").toLowerCase().includes("zero bid");
  const outcome =
    row.bidRejected === true || row.reasonForReject
      ? "rejected"
      : isZeroBid
        ? "zero_bid"
        : bidAmount !== null || winningBid !== null
          ? "accepted"
          : "unknown";

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
    isZeroBid,
    reasonForReject: row.reasonForReject,
    httpStatusCode: null,
    errorMessage: null,
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
    outcome,
  };
}

export async function createImportRunFromCsvDirectUpload(input: {
  file: File;
  forceRefresh: boolean;
}) {
  if (input.file.size === 0) {
    throw new Error("The uploaded CSV file is empty.");
  }

  if (input.file.size > MAX_CSV_DIRECT_UPLOAD_BYTES) {
    throw new Error(
      `The uploaded CSV exceeds the ${Math.floor(
        MAX_CSV_DIRECT_UPLOAD_BYTES / (1024 * 1024),
      )} MB file size limit.`,
    );
  }

  let importRunId: string | null = null;
  let sourceFileId: string | null = null;
  let headerRow: string[] = [];
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
      rowCount: 0,
      headerJson: [],
      sourceMetadata: {},
    });
    sourceFileId = sourceFile.id;

    await streamCsvRows({
      file: input.file,
      onHeader: (header) => {
        headerRow = header;
      },
      onRow: async (row, rowNumber, header, headerMap) => {
        parsedRowCount += 1;
        if (parsedRowCount > MAX_CSV_DIRECT_ROWS) {
          throw new Error(
            `The uploaded CSV exceeds the ${MAX_CSV_DIRECT_ROWS} row limit for direct import.`,
          );
        }

        const parsedRow = buildParsedRow({
          row,
          rowNumber,
          headerRow: header,
          headerMap,
        });

        if (!parsedRow.bidId) {
          missingBidIdCount += 1;
        } else if (!parsedRow.bidIdValid) {
          invalidBidIdCount += 1;
        } else {
          validBidIdCount += 1;
          if (seenBidIds.has(parsedRow.bidId)) {
            duplicateBidIdCount += 1;
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
            sourceMetadata: buildCsvDirectSourceMetadata({
              fileName: input.file.name,
              parsedRowCount,
              validBidIdCount,
              dedupedBidIdCount: seenBidIds.size,
              duplicateBidIdCount,
              missingBidIdCount,
              invalidBidIdCount,
              earliestBidDt,
              latestBidDt,
              headerRow,
            }),
          });
        }
      },
    });

    if (parsedRowCount === 0) {
      throw new Error("The uploaded CSV does not contain any data rows.");
    }

    await flushPending();

    await updateImportSourceFile({
      id: sourceFile.id,
      rowCount: parsedRowCount,
      headerJson: headerRow,
      sourceMetadata: buildCsvDirectSourceMetadata({
        fileName: input.file.name,
        parsedRowCount,
        validBidIdCount,
        dedupedBidIdCount: seenBidIds.size,
        duplicateBidIdCount,
        missingBidIdCount,
        invalidBidIdCount,
        earliestBidDt,
        latestBidDt,
        headerRow,
      }),
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
      sourceMetadata: buildCsvDirectSourceMetadata({
        fileName: input.file.name,
        parsedRowCount,
        validBidIdCount,
        dedupedBidIdCount: seenBidIds.size,
        duplicateBidIdCount,
        missingBidIdCount,
        invalidBidIdCount,
        earliestBidDt,
        latestBidDt,
        headerRow,
      }),
    });

    const detail = await getImportRunDetail(importRunId);

    if (!detail) {
      throw new Error(`Unable to load import run detail after creation: ${importRunId}`);
    }

    return {
      preview: {
        fileName: input.file.name,
        totalRows: parsedRowCount,
        validBidIdCount,
        missingBidIdCount,
        duplicateBidIdCount,
        invalidBidIdCount,
        earliestBidDt,
        latestBidDt,
        headers: headerRow,
        sampleRows: [],
        invalidRows: [],
      },
      importRun: detail,
    };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Unable to create an import run from direct CSV upload.";

    if (sourceFileId) {
      await updateImportSourceFile({
        id: sourceFileId,
        rowCount: parsedRowCount,
        headerJson: headerRow,
        sourceMetadata: {
          ...buildCsvDirectSourceMetadata({
            fileName: input.file.name,
            parsedRowCount,
            validBidIdCount,
            dedupedBidIdCount: seenBidIds.size,
            duplicateBidIdCount,
            missingBidIdCount,
            invalidBidIdCount,
            earliestBidDt,
            latestBidDt,
            headerRow,
          }),
          lastError: message,
        },
      }).catch(() => undefined);
    }

    if (importRunId) {
      await updateImportRunSourceState({
        importRunId,
        exportRowCount: parsedRowCount,
        exportDownloadStatus: "failed",
        sourceStage: "failed",
        lastError: message,
        sourceMetadata: buildCsvDirectSourceMetadata({
          fileName: input.file.name,
          parsedRowCount,
          validBidIdCount,
          dedupedBidIdCount: seenBidIds.size,
          duplicateBidIdCount,
          missingBidIdCount,
          invalidBidIdCount,
          earliestBidDt,
          latestBidDt,
          headerRow,
        }),
      }).catch(() => undefined);

      await markImportRunFailed(importRunId, message).catch(() => undefined);
    }

    throw error;
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
    rowJson: (row.rowJson ?? {}) as Record<string, string>,
  });

  if (!normalizedBid.bidId) {
    throw new Error(`CSV source row missing bid id for run ${input.importRunId}.`);
  }

  const diagnosis = buildCsvDiagnosis({
    normalizedBid,
    sourceRow: row,
  });

  const investigation = await upsertInvestigation({
    importRunId: input.importRunId,
    normalizedBid,
    diagnosis,
  });

  if (!investigation) {
    throw new Error(`Unable to persist investigation for bid ${input.bidId}.`);
  }

  return {
    investigationId: investigation.id,
    resolution: "fetched" as const,
  };
}
