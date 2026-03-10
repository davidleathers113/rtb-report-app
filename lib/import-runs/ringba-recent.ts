import "server-only";

import Papa from "papaparse";
import { unzipSync } from "fflate";

import {
  addImportRunItems,
  createImportRun,
  getImportRunDetail,
  getImportSourceCheckpoint,
  updateImportRunSourceState,
  upsertImportSourceCheckpoint,
} from "@/lib/db/import-runs";
import { getRingbaConfig } from "@/lib/ringba/client";
import { safeJsonParse } from "@/lib/utils/json";
import { isValidBidId } from "@/lib/utils/bid-id";
import type { ImportRunDetail, RingbaRecentImportDiagnostics } from "@/types/import-run";

const DEFAULT_WINDOW_MINUTES = 15;
const DEFAULT_OVERLAP_MINUTES = 2;
const EXPORT_POLL_INTERVAL_MS = 1000;
const EXPORT_POLL_MAX_ATTEMPTS = 15;
const MAX_RINGBA_RECENT_IMPORT_ROWS = 10000;
const MAX_RINGBA_RECENT_IMPORT_ZIP_BYTES = 10 * 1024 * 1024;
const MAX_RINGBA_RECENT_IMPORT_CSV_BYTES = 30 * 1024 * 1024;
const BID_ID_HEADER_ALIASES = ["bidid", "bid_id", "bid id"] as const;
const BID_DATE_HEADER_ALIASES = ["biddate", "bid_dt", "biddt", "bid date"] as const;

const RINGBA_RECENT_IMPORT_COLUMNS = [
  "campaignName",
  "publisherName",
  "campaignId",
  "publisherId",
  "publisherSubId",
  "bidAmount",
  "winningBid",
  "winningBidCallAccepted",
  "winningBidCallRejected",
  "bidDt",
  "bidExpireInSeconds",
  "bidExpireDt",
  "bidDID",
  "bidId",
  "isZeroBid",
  "reasonForReject",
  "tag:User:zip",
  "tag:InboundNumber:Number",
] as const;

interface CreateRtbExportJobInput {
  accountId?: string;
  reportStart: string;
  reportEnd: string;
  formatTimespans?: boolean;
  formatPercentages?: boolean;
  formatDateTime?: boolean;
  generateRollups?: boolean;
  formatTimeZone?: string;
  valueColumns?: Array<{ column: string }>;
  filters?: Array<Record<string, unknown>>;
}

interface RtbExportJobStatus {
  status: string;
  url: string | null;
}

interface ParsedRtbExportCsv {
  bidIds: string[];
  rowCount: number;
  extractedBidIdCount: number;
  dedupedBidIdCount: number;
  duplicateCount: number;
  invalidBidIdCount: number;
  latestBidDt: string | null;
  earliestBidDt: string | null;
  headers: string[];
  sampleBidIds: string[];
}

interface DownloadedRtbExportZip {
  zipBytes: Uint8Array;
  contentLengthBytes: number | null;
}

function buildSourceKey(accountId: string) {
  return `ringba_recent_import:${accountId}`;
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

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
  const normalizedCsvText = csvText.startsWith("\uFEFF") ? csvText.slice(1) : csvText;
  const result = Papa.parse<unknown[]>(normalizedCsvText, {
    skipEmptyLines: true,
  });

  const meaningfulErrors = result.errors.filter((error) => {
    return error.type !== "Delimiter";
  });

  if (meaningfulErrors.length > 0) {
    const firstError = meaningfulErrors[0];
    const rowNumber = typeof firstError.row === "number" ? firstError.row + 1 : 1;
    throw new Error(`Malformed RTB export CSV near row ${rowNumber}: ${firstError.message}`);
  }

  return result.data
    .map((row) => row.map((cell) => String(cell ?? "").trim()))
    .filter((row) => row.some((cell) => cell.length > 0));
}

function parseUtcBidDateValue(value: string) {
  const trimmed = value.trim();

  if (!trimmed) {
    return null;
  }

  const segments = trimmed.split(" ");
  if (segments.length < 3) {
    return null;
  }

  const dateParts = segments[0].split("/");
  const timeParts = segments[1].split(":");
  const meridiem = segments[2].toUpperCase();

  if (dateParts.length !== 3 || timeParts.length < 2) {
    return null;
  }

  let hour = Number(timeParts[0]);
  const minute = Number(timeParts[1]);
  const second = Number(timeParts[2] ?? "0");
  const month = Number(dateParts[0]);
  const day = Number(dateParts[1]);
  const year = Number(dateParts[2]);

  if (
    Number.isNaN(hour) ||
    Number.isNaN(minute) ||
    Number.isNaN(second) ||
    Number.isNaN(month) ||
    Number.isNaN(day) ||
    Number.isNaN(year)
  ) {
    return null;
  }

  if (meridiem === "PM" && hour < 12) {
    hour += 12;
  }

  if (meridiem === "AM" && hour === 12) {
    hour = 0;
  }

  const parsed = new Date(Date.UTC(year, month - 1, day, hour, minute, second));

  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.toISOString();
}

function mergeSourceMetadata(
  current: Record<string, unknown>,
  updates: Record<string, unknown>,
) {
  return {
    ...current,
    ...updates,
  };
}

function normalizeDiagnostics(sourceMetadata: Record<string, unknown>) {
  const diagnostics =
    sourceMetadata.diagnostics &&
    typeof sourceMetadata.diagnostics === "object" &&
    !Array.isArray(sourceMetadata.diagnostics)
      ? (sourceMetadata.diagnostics as RingbaRecentImportDiagnostics)
      : {};

  return diagnostics;
}

function updateDiagnostics(
  sourceMetadata: Record<string, unknown>,
  updates: RingbaRecentImportDiagnostics,
) {
  return mergeSourceMetadata(sourceMetadata, {
    diagnostics: {
      ...normalizeDiagnostics(sourceMetadata),
      ...updates,
    },
  });
}

function getWindowMinutes(sourceMetadata: Record<string, unknown>) {
  const rawValue = sourceMetadata.windowMinutes;

  if (typeof rawValue === "number" && [5, 15, 60].includes(rawValue)) {
    return rawValue;
  }

  return DEFAULT_WINDOW_MINUTES;
}

function getOverlapMinutes(sourceMetadata: Record<string, unknown>) {
  const rawValue = sourceMetadata.overlapMinutes;

  if (typeof rawValue === "number" && rawValue >= 0) {
    return rawValue;
  }

  return DEFAULT_OVERLAP_MINUTES;
}

function getCheckpointStart(input: {
  now: Date;
  windowMinutes: number;
  overlapMinutes: number;
  checkpointBidDt: string | null;
}) {
  const fallbackStart = new Date(
    input.now.getTime() - input.windowMinutes * 60 * 1000,
  );

  if (!input.checkpointBidDt) {
    return fallbackStart;
  }

  const checkpointDate = new Date(input.checkpointBidDt);

  if (Number.isNaN(checkpointDate.getTime())) {
    return fallbackStart;
  }

  const overlapStart = new Date(
    checkpointDate.getTime() - input.overlapMinutes * 60 * 1000,
  );

  return overlapStart > fallbackStart ? overlapStart : fallbackStart;
}

async function parseJsonResponse(response: Response) {
  const text = await response.text();
  return text ? safeJsonParse(text) : null;
}

function buildRingbaErrorMessage(input: {
  action: string;
  response: Response;
  body: unknown;
}) {
  if (input.body && typeof input.body === "object" && "message" in input.body) {
    const message = input.body.message;
    if (typeof message === "string" && message.trim()) {
      return `${input.action}: ${message}`;
    }
  }

  return `${input.action}: Ringba returned HTTP ${input.response.status}.`;
}

export async function createRtbExportJob(input: CreateRtbExportJobInput) {
  const config = getRingbaConfig();
  const accountId = input.accountId ?? config.accountId;
  const requestUrl = `${config.apiBaseUrl}/${accountId}/rtb/export/csv`;
  const response = await fetch(requestUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: `${config.authScheme} ${config.apiToken}`,
    },
    body: JSON.stringify({
      reportStart: input.reportStart,
      reportEnd: input.reportEnd,
      formatTimespans: input.formatTimespans ?? true,
      formatPercentages: input.formatPercentages ?? true,
      formatDateTime: input.formatDateTime ?? true,
      generateRollups: input.generateRollups ?? false,
      formatTimeZone: input.formatTimeZone ?? "UTC",
      valueColumns:
        input.valueColumns ??
        RINGBA_RECENT_IMPORT_COLUMNS.map((column) => ({
          column,
        })),
      filters: input.filters ?? [],
    }),
    cache: "no-store",
  });

  const body = await parseJsonResponse(response);

  if (!response.ok) {
    throw new Error(
      buildRingbaErrorMessage({
        action: "Unable to create RTB export job",
        response,
        body,
      }),
    );
  }

  if (!body || typeof body !== "object" || !("id" in body) || typeof body.id !== "string") {
    throw new Error("Unable to create RTB export job: missing export job id.");
  }

  return {
    jobId: body.id,
    requestUrl,
  };
}

export async function getRtbExportJobStatus(input: {
  accountId?: string;
  jobId: string;
}): Promise<RtbExportJobStatus> {
  const config = getRingbaConfig();
  const accountId = input.accountId ?? config.accountId;
  const requestUrl = `${config.apiBaseUrl}/${accountId}/rtb/export/${encodeURIComponent(
    input.jobId,
  )}`;
  const response = await fetch(requestUrl, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `${config.authScheme} ${config.apiToken}`,
    },
    cache: "no-store",
  });

  const body = await parseJsonResponse(response);

  if (!response.ok) {
    throw new Error(
      buildRingbaErrorMessage({
        action: "Unable to fetch RTB export job status",
        response,
        body,
      }),
    );
  }

  const status =
    body && typeof body === "object" && "status" in body && typeof body.status === "string"
      ? body.status
      : "Unknown";
  const url =
    body && typeof body === "object" && "url" in body && typeof body.url === "string"
      ? body.url
      : null;

  return {
    status,
    url,
  };
}

export async function downloadRtbExportZip(downloadUrl: string) {
  const response = await fetch(downloadUrl, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Unable to download RTB export ZIP: HTTP ${response.status}.`);
  }

  const contentLengthHeader = response.headers.get("content-length");
  const contentLengthBytes = contentLengthHeader ? Number(contentLengthHeader) : null;

  if (
    contentLengthBytes !== null &&
    !Number.isNaN(contentLengthBytes) &&
    contentLengthBytes > MAX_RINGBA_RECENT_IMPORT_ZIP_BYTES
  ) {
    throw new Error(
      `RTB export ZIP exceeds the ${Math.floor(
        MAX_RINGBA_RECENT_IMPORT_ZIP_BYTES / (1024 * 1024),
      )} MB safety limit.`,
    );
  }

  const zipBytes = new Uint8Array(await response.arrayBuffer());

  if (zipBytes.byteLength > MAX_RINGBA_RECENT_IMPORT_ZIP_BYTES) {
    throw new Error(
      `RTB export ZIP exceeds the ${Math.floor(
        MAX_RINGBA_RECENT_IMPORT_ZIP_BYTES / (1024 * 1024),
      )} MB safety limit.`,
    );
  }

  return {
    zipBytes,
    contentLengthBytes:
      contentLengthBytes !== null && !Number.isNaN(contentLengthBytes)
        ? contentLengthBytes
        : null,
  } satisfies DownloadedRtbExportZip;
}

export function extractCsvFromZip(zipBytes: Uint8Array) {
  const archive = unzipSync(zipBytes);
  const csvEntryName = Object.keys(archive).find((entryName) => entryName.endsWith(".csv"));

  if (!csvEntryName) {
    throw new Error("Unable to extract RTB export CSV: ZIP did not contain a CSV file.");
  }

  const decoder = new TextDecoder("utf-8");
  let csvText = decoder.decode(archive[csvEntryName]);

  if (archive[csvEntryName].byteLength > MAX_RINGBA_RECENT_IMPORT_CSV_BYTES) {
    throw new Error(
      `RTB export CSV exceeds the ${Math.floor(
        MAX_RINGBA_RECENT_IMPORT_CSV_BYTES / (1024 * 1024),
      )} MB safety limit.`,
    );
  }

  if (csvText.startsWith("\uFEFF")) {
    csvText = csvText.slice(1);
  }

  return {
    fileName: csvEntryName,
    csvText,
  };
}

export function parseRtbExportCsv(csvText: string): ParsedRtbExportCsv {
  const rows = parseCsvRows(csvText);

  if (rows.length === 0) {
    throw new Error("The RTB export CSV is empty.");
  }

  const headers = rows[0];
  const dataRows = rows.slice(1);

  if (dataRows.length > MAX_RINGBA_RECENT_IMPORT_ROWS) {
    throw new Error(
      `RTB export returned ${dataRows.length} rows, which exceeds the ${MAX_RINGBA_RECENT_IMPORT_ROWS} row safety limit for one import.`,
    );
  }

  if (dataRows.length === 0) {
    return {
      bidIds: [],
      rowCount: 0,
      extractedBidIdCount: 0,
      dedupedBidIdCount: 0,
      duplicateCount: 0,
      invalidBidIdCount: 0,
      latestBidDt: null,
      earliestBidDt: null,
      headers,
      sampleBidIds: [],
    };
  }

  let bidIdIndex = -1;
  let bidDateIndex = -1;

  headers.forEach((header, index) => {
    const normalized = normalizeHeaderValue(header);

    if (
      BID_ID_HEADER_ALIASES.includes(
        normalized as (typeof BID_ID_HEADER_ALIASES)[number],
      )
    ) {
      bidIdIndex = index;
    }

    if (
      BID_DATE_HEADER_ALIASES.includes(
        normalized as (typeof BID_DATE_HEADER_ALIASES)[number],
      )
    ) {
      bidDateIndex = index;
    }
  });

  if (bidIdIndex === -1) {
    throw new Error("The RTB export CSV did not include a Bid ID column.");
  }

  const rawBidIds: string[] = [];
  const parsedBidDates: string[] = [];
  let invalidBidIdCount = 0;

  for (const row of dataRows) {
    const bidId = (row[bidIdIndex] ?? "").trim();

    if (!bidId) {
      continue;
    }

    if (!isValidBidId(bidId)) {
      invalidBidIdCount += 1;
      continue;
    }

    rawBidIds.push(bidId);

    if (bidDateIndex >= 0) {
      const parsedBidDate = parseUtcBidDateValue((row[bidDateIndex] ?? "").trim());

      if (parsedBidDate) {
        parsedBidDates.push(parsedBidDate);
      }
    }
  }

  const { bidIds, duplicateCount } = dedupeBidIds(rawBidIds);
  const sortedBidDates = [...parsedBidDates].sort((left, right) => {
    if (left < right) {
      return -1;
    }

    if (left > right) {
      return 1;
    }

    return 0;
  });

  return {
    bidIds,
    rowCount: dataRows.length,
    extractedBidIdCount: rawBidIds.length,
    dedupedBidIdCount: bidIds.length,
    duplicateCount,
    invalidBidIdCount,
    latestBidDt: sortedBidDates[sortedBidDates.length - 1] ?? null,
    earliestBidDt: sortedBidDates[0] ?? null,
    headers,
    sampleBidIds: bidIds.slice(0, 10),
  };
}

export async function createRingbaRecentImportRun(input: {
  windowMinutes: 5 | 15 | 60;
  forceRefresh: boolean;
  accountId?: string;
  overlapMinutes?: number;
  triggerType?: "manual" | "scheduled";
  scheduleId?: string | null;
  scheduleName?: string;
}) {
  const config = getRingbaConfig();
  const accountId = input.accountId ?? config.accountId;
  const overlapMinutes = input.overlapMinutes ?? DEFAULT_OVERLAP_MINUTES;
  const checkpointSourceKey =
    input.triggerType === "scheduled" && input.scheduleId
      ? `ringba_recent_import_schedule:${input.scheduleId}`
      : buildSourceKey(accountId);
  const sourceMetadata = updateDiagnostics(
    {
      accountId,
    windowMinutes: input.windowMinutes,
      overlapMinutes,
    requestedColumns: [...RINGBA_RECENT_IMPORT_COLUMNS],
      checkpointSourceKey,
      scheduleId: input.scheduleId ?? null,
      scheduleName: input.scheduleName ?? null,
    },
    {
      windowMinutes: input.windowMinutes,
      overlapMinutes,
      checkpointSourceKey,
    },
  );
  const importRunId = await createImportRun({
    sourceType: "ringba_recent_import",
    bidIds: [],
    forceRefresh: input.forceRefresh,
    notes:
      input.triggerType === "scheduled" && input.scheduleName
        ? `Scheduled Ringba recent import from ${input.scheduleName} for the last ${input.windowMinutes} minutes.`
        : `Ringba recent import for the last ${input.windowMinutes} minutes.`,
    triggerType: input.triggerType ?? "manual",
    scheduleId: input.scheduleId ?? null,
    sourceStage: "creating_export",
    exportDownloadStatus: "pending",
    sourceMetadata,
  });
  const detail = await getImportRunDetail(importRunId);

  if (!detail) {
    throw new Error(`Unable to load import run detail after creation: ${importRunId}`);
  }

  return detail;
}

export async function prepareRingbaRecentImportRun(input: {
  importRunId: string;
  sourceMetadata: Record<string, unknown>;
}) {
  const config = getRingbaConfig();
  let current = await getImportRunDetail(input.importRunId);

  if (!current) {
    throw new Error(`Import run not found: ${input.importRunId}`);
  }

  if (current.sourceType !== "ringba_recent_import") {
    return current;
  }

  let sourceMetadata = mergeSourceMetadata(current.sourceMetadata, input.sourceMetadata);
  const sourceKey =
    typeof sourceMetadata.checkpointSourceKey === "string"
      ? sourceMetadata.checkpointSourceKey
      : buildSourceKey(config.accountId);
  const accountId =
    typeof sourceMetadata.accountId === "string" && sourceMetadata.accountId.trim()
      ? sourceMetadata.accountId
      : config.accountId;
  const checkpoint = await getImportSourceCheckpoint(sourceKey);
  const windowMinutes = getWindowMinutes(sourceMetadata);
  const overlapMinutes = getOverlapMinutes(sourceMetadata);

  if (current.totalItems > 0 && ["queued", "processing", "completed"].includes(current.sourceStage)) {
    return current;
  }

  try {
    let reportStart = current.sourceWindowStart;
    let reportEnd = current.sourceWindowEnd;

    if (!reportStart || !reportEnd) {
      const now = new Date();
      const windowStart = getCheckpointStart({
        now,
        windowMinutes,
        overlapMinutes,
        checkpointBidDt: checkpoint?.lastSuccessfulBidDt ?? null,
      });

      reportStart = windowStart.toISOString();
      reportEnd = now.toISOString();
      sourceMetadata = updateDiagnostics(
        mergeSourceMetadata(sourceMetadata, {
          accountId,
          windowMinutes,
          overlapMinutes,
          checkpointSourceKey: sourceKey,
          checkpointBidDt: checkpoint?.lastSuccessfulBidDt ?? null,
          reportStart,
          reportEnd,
        }),
        {
          windowMinutes,
          overlapMinutes,
          checkpointSourceKey: sourceKey,
          checkpointBidDt: checkpoint?.lastSuccessfulBidDt ?? null,
          reportStart,
          reportEnd,
        },
      );
      current = (await updateImportRunSourceState({
        importRunId: input.importRunId,
        sourceStage: "creating_export",
        sourceWindowStart: reportStart,
        sourceWindowEnd: reportEnd,
        exportDownloadStatus: "pending",
        sourceMetadata,
        lastError: null,
      })) as ImportRunDetail;
    } else {
      sourceMetadata = updateDiagnostics(
        mergeSourceMetadata(sourceMetadata, {
          accountId,
          windowMinutes,
          overlapMinutes,
          checkpointSourceKey: sourceKey,
          checkpointBidDt: checkpoint?.lastSuccessfulBidDt ?? null,
          reportStart,
          reportEnd,
        }),
        {
          windowMinutes,
          overlapMinutes,
          checkpointSourceKey: sourceKey,
          checkpointBidDt: checkpoint?.lastSuccessfulBidDt ?? null,
          reportStart,
          reportEnd,
        },
      );
    }

    let exportJobId = current.exportJobId;

    if (!exportJobId) {
      const exportCreatedAt = new Date().toISOString();
      const exportJob = await createRtbExportJob({
        accountId,
        reportStart,
        reportEnd,
      });

      sourceMetadata = updateDiagnostics(
        sourceMetadata,
        {
          exportRequestUrl: exportJob.requestUrl,
          exportJobCreatedAt: exportCreatedAt,
        },
      );
      exportJobId = exportJob.jobId;
      current = (await updateImportRunSourceState({
        importRunId: input.importRunId,
        sourceStage: "polling_export",
        exportJobId,
        exportDownloadStatus: "pending",
        sourceMetadata,
        lastError: null,
      })) as ImportRunDetail;
    }

    const diagnostics = normalizeDiagnostics(sourceMetadata);
    let exportDownloadUrl =
      typeof diagnostics.exportDownloadUrl === "string" ? diagnostics.exportDownloadUrl : null;
    let exportStatus: RtbExportJobStatus = {
      status:
        typeof diagnostics.exportJobStatus === "string"
          ? diagnostics.exportJobStatus
          : "Unknown",
      url: exportDownloadUrl,
    };

    if (!exportDownloadUrl) {
      const pollStartedAt =
        typeof diagnostics.exportPollStartedAt === "string"
          ? diagnostics.exportPollStartedAt
          : new Date().toISOString();
      let totalPollCount =
        typeof diagnostics.exportPollCount === "number" ? diagnostics.exportPollCount : 0;

      current = (await updateImportRunSourceState({
        importRunId: input.importRunId,
        sourceStage: "polling_export",
        exportJobId,
        exportDownloadStatus: "pending",
        sourceMetadata: updateDiagnostics(sourceMetadata, {
          exportPollStartedAt: pollStartedAt,
          exportPollCount: totalPollCount,
        }),
        lastError: null,
      })) as ImportRunDetail;
      sourceMetadata = current.sourceMetadata;

      for (let attempt = 0; attempt < EXPORT_POLL_MAX_ATTEMPTS; attempt += 1) {
        totalPollCount += 1;
        exportStatus = await getRtbExportJobStatus({
          accountId,
          jobId: exportJobId,
        });

        if (exportStatus.url) {
          exportDownloadUrl = exportStatus.url;
          break;
        }

        await sleep(EXPORT_POLL_INTERVAL_MS);
      }

      if (!exportDownloadUrl) {
        throw new Error("RTB export job did not become ready before the polling limit.");
      }

      const exportReadyAt = new Date().toISOString();
      const exportJobCreatedAt =
        typeof diagnostics.exportJobCreatedAt === "string"
          ? diagnostics.exportJobCreatedAt
          : pollStartedAt;
      const exportReadyLatencyMs =
        new Date(exportReadyAt).getTime() - new Date(exportJobCreatedAt).getTime();

      sourceMetadata = updateDiagnostics(sourceMetadata, {
        exportPollStartedAt: pollStartedAt,
        exportPollCount: totalPollCount,
        exportReadyAt,
        exportReadyLatencyMs,
        exportJobStatus: exportStatus.status,
        exportDownloadUrl,
      });
      current = (await updateImportRunSourceState({
        importRunId: input.importRunId,
        sourceStage: "downloading",
        exportDownloadStatus: "ready",
        sourceMetadata,
        lastError: null,
      })) as ImportRunDetail;
    }

    const downloaded = await downloadRtbExportZip(exportDownloadUrl);
    sourceMetadata = updateDiagnostics(sourceMetadata, {
      exportDownloadedAt: new Date().toISOString(),
      downloadSizeBytes:
        downloaded.contentLengthBytes ?? downloaded.zipBytes.byteLength,
      exportDownloadUrl,
      exportJobStatus: exportStatus.status,
    });
    current = (await updateImportRunSourceState({
      importRunId: input.importRunId,
      sourceStage: "extracting",
      exportDownloadStatus: "downloaded",
      sourceMetadata,
      lastError: null,
    })) as ImportRunDetail;

    const extracted = extractCsvFromZip(downloaded.zipBytes);
    sourceMetadata = updateDiagnostics(sourceMetadata, {
      exportFileName: extracted.fileName,
      extractedAt: new Date().toISOString(),
    });
    current = (await updateImportRunSourceState({
      importRunId: input.importRunId,
      sourceStage: "parsing",
      exportDownloadStatus: "extracted",
      sourceMetadata,
      lastError: null,
    })) as ImportRunDetail;

    const parsed = parseRtbExportCsv(extracted.csvText);
    sourceMetadata = updateDiagnostics(sourceMetadata, {
      parsedAt: new Date().toISOString(),
      parsedRowCount: parsed.rowCount,
      extractedBidIdCount: parsed.extractedBidIdCount,
      dedupedBidIdCount: parsed.dedupedBidIdCount,
      duplicateBidIdsRemoved: parsed.duplicateCount,
      invalidBidIdCount: parsed.invalidBidIdCount,
      sampleBidIds: parsed.sampleBidIds,
      parsedHeaders: parsed.headers,
      earliestBidDt: parsed.earliestBidDt,
      latestBidDt: parsed.latestBidDt,
    });

    const addResult = await addImportRunItems({
      importRunId: input.importRunId,
      bidIds: parsed.bidIds,
      sourceStage: "queued",
      exportRowCount: parsed.rowCount,
      exportDownloadStatus: "parsed",
      sourceMetadata,
    });

    current = (await updateImportRunSourceState({
      importRunId: input.importRunId,
      sourceStage: "queued",
      exportDownloadStatus: "parsed",
      sourceMetadata: updateDiagnostics(addResult.detail.sourceMetadata, {
        insertedItemCount: addResult.insertedCount,
      }),
      lastError: null,
    })) as ImportRunDetail;

    if (parsed.latestBidDt && addResult.insertedCount > 0) {
      await upsertImportSourceCheckpoint({
        sourceKey,
        sourceType: "ringba_recent_import",
        lastSuccessfulBidDt: parsed.latestBidDt,
        sourceMetadata: {
          accountId,
          reportStart,
          reportEnd,
          exportFileName: extracted.fileName,
          insertedItemCount: addResult.insertedCount,
        },
      });
    }

    return current;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unexpected Ringba recent import error.";
    const failedDetail = await getImportRunDetail(input.importRunId);
    const failedMetadata = updateDiagnostics(
      failedDetail?.sourceMetadata ?? sourceMetadata,
      {
        failedStage: failedDetail?.sourceStage ?? current.sourceStage,
        sourceStageError: message,
      },
    );

    await updateImportRunSourceState({
      importRunId: input.importRunId,
      sourceStage: "failed",
      exportDownloadStatus: "failed",
      sourceMetadata: failedMetadata,
      lastError: message,
      completedAt: new Date().toISOString(),
    });

    throw error;
  }
}
