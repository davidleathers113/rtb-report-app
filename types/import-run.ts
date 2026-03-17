import type { InvestigationListItem } from "@/types/bid";

export const IMPORT_RUN_SOURCE_TYPES = [
  "manual_bulk",
  "csv_upload",
  "csv_direct_import",
  "ringba_recent_import",
  "historical_ringba_backfill",
  "import_run_rerun",
] as const;

export const IMPORT_RUN_STATUSES = [
  "queued",
  "running",
  "completed",
  "completed_with_errors",
  "failed",
  "cancelled",
] as const;

export const IMPORT_RUN_ITEM_STATUSES = [
  "queued",
  "running",
  "completed",
  "failed",
] as const;

export const IMPORT_RUN_ITEM_RESOLUTIONS = [
  "reused",
  "fetched",
  "failed",
  "skipped",
] as const;

export const IMPORT_RUN_SOURCE_STAGES = [
  "creating_export",
  "polling_export",
  "downloading",
  "extracting",
  "parsing",
  "queued",
  "processing",
  "completed",
  "failed",
] as const;

export const IMPORT_RUN_EXPORT_DOWNLOAD_STATUSES = [
  "pending",
  "ready",
  "downloaded",
  "extracted",
  "parsed",
  "failed",
] as const;

export type ImportRunStatus = (typeof IMPORT_RUN_STATUSES)[number];
export type ImportRunItemStatus = (typeof IMPORT_RUN_ITEM_STATUSES)[number];
export type ImportRunItemResolution = (typeof IMPORT_RUN_ITEM_RESOLUTIONS)[number];
export type ImportRunSourceType = (typeof IMPORT_RUN_SOURCE_TYPES)[number];
export type ImportRunSourceStage = (typeof IMPORT_RUN_SOURCE_STAGES)[number];
export type ImportRunExportDownloadStatus =
  (typeof IMPORT_RUN_EXPORT_DOWNLOAD_STATUSES)[number];

export interface HistoricalBackfillMetrics extends Record<string, unknown> {
  attemptedCount: number;
  enrichedCount: number;
  reusedCount: number;
  notFoundCount: number;
  failedCount: number;
  rateLimitedCount: number;
  serverErrorCount: number;
  averageFetchLatencyMs: number | null;
  latencySampleCount: number;
  totalFetchLatencyMs: number;
}

export interface HistoricalBackfillCandidateSummary extends Record<string, unknown> {
  bidId: string;
  bidDt: string | null;
  campaignId: string | null;
  publisherId: string | null;
  sourceImportRunId: string | null;
  enrichmentState: string;
  nextRingbaRetryAt: string | null;
}

export interface HistoricalBackfillSourceMetadata extends Record<string, unknown> {
  selection?: {
    startBidDt?: string | null;
    endBidDt?: string | null;
    sort?: "newest_first" | "oldest_first";
    limit?: number;
    campaignId?: string | null;
    publisherId?: string | null;
    sourceImportRunId?: string | null;
  };
  pilotLabel?: string | null;
  throttleProfileName?: string | null;
  checkpointSourceKey?: string | null;
  checkpointCursor?: {
    bidDt?: string | null;
    bidId?: string | null;
  };
  candidateCount?: number;
  selectedCandidateCount?: number;
  remainingCandidateCount?: number;
  selectedCandidates?: HistoricalBackfillCandidateSummary[];
  metrics?: HistoricalBackfillMetrics;
}

export interface ImportRunItemSummary {
  id: string;
  importRunId: string;
  bidId: string;
  position: number;
  status: ImportRunItemStatus;
  resolution: ImportRunItemResolution | null;
  errorMessage: string | null;
  investigationId: string | null;
  startedAt: string | null;
  completedAt: string | null;
  attemptCount: number;
  createdAt: string;
  updatedAt: string;
  investigation: InvestigationListItem | null;
}

export interface ImportRunProgress {
  totalItems: number;
  queuedCount: number;
  runningCount: number;
  completedCount: number;
  reusedCount: number;
  fetchedCount: number;
  failedCount: number;
  percentComplete: number;
}

export interface ImportRunDetail extends ImportRunProgress {
  id: string;
  sourceType: ImportRunSourceType;
  triggerType: "manual" | "scheduled";
  scheduleId: string | null;
  sourceStage: ImportRunSourceStage;
  status: ImportRunStatus;
  forceRefresh: boolean;
  notes: string | null;
  lastError: string | null;
  sourceWindowStart: string | null;
  sourceWindowEnd: string | null;
  exportJobId: string | null;
  exportRowCount: number;
  exportDownloadStatus: ImportRunExportDownloadStatus | null;
  sourceMetadata: Record<string, unknown>;
  startedAt: string | null;
  completedAt: string | null;
  createdAt: string;
  updatedAt: string;
  items: ImportRunItemSummary[];
}

export interface RingbaRecentImportDiagnostics {
  windowMinutes?: number;
  overlapMinutes?: number;
  checkpointSourceKey?: string;
  checkpointBidDt?: string | null;
  reportStart?: string;
  reportEnd?: string;
  exportRequestUrl?: string;
  exportJobCreatedAt?: string;
  exportPollCount?: number;
  exportPollStartedAt?: string;
  exportReadyAt?: string;
  exportReadyLatencyMs?: number;
  exportJobStatus?: string;
  exportDownloadUrl?: string;
  exportDownloadedAt?: string;
  downloadSizeBytes?: number;
  exportFileName?: string;
  extractedAt?: string;
  parsedAt?: string;
  parsedRowCount?: number;
  extractedBidIdCount?: number;
  dedupedBidIdCount?: number;
  duplicateBidIdsRemoved?: number;
  invalidBidIdCount?: number;
  insertedItemCount?: number;
  parsedHeaders?: string[];
  sampleBidIds?: string[];
  earliestBidDt?: string | null;
  latestBidDt?: string | null;
  failedStage?: string;
  sourceStageError?: string;
}

export interface CsvPreviewColumnOption {
  key: string;
  label: string;
  index: number;
}

export interface CsvPreviewResult {
  fileName: string;
  totalRows: number;
  validBidIdCount: number;
  duplicateCount: number;
  invalidRowCount: number;
  selectedColumnKey: string;
  headerDetected: boolean;
  columnOptions: CsvPreviewColumnOption[];
  previewBidIds: string[];
  invalidRows: Array<{
    rowNumber: number;
    value: string;
    message: string;
  }>;
}

export interface ApiErrorResponse {
  error: string;
  code?: string;
  details?: Record<string, unknown>;
}

export interface CsvDirectHeaderMappingEntry {
  columnIndex: number;
  sourceHeader: string;
  normalizedHeader: string;
  storedKey: string;
  mappedField: string | null;
  duplicateIndex: number;
}

export interface CsvDirectDuplicateImportMatch {
  importRunId: string;
  sourceFileId: string;
  fileName: string;
  rowCount: number;
  createdAt: string;
  contentHash: string;
}

export interface CsvDirectImportSummary extends Record<string, unknown> {
  fileName: string;
  contentHash: string;
  storedRowCount: number;
  parsedRowCount: number;
  queuedItemCount: number;
  rejectedRowCount: number;
  skippedDuplicateRowCount: number;
  validBidIdCount: number;
  duplicateBidIdCount: number;
  missingBidIdCount: number;
  invalidBidIdCount: number;
  earliestBidDt: string | null;
  latestBidDt: string | null;
}

export interface CsvDirectSourceMetadata extends CsvDirectImportSummary {
  parsedHeaders: string[];
  headerMapping: CsvDirectHeaderMappingEntry[];
  duplicateImport: CsvDirectDuplicateImportMatch | null;
  warnings: string[];
  lastError?: string;
}

export interface CsvDirectPreviewResult {
  fileName: string;
  contentHash: string;
  totalRows: number;
  validBidIdCount: number;
  queuedItemCount: number;
  rejectedRowCount: number;
  skippedDuplicateRowCount: number;
  missingBidIdCount: number;
  duplicateBidIdCount: number;
  invalidBidIdCount: number;
  earliestBidDt: string | null;
  latestBidDt: string | null;
  headers: string[];
  duplicateImport: CsvDirectDuplicateImportMatch | null;
  sampleRows: Array<{
    rowNumber: number;
    bidId: string | null;
    bidDt: string | null;
    campaignName: string | null;
    publisherName: string | null;
    bidAmount: number | null;
    reasonForReject: string | null;
  }>;
  invalidRows: Array<{
    rowNumber: number;
    value: string;
    message: string;
  }>;
}

export interface CsvDirectImportResponse {
  preview: CsvDirectPreviewResult;
  summary: CsvDirectImportSummary;
  importRun: ImportRunDetail;
}
