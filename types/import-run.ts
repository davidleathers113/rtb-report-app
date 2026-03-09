import type { InvestigationListItem } from "@/types/bid";

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

export type ImportRunStatus = (typeof IMPORT_RUN_STATUSES)[number];
export type ImportRunItemStatus = (typeof IMPORT_RUN_ITEM_STATUSES)[number];
export type ImportRunItemResolution = (typeof IMPORT_RUN_ITEM_RESOLUTIONS)[number];

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
  sourceType: string;
  status: ImportRunStatus;
  forceRefresh: boolean;
  notes: string | null;
  lastError: string | null;
  startedAt: string | null;
  completedAt: string | null;
  createdAt: string;
  updatedAt: string;
  items: ImportRunItemSummary[];
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
