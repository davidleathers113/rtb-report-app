export interface ImportSourceFileSummary {
  id: string;
  importRunId: string;
  fileName: string;
  rowCount: number;
  createdAt: string;
}

export interface ImportSourceRowListItem {
  id: string;
  importRunId: string;
  importSourceFileId: string;
  fileName: string;
  rowNumber: number;
  bidId: string | null;
  bidDt: string | null;
  campaignName: string | null;
  publisherName: string | null;
  bidAmount: number | null;
  reasonForReject: string | null;
  ingestStatus: string;
  ingestErrorCode: string | null;
  ingestErrorMessage: string | null;
  rowJson: Record<string, unknown>;
}

export interface ImportSourceRowsResponse {
  items: ImportSourceRowListItem[];
  total: number;
  limit: number;
  offset: number;
  files: ImportSourceFileSummary[];
}
