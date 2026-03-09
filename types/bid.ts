export const ROOT_CAUSES = [
  "missing_caller_id",
  "missing_zip_or_required_payload_field",
  "payload_validation_error",
  "buyer_returned_zero_bid",
  "below_minimum_revenue",
  "rate_limited",
  "timeout",
  "confirmation_failure",
  "third_party_or_enrichment_failure",
  "no_eligible_targets",
  "unknown_needs_review",
] as const;

export const OWNER_TYPES = [
  "publisher",
  "buyer",
  "ringba_config",
  "system",
  "unknown",
] as const;

export const DIAGNOSIS_SEVERITIES = [
  "low",
  "medium",
  "high",
  "critical",
] as const;

export const OUTCOMES = ["accepted", "rejected", "zero_bid", "unknown"] as const;
export const FETCH_STATUSES = ["pending", "fetched", "failed"] as const;

export type RootCause = (typeof ROOT_CAUSES)[number];
export type OwnerType = (typeof OWNER_TYPES)[number];
export type DiagnosisSeverity = (typeof DIAGNOSIS_SEVERITIES)[number];
export type InvestigationOutcome = (typeof OUTCOMES)[number];
export type FetchStatus = (typeof FETCH_STATUSES)[number];

export interface BidEvent {
  id?: string;
  eventName: string;
  eventTimestamp: string | null;
  eventValsJson: Record<string, unknown> | null;
  eventStrValsJson: Record<string, string> | null;
}

export interface NormalizedBidData {
  bidId: string;
  bidDt: string | null;
  campaignName: string | null;
  campaignId: string | null;
  publisherName: string | null;
  publisherId: string | null;
  targetName: string | null;
  targetId: string | null;
  buyerName: string | null;
  buyerId: string | null;
  bidAmount: number | null;
  winningBid: number | null;
  isZeroBid: boolean;
  reasonForReject: string | null;
  httpStatusCode: number | null;
  errorMessage: string | null;
  requestBody: Record<string, unknown> | string | null;
  responseBody: Record<string, unknown> | string | null;
  rawTraceJson: Record<string, unknown>;
  relevantEvents: BidEvent[];
  outcome: InvestigationOutcome;
}

export interface DiagnosisEvidence {
  field: string;
  value: string | number | boolean | null;
  description: string;
}

export interface DiagnosisResult {
  rootCause: RootCause;
  confidence: number;
  severity: DiagnosisSeverity;
  ownerType: OwnerType;
  suggestedFix: string;
  explanation: string;
  evidence: DiagnosisEvidence[];
}

export interface PersistedBidInvestigation extends NormalizedBidData, DiagnosisResult {
  id: string;
  importRunId: string | null;
  fetchStatus: FetchStatus;
  fetchedAt: string | null;
  fetchStartedAt: string | null;
  lastError: string | null;
  refreshRequestedAt: string | null;
  leaseExpiresAt: string | null;
  fetchAttemptCount: number;
  importedAt: string;
  createdAt: string;
  updatedAt: string;
}

export interface InvestigationListItem {
  id: string;
  bidId: string;
  bidDt: string | null;
  campaignName: string | null;
  publisherName: string | null;
  targetName: string | null;
  bidAmount: number | null;
  winningBid: number | null;
  isZeroBid: boolean;
  httpStatusCode: number | null;
  rootCause: RootCause;
  ownerType: OwnerType;
  severity: DiagnosisSeverity;
  explanation: string;
  outcome: InvestigationOutcome;
  fetchStatus: FetchStatus;
  fetchedAt: string | null;
  lastError: string | null;
  importedAt: string;
}

export interface InvestigationDetail extends PersistedBidInvestigation {
  events: BidEvent[];
}

export interface InvestigationsPageData {
  items: InvestigationListItem[];
  total: number;
  page: number;
  pageSize: number;
}

export interface DashboardMetric {
  label: string;
  value: number;
}

export interface DashboardTimePoint {
  date: string;
  total: number;
  rejected: number;
  zeroBid: number;
}

export interface DashboardStats {
  totalInvestigated: number;
  acceptedCount: number;
  rejectedCount: number;
  zeroBidCount: number;
  topRootCauses: DashboardMetric[];
  topCampaigns: DashboardMetric[];
  topPublishers: DashboardMetric[];
  topTargets: DashboardMetric[];
  errorsByCategory: DashboardMetric[];
  bidsByOutcome: DashboardMetric[];
  issuesOverTime: DashboardTimePoint[];
  recentInvestigations: InvestigationListItem[];
}

export interface BulkInvestigationResponse {
  importRunId: string;
  totalSubmitted: number;
  totalProcessed: number;
  totalFetched: number;
  totalReused: number;
  items: InvestigationListItem[];
  failures: Array<{
    bidId: string;
    message: string;
  }>;
}
