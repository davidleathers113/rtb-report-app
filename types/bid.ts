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
export const DETAIL_SOURCES = ["csv_direct", "ringba_api"] as const;
export const OUTCOME_REASON_CATEGORIES = [
  "accepted",
  "buyer_returned_zero_bid",
  "below_minimum_revenue",
  "missing_required_field",
  "missing_caller_id",
  "tag_filtered_initial",
  "tag_filtered_final",
  "no_matching_buyer",
  "no_capacity",
  "request_invalid",
  "rate_limited",
  "unknown_no_payable_bid",
] as const;
export const CLASSIFICATION_SOURCES = [
  "primary_attempt_structured",
  "response_body_structured",
  "primary_attempt_error_code",
  "top_level_error",
  "reason_for_reject_text",
  "is_zero_bid_flag",
  "heuristic",
] as const;
export const NORMALIZATION_PARSE_STATUSES = [
  "complete",
  "partial",
  "text_fallback",
  "shape_unknown",
  "not_attempted",
] as const;
export const ENRICHMENT_STATES = [
  "csv_only",
  "queued",
  "fetching",
  "enriched",
  "not_found",
  "failed",
] as const;
export const FAILURE_STAGES = [
  "accepted",
  "target_rejected",
  "zero_bid",
  "routing",
  "fetch_failed",
  "unknown",
] as const;

export type RootCause = (typeof ROOT_CAUSES)[number];
export type OwnerType = (typeof OWNER_TYPES)[number];
export type DiagnosisSeverity = (typeof DIAGNOSIS_SEVERITIES)[number];
export type InvestigationOutcome = (typeof OUTCOMES)[number];
export type FetchStatus = (typeof FETCH_STATUSES)[number];
export type DetailSource = (typeof DETAIL_SOURCES)[number];
export type OutcomeReasonCategory = (typeof OUTCOME_REASON_CATEGORIES)[number];
export type ClassificationSource = (typeof CLASSIFICATION_SOURCES)[number];
export type NormalizationParseStatus = (typeof NORMALIZATION_PARSE_STATUSES)[number];
export type EnrichmentState = (typeof ENRICHMENT_STATES)[number];
export type FailureStage = (typeof FAILURE_STAGES)[number];

export interface NormalizationWarning {
  code: string;
  message: string;
  field: string | null;
}

export interface ClassificationWarning {
  code: string;
  message: string;
  field: string | null;
}

export interface BidEvent {
  id?: string;
  eventName: string;
  eventTimestamp: string | null;
  eventValsJson: Record<string, unknown> | null;
  eventStrValsJson: Record<string, string> | null;
}

export interface BidTargetAttempt {
  id?: string;
  sequence: number;
  eventName: string;
  eventTimestamp: string | null;
  targetName: string | null;
  targetId: string | null;
  targetBuyer: string | null;
  targetBuyerId: string | null;
  targetNumber: string | null;
  targetGroupName: string | null;
  targetGroupId: string | null;
  targetSubId: string | null;
  targetBuyerSubId: string | null;
  requestUrl: string | null;
  httpMethod: string | null;
  requestStatus: string | null;
  httpStatusCode: number | null;
  durationMs: number | null;
  routePriority: number | null;
  routeWeight: number | null;
  accepted: boolean | null;
  winning: boolean | null;
  bidAmount: number | null;
  minDurationSeconds: number | null;
  rejectReason: string | null;
  errorCode: number | null;
  errorMessage: string | null;
  errors: string[];
  requestBody: Record<string, unknown> | string | null;
  responseBody: Record<string, unknown> | string | null;
  summaryReason: string | null;
  rawEventJson: Record<string, unknown>;
}

export interface InvestigationSourceContext {
  fileName: string;
  rowNumber: number;
  bidDt: string | null;
  bidAmount: number | null;
  reasonForReject: string | null;
  rowJson: Record<string, unknown>;
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
  bidElapsedMs: number | null;
  isZeroBid: boolean;
  reasonForReject: string | null;
  httpStatusCode: number | null;
  errorMessage: string | null;
  primaryFailureStage: FailureStage;
  primaryTargetName: string | null;
  primaryTargetId: string | null;
  primaryBuyerName: string | null;
  primaryBuyerId: string | null;
  primaryErrorCode: number | null;
  primaryErrorMessage: string | null;
  requestBody: Record<string, unknown> | string | null;
  responseBody: Record<string, unknown> | string | null;
  rawTraceJson: Record<string, unknown>;
  relevantEvents: BidEvent[];
  targetAttempts: BidTargetAttempt[];
  outcome: InvestigationOutcome;
  outcomeReasonCategory: OutcomeReasonCategory | null;
  outcomeReasonCode: string | null;
  outcomeReasonMessage: string | null;
  classificationSource: ClassificationSource | null;
  classificationConfidence: number | null;
  classificationWarnings: ClassificationWarning[];
  parseStatus: NormalizationParseStatus;
  normalizationVersion: string | null;
  schemaVariant: string | null;
  normalizationConfidence: number | null;
  normalizationWarnings: NormalizationWarning[];
  missingCriticalFields: string[];
  missingOptionalFields: string[];
  unknownEventNames: string[];
  rawPathsUsed: Record<string, string[]>;
  primaryErrorCodeSource: string | null;
  primaryErrorCodeConfidence: number | null;
  primaryErrorCodeRawMatch: string | null;
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
  detailSource: DetailSource;
  enrichmentState: EnrichmentState;
  fetchStatus: FetchStatus;
  fetchedAt: string | null;
  fetchStartedAt: string | null;
  lastError: string | null;
  lastRingbaAttemptAt: string | null;
  lastRingbaFetchAt: string | null;
  ringbaFailureCount: number;
  nextRingbaRetryAt: string | null;
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
  bidElapsedMs: number | null;
  isZeroBid: boolean;
  httpStatusCode: number | null;
  primaryFailureStage: FailureStage;
  primaryTargetName: string | null;
  primaryBuyerName: string | null;
  primaryErrorCode: number | null;
  primaryErrorMessage: string | null;
  rootCause: RootCause;
  ownerType: OwnerType;
  severity: DiagnosisSeverity;
  explanation: string;
  outcome: InvestigationOutcome;
  outcomeReasonCategory: OutcomeReasonCategory | null;
  outcomeReasonCode: string | null;
  outcomeReasonMessage: string | null;
  classificationSource: ClassificationSource | null;
  classificationConfidence: number | null;
  detailSource: DetailSource;
  enrichmentState: EnrichmentState;
  fetchStatus: FetchStatus;
  parseStatus: NormalizationParseStatus;
  schemaVariant: string | null;
  normalizationConfidence: number | null;
  normalizationWarningCount: number;
  primaryErrorCodeSource: string | null;
  fetchedAt: string | null;
  lastError: string | null;
  importedAt: string;
}

export interface InvestigationDetail extends PersistedBidInvestigation {
  events: BidEvent[];
  targetAttempts: BidTargetAttempt[];
  sourceContext: InvestigationSourceContext | null;
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
