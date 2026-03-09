import "server-only";

import type {
  DashboardMetric,
  DashboardStats,
  DashboardTimePoint,
  DiagnosisResult,
  FetchStatus,
  InvestigationDetail,
  InvestigationListItem,
  InvestigationsPageData,
  NormalizedBidData,
} from "@/types/bid";
import { getSupabaseAdminClient } from "@/lib/db/server";

export interface BidInvestigationRow {
  id: string;
  import_run_id: string | null;
  bid_id: string;
  bid_dt: string | null;
  campaign_name: string | null;
  campaign_id: string | null;
  publisher_name: string | null;
  publisher_id: string | null;
  target_name: string | null;
  target_id: string | null;
  buyer_name: string | null;
  buyer_id: string | null;
  bid_amount: number | null;
  winning_bid: number | null;
  is_zero_bid: boolean;
  reason_for_reject: string | null;
  http_status_code: number | null;
  parsed_error_message: string | null;
  request_body: Record<string, unknown> | string | null;
  response_body: Record<string, unknown> | string | null;
  raw_trace_json: Record<string, unknown>;
  outcome: InvestigationListItem["outcome"];
  root_cause: InvestigationListItem["rootCause"];
  root_cause_confidence: number;
  severity: InvestigationListItem["severity"];
  owner_type: InvestigationListItem["ownerType"];
  suggested_fix: string;
  explanation: string;
  evidence_json: DiagnosisResult["evidence"];
  fetch_status: FetchStatus;
  fetched_at: string | null;
  fetch_started_at: string | null;
  last_error: string | null;
  refresh_requested_at: string | null;
  lease_expires_at: string | null;
  fetch_attempt_count: number;
  imported_at: string;
  created_at: string;
  updated_at: string;
}

interface BidEventRow {
  id: string;
  bid_investigation_id: string;
  event_name: string;
  event_timestamp: string | null;
  event_vals_json: Record<string, unknown> | null;
  event_str_vals_json: Record<string, string> | null;
  created_at: string;
  updated_at: string;
}

function toListItem(row: BidInvestigationRow): InvestigationListItem {
  return {
    id: row.id,
    bidId: row.bid_id,
    bidDt: row.bid_dt,
    campaignName: row.campaign_name,
    publisherName: row.publisher_name,
    targetName: row.target_name,
    bidAmount: row.bid_amount,
    winningBid: row.winning_bid,
    isZeroBid: row.is_zero_bid,
    httpStatusCode: row.http_status_code,
    rootCause: row.root_cause,
    ownerType: row.owner_type,
    severity: row.severity,
    explanation: row.explanation,
    outcome: row.outcome,
    fetchStatus: row.fetch_status,
    fetchedAt: row.fetched_at,
    lastError: row.last_error,
    importedAt: row.imported_at,
  };
}

function toDetail(
  row: BidInvestigationRow,
  eventRows: BidEventRow[],
): InvestigationDetail {
  return {
    id: row.id,
    importRunId: row.import_run_id,
    bidId: row.bid_id,
    bidDt: row.bid_dt,
    campaignName: row.campaign_name,
    campaignId: row.campaign_id,
    publisherName: row.publisher_name,
    publisherId: row.publisher_id,
    targetName: row.target_name,
    targetId: row.target_id,
    buyerName: row.buyer_name,
    buyerId: row.buyer_id,
    bidAmount: row.bid_amount,
    winningBid: row.winning_bid,
    isZeroBid: row.is_zero_bid,
    reasonForReject: row.reason_for_reject,
    httpStatusCode: row.http_status_code,
    errorMessage: row.parsed_error_message,
    requestBody: row.request_body,
    responseBody: row.response_body,
    rawTraceJson: row.raw_trace_json,
    relevantEvents: eventRows.map((event) => ({
      id: event.id,
      eventName: event.event_name,
      eventTimestamp: event.event_timestamp,
      eventValsJson: event.event_vals_json,
      eventStrValsJson: event.event_str_vals_json,
    })),
    events: eventRows.map((event) => ({
      id: event.id,
      eventName: event.event_name,
      eventTimestamp: event.event_timestamp,
      eventValsJson: event.event_vals_json,
      eventStrValsJson: event.event_str_vals_json,
    })),
    outcome: row.outcome,
    rootCause: row.root_cause,
    confidence: row.root_cause_confidence,
    severity: row.severity,
    ownerType: row.owner_type,
    suggestedFix: row.suggested_fix,
    explanation: row.explanation,
    evidence: row.evidence_json,
    fetchStatus: row.fetch_status,
    fetchedAt: row.fetched_at,
    fetchStartedAt: row.fetch_started_at,
    lastError: row.last_error,
    refreshRequestedAt: row.refresh_requested_at,
    leaseExpiresAt: row.lease_expires_at,
    fetchAttemptCount: row.fetch_attempt_count,
    importedAt: row.imported_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function incrementMetric(map: Map<string, number>, key: string | null | undefined) {
  if (!key) {
    return;
  }

  map.set(key, (map.get(key) ?? 0) + 1);
}

function topMetrics(map: Map<string, number>, limit: number): DashboardMetric[] {
  return Array.from(map.entries())
    .sort((left, right) => right[1] - left[1])
    .slice(0, limit)
    .map(([label, value]) => ({
      label,
      value,
    }));
}

function buildIssuesOverTime(rows: BidInvestigationRow[]): DashboardTimePoint[] {
  const byDate = new Map<string, DashboardTimePoint>();

  for (const row of rows) {
    const dateKey = (row.bid_dt ?? row.imported_at).slice(0, 10);
    const existing = byDate.get(dateKey) ?? {
      date: dateKey,
      total: 0,
      rejected: 0,
      zeroBid: 0,
    };

    existing.total += 1;
    if (row.outcome === "rejected") {
      existing.rejected += 1;
    }
    if (row.outcome === "zero_bid") {
      existing.zeroBid += 1;
    }

    byDate.set(dateKey, existing);
  }

  return Array.from(byDate.values()).sort((left, right) =>
    left.date.localeCompare(right.date),
  );
}

function sanitizeSearch(search: string) {
  return search
    .split(",")
    .join("")
    .split("(")
    .join("")
    .split(")")
    .join("")
    .trim();
}

interface ClaimBidInvestigationRow {
  id: string;
  bid_id: string;
  fetch_status: FetchStatus;
  should_fetch: boolean;
  fetched_at: string | null;
  last_error: string | null;
  fetch_attempt_count: number;
  lease_expires_at: string | null;
}

async function getBidInvestigationRowByBidId(bidId: string) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("bid_investigations")
    .select("*")
    .eq("bid_id", bidId)
    .single();

  if (error) {
    if (error.code === "PGRST116") {
      return null;
    }

    throw new Error(`Unable to fetch investigation row: ${error.message}`);
  }

  return data as BidInvestigationRow;
}

export async function createImportRun(sourceType: string, notes?: string) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("import_runs")
    .insert({
      source_type: sourceType,
      status: "running",
      notes: notes ?? null,
      started_at: new Date().toISOString(),
    })
    .select("id")
    .single();

  if (error || !data) {
    throw new Error(`Unable to create import run: ${error?.message ?? "unknown error"}`);
  }

  return data.id as string;
}

export async function completeImportRun(input: {
  id: string;
  status: string;
  totalFound: number;
  totalProcessed: number;
  notes?: string;
}) {
  const supabase = getSupabaseAdminClient();
  const { error } = await supabase
    .from("import_runs")
    .update({
      status: input.status,
      total_found: input.totalFound,
      total_processed: input.totalProcessed,
      notes: input.notes ?? null,
      completed_at: new Date().toISOString(),
    })
    .eq("id", input.id);

  if (error) {
    throw new Error(`Unable to complete import run: ${error.message}`);
  }
}

export async function upsertInvestigation(input: {
  importRunId: string | null;
  normalizedBid: NormalizedBidData;
  diagnosis: DiagnosisResult;
}) {
  const supabase = getSupabaseAdminClient();

  const rowToPersist = {
    import_run_id: input.importRunId,
    bid_id: input.normalizedBid.bidId,
    bid_dt: input.normalizedBid.bidDt,
    campaign_name: input.normalizedBid.campaignName,
    campaign_id: input.normalizedBid.campaignId,
    publisher_name: input.normalizedBid.publisherName,
    publisher_id: input.normalizedBid.publisherId,
    target_name: input.normalizedBid.targetName,
    target_id: input.normalizedBid.targetId,
    buyer_name: input.normalizedBid.buyerName,
    buyer_id: input.normalizedBid.buyerId,
    bid_amount: input.normalizedBid.bidAmount,
    winning_bid: input.normalizedBid.winningBid,
    is_zero_bid: input.normalizedBid.isZeroBid,
    reason_for_reject: input.normalizedBid.reasonForReject,
    http_status_code: input.normalizedBid.httpStatusCode,
    parsed_error_message: input.normalizedBid.errorMessage,
    request_body: input.normalizedBid.requestBody,
    response_body: input.normalizedBid.responseBody,
    raw_trace_json: input.normalizedBid.rawTraceJson,
    outcome: input.normalizedBid.outcome,
    root_cause: input.diagnosis.rootCause,
    root_cause_confidence: input.diagnosis.confidence,
    severity: input.diagnosis.severity,
    owner_type: input.diagnosis.ownerType,
    suggested_fix: input.diagnosis.suggestedFix,
    explanation: input.diagnosis.explanation,
    evidence_json: input.diagnosis.evidence,
    fetch_status: "fetched" as const,
    fetched_at: new Date().toISOString(),
    last_error: null,
    lease_expires_at: null,
    imported_at: new Date().toISOString(),
  };

  const { data, error } = await supabase
    .from("bid_investigations")
    .upsert(rowToPersist, {
      onConflict: "bid_id",
    })
    .select("*")
    .single();

  if (error || !data) {
    throw new Error(`Unable to upsert investigation: ${error?.message ?? "unknown error"}`);
  }

  const investigationId = (data as BidInvestigationRow).id;

  const { error: deleteEventsError } = await supabase
    .from("bid_events")
    .delete()
    .eq("bid_investigation_id", investigationId);

  if (deleteEventsError) {
    throw new Error(`Unable to replace investigation events: ${deleteEventsError.message}`);
  }

  if (input.normalizedBid.relevantEvents.length > 0) {
    const { error: insertEventsError } = await supabase.from("bid_events").insert(
      input.normalizedBid.relevantEvents.map((event) => ({
        bid_investigation_id: investigationId,
        event_name: event.eventName,
        event_timestamp: event.eventTimestamp,
        event_vals_json: event.eventValsJson,
        event_str_vals_json: event.eventStrValsJson,
      })),
    );

    if (insertEventsError) {
      throw new Error(`Unable to insert investigation events: ${insertEventsError.message}`);
    }
  }

  return getInvestigationByBidId(input.normalizedBid.bidId);
}

export async function claimInvestigationFetch(input: {
  bidId: string;
  importRunId: string | null;
  forceRefresh: boolean;
  leaseSeconds?: number;
}) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase.rpc("claim_bid_investigation", {
    p_bid_id: input.bidId,
    p_import_run_id: input.importRunId,
    p_force_refresh: input.forceRefresh,
    p_lease_seconds: input.leaseSeconds ?? 120,
  });

  if (error) {
    throw new Error(`Unable to claim investigation fetch: ${error.message}`);
  }

  const row = (data?.[0] ?? null) as ClaimBidInvestigationRow | null;

  if (!row) {
    throw new Error("Unable to claim investigation fetch: missing claim row.");
  }

  return {
    id: row.id,
    bidId: row.bid_id,
    fetchStatus: row.fetch_status,
    shouldFetch: row.should_fetch,
    fetchedAt: row.fetched_at,
    lastError: row.last_error,
    fetchAttemptCount: row.fetch_attempt_count,
    leaseExpiresAt: row.lease_expires_at,
  };
}

export async function markInvestigationFetchFailed(input: {
  bidId: string;
  importRunId: string | null;
  errorMessage: string;
  httpStatusCode?: number | null;
  responseBody?: Record<string, unknown> | string | null;
  rawTraceJson?: Record<string, unknown>;
}) {
  const supabase = getSupabaseAdminClient();
  const existing = await getBidInvestigationRowByBidId(input.bidId);
  const shouldPreserveExistingData = Boolean(existing?.fetched_at);
  const updatePayload: Record<string, unknown> = {
    import_run_id: input.importRunId,
    fetch_status: "failed",
    parsed_error_message: input.errorMessage,
    last_error: input.errorMessage,
    lease_expires_at: null,
    imported_at: new Date().toISOString(),
  };

  if (!shouldPreserveExistingData) {
    updatePayload.http_status_code = input.httpStatusCode ?? null;
    updatePayload.response_body = input.responseBody ?? null;
    updatePayload.raw_trace_json = input.rawTraceJson ?? {};
    updatePayload.outcome = "unknown";
    updatePayload.root_cause = "unknown_needs_review";
    updatePayload.root_cause_confidence = 0.2;
    updatePayload.severity = "high";
    updatePayload.owner_type = "system";
    updatePayload.suggested_fix =
      "Retry the fetch or inspect Ringba credentials, connectivity, and upstream API availability.";
    updatePayload.explanation =
      "The investigation could not complete because the Ringba bid detail fetch failed before normalization finished.";
    updatePayload.evidence_json = [
      {
        field: "last_error",
        value: input.errorMessage,
        description: "Ringba fetch failure captured by the persistence layer.",
      },
    ] as DiagnosisResult["evidence"];
  }

  const { error } = await supabase
    .from("bid_investigations")
    .update(updatePayload)
    .eq("bid_id", input.bidId);

  if (error) {
    throw new Error(`Unable to mark fetch failure: ${error.message}`);
  }

  return getInvestigationByBidId(input.bidId);
}

export async function getInvestigations(input: {
  page: number;
  pageSize: number;
  rootCause?: string;
  ownerType?: string;
  search?: string;
}): Promise<InvestigationsPageData> {
  const supabase = getSupabaseAdminClient();
  const from = (input.page - 1) * input.pageSize;
  const to = from + input.pageSize - 1;

  let query = supabase
    .from("bid_investigations")
    .select("*", { count: "exact" })
    .order("bid_dt", { ascending: false, nullsFirst: false })
    .order("imported_at", { ascending: false })
    .range(from, to);

  if (input.rootCause) {
    query = query.eq("root_cause", input.rootCause);
  }

  if (input.ownerType) {
    query = query.eq("owner_type", input.ownerType);
  }

  if (input.search) {
    const search = sanitizeSearch(input.search);
    if (search) {
      query = query.or(
        [
          `bid_id.ilike.%${search}%`,
          `campaign_name.ilike.%${search}%`,
          `publisher_name.ilike.%${search}%`,
          `target_name.ilike.%${search}%`,
        ].join(","),
      );
    }
  }

  const { data, error, count } = await query;

  if (error) {
    throw new Error(`Unable to fetch investigations: ${error.message}`);
  }

  const rows = (data ?? []) as BidInvestigationRow[];

  return {
    items: rows.map(toListItem),
    total: count ?? rows.length,
    page: input.page,
    pageSize: input.pageSize,
  };
}

export async function getInvestigationByBidId(bidId: string) {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("bid_investigations")
    .select("*")
    .eq("bid_id", bidId)
    .single();

  if (error) {
    if (error.code === "PGRST116") {
      return null;
    }

    throw new Error(`Unable to fetch investigation detail: ${error.message}`);
  }

  const investigation = data as BidInvestigationRow;

  const { data: eventData, error: eventError } = await supabase
    .from("bid_events")
    .select("*")
    .eq("bid_investigation_id", investigation.id)
    .order("event_timestamp", { ascending: true, nullsFirst: false })
    .order("created_at", { ascending: true });

  if (eventError) {
    throw new Error(`Unable to fetch investigation events: ${eventError.message}`);
  }

  return toDetail(investigation, (eventData ?? []) as BidEventRow[]);
}

export async function getDashboardStats(): Promise<DashboardStats> {
  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("bid_investigations")
    .select("*")
    .order("bid_dt", { ascending: false, nullsFirst: false })
    .order("imported_at", { ascending: false })
    .limit(1000);

  if (error) {
    throw new Error(`Unable to fetch dashboard stats: ${error.message}`);
  }

  const rows = (data ?? []) as BidInvestigationRow[];
  const rootCauseCounts = new Map<string, number>();
  const campaignCounts = new Map<string, number>();
  const publisherCounts = new Map<string, number>();
  const targetCounts = new Map<string, number>();
  const outcomeCounts = new Map<string, number>();

  let acceptedCount = 0;
  let rejectedCount = 0;
  let zeroBidCount = 0;

  for (const row of rows) {
    incrementMetric(rootCauseCounts, row.root_cause);
    incrementMetric(campaignCounts, row.campaign_name);
    incrementMetric(publisherCounts, row.publisher_name);
    incrementMetric(targetCounts, row.target_name);
    incrementMetric(outcomeCounts, row.outcome);

    if (row.outcome === "accepted") {
      acceptedCount += 1;
    }
    if (row.outcome === "rejected") {
      rejectedCount += 1;
    }
    if (row.outcome === "zero_bid") {
      zeroBidCount += 1;
    }
  }

  return {
    totalInvestigated: rows.length,
    acceptedCount,
    rejectedCount,
    zeroBidCount,
    topRootCauses: topMetrics(rootCauseCounts, 5),
    topCampaigns: topMetrics(campaignCounts, 5),
    topPublishers: topMetrics(publisherCounts, 5),
    topTargets: topMetrics(targetCounts, 5),
    errorsByCategory: topMetrics(rootCauseCounts, 8),
    bidsByOutcome: topMetrics(outcomeCounts, 4),
    issuesOverTime: buildIssuesOverTime(rows),
    recentInvestigations: rows.slice(0, 10).map(toListItem),
  };
}

export async function getInvestigationsForExport(input: {
  rootCause?: string;
  ownerType?: string;
  search?: string;
}) {
  const result = await getInvestigations({
    page: 1,
    pageSize: 1000,
    rootCause: input.rootCause,
    ownerType: input.ownerType,
    search: input.search,
  });

  const supabase = getSupabaseAdminClient();
  const bidIds = result.items.map((item) => item.bidId);

  if (bidIds.length === 0) {
    return [];
  }

  const { data, error } = await supabase
    .from("bid_investigations")
    .select("*")
    .in("bid_id", bidIds)
    .order("bid_dt", { ascending: false, nullsFirst: false })
    .order("imported_at", { ascending: false });

  if (error) {
    throw new Error(`Unable to fetch export rows: ${error.message}`);
  }

  return (data ?? []) as BidInvestigationRow[];
}

export async function getInvestigationListItemsByIds(ids: string[]) {
  const uniqueIds = Array.from(new Set(ids.filter(Boolean)));

  if (uniqueIds.length === 0) {
    return [];
  }

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("bid_investigations")
    .select("*")
    .in("id", uniqueIds);

  if (error) {
    throw new Error(`Unable to fetch investigation list items by ids: ${error.message}`);
  }

  return ((data ?? []) as BidInvestigationRow[]).map(toListItem);
}
