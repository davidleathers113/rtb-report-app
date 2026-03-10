import "server-only";

import { eq, inArray } from "drizzle-orm";

import { getDb, getSqlite } from "@/lib/db/client";
import { getImportSourceRowContextForBidId } from "@/lib/db/import-sources";
import {
  bidEvents,
  bidInvestigations,
  bidTargetAttempts,
  importRuns,
  type BidEventRow,
  type BidInvestigationRow,
  type BidTargetAttemptRow,
} from "@/lib/db/schema";
import { addSeconds, createId, nowIso, toTimestamp } from "@/lib/db/utils";
import type {
  DashboardMetric,
  DashboardStats,
  DashboardTimePoint,
  DiagnosisResult,
  FetchStatus,
  InvestigationDetail,
  InvestigationListItem,
  InvestigationSourceContext,
  InvestigationsPageData,
  NormalizedBidData,
} from "@/types/bid";

const SQLITE_IN_ARRAY_CHUNK_SIZE = 900;

function splitIntoChunks<T>(values: T[], chunkSize: number) {
  const chunks: T[][] = [];

  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }

  return chunks;
}

function toListItem(row: BidInvestigationRow): InvestigationListItem {
  return {
    id: row.id,
    bidId: row.bidId,
    bidDt: row.bidDt,
    campaignName: row.campaignName,
    publisherName: row.publisherName,
    targetName: row.targetName,
    bidAmount: row.bidAmount,
    winningBid: row.winningBid,
    bidElapsedMs: row.bidElapsedMs,
    isZeroBid: row.isZeroBid,
    httpStatusCode: row.httpStatusCode,
    primaryFailureStage: row.primaryFailureStage as InvestigationListItem["primaryFailureStage"],
    primaryTargetName: row.primaryTargetName,
    primaryBuyerName: row.primaryBuyerName,
    primaryErrorCode: row.primaryErrorCode,
    primaryErrorMessage: row.primaryErrorMessage,
    rootCause: row.rootCause as InvestigationListItem["rootCause"],
    ownerType: row.ownerType as InvestigationListItem["ownerType"],
    severity: row.severity as InvestigationListItem["severity"],
    explanation: row.explanation,
    outcome: row.outcome as InvestigationListItem["outcome"],
    fetchStatus: row.fetchStatus as FetchStatus,
    fetchedAt: row.fetchedAt,
    lastError: row.lastError,
    importedAt: row.importedAt,
  };
}

function toDetail(
  row: BidInvestigationRow,
  eventRows: BidEventRow[],
  targetAttemptRows: BidTargetAttemptRow[],
  sourceContext: InvestigationSourceContext | null,
): InvestigationDetail {
  return {
    id: row.id,
    importRunId: row.importRunId,
    bidId: row.bidId,
    bidDt: row.bidDt,
    campaignName: row.campaignName,
    campaignId: row.campaignId,
    publisherName: row.publisherName,
    publisherId: row.publisherId,
    targetName: row.targetName,
    targetId: row.targetId,
    buyerName: row.buyerName,
    buyerId: row.buyerId,
    bidAmount: row.bidAmount,
    winningBid: row.winningBid,
    bidElapsedMs: row.bidElapsedMs,
    isZeroBid: row.isZeroBid,
    reasonForReject: row.reasonForReject,
    httpStatusCode: row.httpStatusCode,
    errorMessage: row.parsedErrorMessage,
    primaryFailureStage: row.primaryFailureStage as InvestigationDetail["primaryFailureStage"],
    primaryTargetName: row.primaryTargetName,
    primaryTargetId: row.primaryTargetId,
    primaryBuyerName: row.primaryBuyerName,
    primaryBuyerId: row.primaryBuyerId,
    primaryErrorCode: row.primaryErrorCode,
    primaryErrorMessage: row.primaryErrorMessage,
    requestBody: (row.requestBody ?? null) as Record<string, unknown> | string | null,
    responseBody: (row.responseBody ?? null) as Record<string, unknown> | string | null,
    rawTraceJson: (row.rawTraceJson ?? {}) as Record<string, unknown>,
    relevantEvents: eventRows.map((event) => ({
      id: event.id,
      eventName: event.eventName,
      eventTimestamp: event.eventTimestamp,
      eventValsJson: (event.eventValsJson ?? null) as Record<string, unknown> | null,
      eventStrValsJson: (event.eventStrValsJson ?? null) as Record<string, string> | null,
    })),
    events: eventRows.map((event) => ({
      id: event.id,
      eventName: event.eventName,
      eventTimestamp: event.eventTimestamp,
      eventValsJson: (event.eventValsJson ?? null) as Record<string, unknown> | null,
      eventStrValsJson: (event.eventStrValsJson ?? null) as Record<string, string> | null,
    })),
    targetAttempts: targetAttemptRows.map((attempt) => ({
      id: attempt.id,
      sequence: attempt.sequence,
      eventName: attempt.eventName,
      eventTimestamp: attempt.eventTimestamp,
      targetName: attempt.targetName,
      targetId: attempt.targetId,
      targetBuyer: attempt.targetBuyer,
      targetBuyerId: attempt.targetBuyerId,
      targetNumber: attempt.targetNumber,
      targetGroupName: attempt.targetGroupName,
      targetGroupId: attempt.targetGroupId,
      targetSubId: attempt.targetSubId,
      targetBuyerSubId: attempt.targetBuyerSubId,
      requestUrl: attempt.requestUrl,
      httpMethod: attempt.httpMethod,
      requestStatus: attempt.requestStatus,
      httpStatusCode: attempt.httpStatusCode,
      durationMs: attempt.durationMs,
      routePriority: attempt.routePriority,
      routeWeight: attempt.routeWeight,
      accepted: attempt.accepted,
      winning: attempt.winning,
      bidAmount: attempt.bidAmount,
      minDurationSeconds: attempt.minDurationSeconds,
      rejectReason: attempt.rejectReason,
      errorCode: attempt.errorCode,
      errorMessage: attempt.errorMessage,
      errors: (attempt.errorsJson ?? []) as string[],
      requestBody: (attempt.requestBody ?? null) as Record<string, unknown> | string | null,
      responseBody: (attempt.responseBody ?? null) as Record<string, unknown> | string | null,
      summaryReason: attempt.summaryReason,
      rawEventJson: (attempt.rawEventJson ?? {}) as Record<string, unknown>,
    })),
    outcome: row.outcome as InvestigationDetail["outcome"],
    rootCause: row.rootCause as InvestigationDetail["rootCause"],
    confidence: row.rootCauseConfidence,
    severity: row.severity as InvestigationDetail["severity"],
    ownerType: row.ownerType as InvestigationDetail["ownerType"],
    suggestedFix: row.suggestedFix,
    explanation: row.explanation,
    evidence: (row.evidenceJson ?? []) as DiagnosisResult["evidence"],
    fetchStatus: row.fetchStatus as FetchStatus,
    fetchedAt: row.fetchedAt,
    fetchStartedAt: row.fetchStartedAt,
    lastError: row.lastError,
    refreshRequestedAt: row.refreshRequestedAt,
    leaseExpiresAt: row.leaseExpiresAt,
    fetchAttemptCount: row.fetchAttemptCount,
    importedAt: row.importedAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    sourceContext,
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
    const dateKey = (row.bidDt ?? row.importedAt).slice(0, 10);
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
  bidId: string;
  fetchStatus: FetchStatus;
  shouldFetch: boolean;
  fetchedAt: string | null;
  lastError: string | null;
  fetchAttemptCount: number;
  leaseExpiresAt: string | null;
}

async function getBidInvestigationRowByBidId(bidId: string) {
  const db = getDb();
  const row = db
    .select()
    .from(bidInvestigations)
    .where(eq(bidInvestigations.bidId, bidId))
    .get() as BidInvestigationRow | undefined;
  return row ?? null;
}

export async function createImportRun(sourceType: string, notes?: string) {
  const db = getDb();
  const id = createId();
  const now = nowIso();
  db.insert(importRuns)
    .values({
      id,
      sourceType,
      status: "running",
      notes: notes ?? null,
      startedAt: now,
      createdAt: now,
      updatedAt: now,
    })
    .run();
  return id;
}

export async function completeImportRun(input: {
  id: string;
  status: string;
  totalFound: number;
  totalProcessed: number;
  notes?: string;
}) {
  const db = getDb();
  const now = nowIso();
  db.update(importRuns)
    .set({
      status: input.status,
      totalFound: input.totalFound,
      totalProcessed: input.totalProcessed,
      notes: input.notes ?? null,
      completedAt: now,
      updatedAt: now,
    })
    .where(eq(importRuns.id, input.id))
    .run();
}

export async function upsertInvestigation(input: {
  importRunId: string | null;
  normalizedBid: NormalizedBidData;
  diagnosis: DiagnosisResult;
}) {
  const db = getDb();
  const now = nowIso();
  const existing = await getBidInvestigationRowByBidId(input.normalizedBid.bidId);
  const investigationId = existing?.id ?? createId();

  const rowToPersist = {
    id: investigationId,
    importRunId: input.importRunId,
    bidId: input.normalizedBid.bidId,
    bidDt: input.normalizedBid.bidDt,
    campaignName: input.normalizedBid.campaignName,
    campaignId: input.normalizedBid.campaignId,
    publisherName: input.normalizedBid.publisherName,
    publisherId: input.normalizedBid.publisherId,
    targetName: input.normalizedBid.targetName,
    targetId: input.normalizedBid.targetId,
    buyerName: input.normalizedBid.buyerName,
    buyerId: input.normalizedBid.buyerId,
    bidAmount: input.normalizedBid.bidAmount,
    winningBid: input.normalizedBid.winningBid,
    bidElapsedMs: input.normalizedBid.bidElapsedMs,
    isZeroBid: input.normalizedBid.isZeroBid,
    reasonForReject: input.normalizedBid.reasonForReject,
    httpStatusCode: input.normalizedBid.httpStatusCode,
    parsedErrorMessage: input.normalizedBid.errorMessage,
    primaryFailureStage: input.normalizedBid.primaryFailureStage,
    primaryTargetName: input.normalizedBid.primaryTargetName,
    primaryTargetId: input.normalizedBid.primaryTargetId,
    primaryBuyerName: input.normalizedBid.primaryBuyerName,
    primaryBuyerId: input.normalizedBid.primaryBuyerId,
    primaryErrorCode: input.normalizedBid.primaryErrorCode,
    primaryErrorMessage: input.normalizedBid.primaryErrorMessage,
    requestBody: input.normalizedBid.requestBody,
    responseBody: input.normalizedBid.responseBody,
    rawTraceJson: input.normalizedBid.rawTraceJson,
    outcome: input.normalizedBid.outcome,
    rootCause: input.diagnosis.rootCause,
    rootCauseConfidence: input.diagnosis.confidence,
    severity: input.diagnosis.severity,
    ownerType: input.diagnosis.ownerType,
    suggestedFix: input.diagnosis.suggestedFix,
    explanation: input.diagnosis.explanation,
    evidenceJson: input.diagnosis.evidence,
    fetchStatus: "fetched" as const,
    fetchedAt: now,
    lastError: null,
    leaseExpiresAt: null,
    importedAt: now,
    updatedAt: now,
    createdAt: existing?.createdAt ?? now,
  };

  if (existing) {
    db.update(bidInvestigations)
      .set(rowToPersist)
      .where(eq(bidInvestigations.id, existing.id))
      .run();
  } else {
    db.insert(bidInvestigations).values(rowToPersist).run();
  }

  db.delete(bidEvents)
    .where(eq(bidEvents.bidInvestigationId, investigationId))
    .run();
  db.delete(bidTargetAttempts)
    .where(eq(bidTargetAttempts.bidInvestigationId, investigationId))
    .run();

  if (input.normalizedBid.relevantEvents.length > 0) {
    db.insert(bidEvents)
      .values(
        input.normalizedBid.relevantEvents.map((event) => ({
          id: createId(),
          bidInvestigationId: investigationId,
          eventName: event.eventName,
          eventTimestamp: event.eventTimestamp,
          eventValsJson: event.eventValsJson,
          eventStrValsJson: event.eventStrValsJson,
          createdAt: now,
          updatedAt: now,
        })),
      )
      .run();
  }

  if (input.normalizedBid.targetAttempts.length > 0) {
    db.insert(bidTargetAttempts)
      .values(
        input.normalizedBid.targetAttempts.map((attempt) => ({
          id: createId(),
          bidInvestigationId: investigationId,
          sequence: attempt.sequence,
          eventName: attempt.eventName,
          eventTimestamp: attempt.eventTimestamp,
          targetName: attempt.targetName,
          targetId: attempt.targetId,
          targetBuyer: attempt.targetBuyer,
          targetBuyerId: attempt.targetBuyerId,
          targetNumber: attempt.targetNumber,
          targetGroupName: attempt.targetGroupName,
          targetGroupId: attempt.targetGroupId,
          targetSubId: attempt.targetSubId,
          targetBuyerSubId: attempt.targetBuyerSubId,
          requestUrl: attempt.requestUrl,
          httpMethod: attempt.httpMethod,
          requestStatus: attempt.requestStatus,
          httpStatusCode: attempt.httpStatusCode,
          durationMs: attempt.durationMs,
          routePriority: attempt.routePriority,
          routeWeight: attempt.routeWeight,
          accepted: attempt.accepted,
          winning: attempt.winning,
          bidAmount: attempt.bidAmount,
          minDurationSeconds: attempt.minDurationSeconds,
          rejectReason: attempt.rejectReason,
          errorCode: attempt.errorCode,
          errorMessage: attempt.errorMessage,
          errorsJson: attempt.errors,
          requestBody: attempt.requestBody,
          responseBody: attempt.responseBody,
          summaryReason: attempt.summaryReason,
          rawEventJson: attempt.rawEventJson,
          createdAt: now,
          updatedAt: now,
        })),
      )
      .run();
  }

  return getInvestigationByBidId(input.normalizedBid.bidId);
}

export async function claimInvestigationFetch(input: {
  bidId: string;
  importRunId: string | null;
  forceRefresh: boolean;
  leaseSeconds?: number;
}) {
  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const leaseExpiresAt = addSeconds(now, input.leaseSeconds ?? 120);
  const row = sqlite.transaction(() => {
    const existing = db
      .select()
      .from(bidInvestigations)
      .where(eq(bidInvestigations.bidId, input.bidId))
      .get() as BidInvestigationRow | undefined;

    if (!existing) {
      const created: typeof bidInvestigations.$inferInsert = {
        id: createId(),
        importRunId: input.importRunId,
        bidId: input.bidId,
        rawTraceJson: {},
        evidenceJson: [],
        fetchStatus: "pending",
        fetchStartedAt: now,
        refreshRequestedAt: input.forceRefresh ? now : null,
        leaseExpiresAt,
        fetchAttemptCount: 1,
        importedAt: now,
        createdAt: now,
        updatedAt: now,
      };
      db.insert(bidInvestigations).values(created).run();
      return {
        id: created.id,
        bidId: input.bidId,
        fetchStatus: "pending" as FetchStatus,
        shouldFetch: true,
        fetchedAt: null,
        lastError: null,
        fetchAttemptCount: 1,
        leaseExpiresAt,
      } satisfies ClaimBidInvestigationRow;
    }

    const activeLease = (() => {
      const leaseMs = toTimestamp(existing.leaseExpiresAt);
      const nowMs = toTimestamp(now);
      return leaseMs !== null && nowMs !== null && leaseMs > nowMs;
    })();
    const hasReusableFetch = existing.fetchStatus === "fetched" && Boolean(existing.fetchedAt);

    if (!input.forceRefresh && hasReusableFetch) {
      return {
        id: existing.id,
        bidId: existing.bidId,
        fetchStatus: existing.fetchStatus as FetchStatus,
        shouldFetch: false,
        fetchedAt: existing.fetchedAt,
        lastError: existing.lastError,
        fetchAttemptCount: existing.fetchAttemptCount,
        leaseExpiresAt: existing.leaseExpiresAt,
      } satisfies ClaimBidInvestigationRow;
    }

    if (!input.forceRefresh && existing.fetchStatus === "pending" && activeLease) {
      return {
        id: existing.id,
        bidId: existing.bidId,
        fetchStatus: existing.fetchStatus as FetchStatus,
        shouldFetch: false,
        fetchedAt: existing.fetchedAt,
        lastError: existing.lastError,
        fetchAttemptCount: existing.fetchAttemptCount,
        leaseExpiresAt: existing.leaseExpiresAt,
      } satisfies ClaimBidInvestigationRow;
    }

    db.update(bidInvestigations)
      .set({
        importRunId: input.importRunId,
        fetchStatus: "pending",
        fetchStartedAt: now,
        refreshRequestedAt: input.forceRefresh ? now : existing.refreshRequestedAt,
        leaseExpiresAt,
        lastError: null,
        updatedAt: now,
        fetchAttemptCount: existing.fetchAttemptCount + 1,
      })
      .where(eq(bidInvestigations.id, existing.id))
      .run();

    return {
      id: existing.id,
      bidId: existing.bidId,
      fetchStatus: "pending" as FetchStatus,
      shouldFetch: true,
      fetchedAt: existing.fetchedAt,
      lastError: null,
      fetchAttemptCount: existing.fetchAttemptCount + 1,
      leaseExpiresAt,
    } satisfies ClaimBidInvestigationRow;
  })();

  return {
    id: row.id,
    bidId: row.bidId,
    fetchStatus: row.fetchStatus,
    shouldFetch: row.shouldFetch,
    fetchedAt: row.fetchedAt,
    lastError: row.lastError,
    fetchAttemptCount: row.fetchAttemptCount,
    leaseExpiresAt: row.leaseExpiresAt,
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
  const db = getDb();
  const now = nowIso();
  const existing = await getBidInvestigationRowByBidId(input.bidId);
  const shouldPreserveExistingData = Boolean(existing?.fetchedAt);
  const updatePayload: Record<string, unknown> = {
    importRunId: input.importRunId,
    fetchStatus: "failed",
    parsedErrorMessage: input.errorMessage,
    lastError: input.errorMessage,
    primaryFailureStage: "fetch_failed",
    primaryErrorMessage: input.errorMessage,
    leaseExpiresAt: null,
    importedAt: now,
    updatedAt: now,
  };

  if (!shouldPreserveExistingData) {
    updatePayload.httpStatusCode = input.httpStatusCode ?? null;
    updatePayload.responseBody = input.responseBody ?? null;
    updatePayload.rawTraceJson = input.rawTraceJson ?? {};
    updatePayload.outcome = "unknown";
    updatePayload.rootCause = "unknown_needs_review";
    updatePayload.rootCauseConfidence = 0.2;
    updatePayload.severity = "high";
    updatePayload.ownerType = "system";
    updatePayload.suggestedFix =
      "Retry the fetch or inspect Ringba credentials, connectivity, and upstream API availability.";
    updatePayload.explanation =
      "The investigation could not complete because the Ringba bid detail fetch failed before normalization finished.";
    updatePayload.evidenceJson = [
      {
        field: "last_error",
        value: input.errorMessage,
        description: "Ringba fetch failure captured by the persistence layer.",
      },
    ] as DiagnosisResult["evidence"];
  }

  if (existing) {
    db.update(bidInvestigations)
      .set(updatePayload)
      .where(eq(bidInvestigations.id, existing.id))
      .run();
  } else {
    db.insert(bidInvestigations)
      .values({
        id: createId(),
        bidId: input.bidId,
        fetchStatus: "failed",
        parsedErrorMessage: input.errorMessage,
        lastError: input.errorMessage,
        importRunId: input.importRunId,
        httpStatusCode: input.httpStatusCode ?? null,
        responseBody: input.responseBody ?? null,
        rawTraceJson: input.rawTraceJson ?? {},
        primaryFailureStage: "fetch_failed",
        primaryErrorMessage: input.errorMessage,
        outcome: "unknown",
        rootCause: "unknown_needs_review",
        rootCauseConfidence: 0.2,
        severity: "high",
        ownerType: "system",
        suggestedFix:
          "Retry the fetch or inspect Ringba credentials, connectivity, and upstream API availability.",
        explanation:
          "The investigation could not complete because the Ringba bid detail fetch failed before normalization finished.",
        evidenceJson: [
          {
            field: "last_error",
            value: input.errorMessage,
            description: "Ringba fetch failure captured by the persistence layer.",
          },
        ],
        leaseExpiresAt: null,
        importedAt: now,
        createdAt: now,
        updatedAt: now,
      })
      .run();
  }

  return getInvestigationByBidId(input.bidId);
}

function sortInvestigations(rows: BidInvestigationRow[]) {
  return [...rows].sort((left, right) => {
    const leftPrimary = left.bidDt ?? left.importedAt;
    const rightPrimary = right.bidDt ?? right.importedAt;
    if (leftPrimary !== rightPrimary) {
      return rightPrimary.localeCompare(leftPrimary);
    }
    return right.importedAt.localeCompare(left.importedAt);
  });
}

function matchesSearch(row: BidInvestigationRow, search: string | undefined) {
  const sanitized = search ? sanitizeSearch(search).toLowerCase() : "";
  if (!sanitized) {
    return true;
  }

  const values = [
    row.bidId,
    row.campaignName,
    row.publisherName,
    row.targetName,
    row.primaryTargetName,
    row.primaryBuyerName,
    row.primaryErrorMessage,
  ];

  return values.some((value) => value?.toLowerCase().includes(sanitized));
}

async function listFilteredInvestigations(input: {
  rootCause?: string;
  ownerType?: string;
  search?: string;
}) {
  const db = getDb();
  const rows = db.select().from(bidInvestigations).all() as BidInvestigationRow[];

  return sortInvestigations(
    rows.filter((row) => {
      if (input.rootCause && row.rootCause !== input.rootCause) {
        return false;
      }

      if (input.ownerType && row.ownerType !== input.ownerType) {
        return false;
      }

      return matchesSearch(row, input.search);
    }),
  );
}

export async function getInvestigations(input: {
  page: number;
  pageSize: number;
  rootCause?: string;
  ownerType?: string;
  search?: string;
}): Promise<InvestigationsPageData> {
  const from = (input.page - 1) * input.pageSize;
  const rows = await listFilteredInvestigations(input);

  return {
    items: rows.slice(from, from + input.pageSize).map(toListItem),
    total: rows.length,
    page: input.page,
    pageSize: input.pageSize,
  };
}

export async function getInvestigationByBidId(bidId: string) {
  const db = getDb();
  const investigation = await getBidInvestigationRowByBidId(bidId);

  if (!investigation) {
    return null;
  }

  const eventData = db
    .select()
    .from(bidEvents)
    .where(eq(bidEvents.bidInvestigationId, investigation.id))
    .all() as BidEventRow[];
  const targetAttemptData = db
    .select()
    .from(bidTargetAttempts)
    .where(eq(bidTargetAttempts.bidInvestigationId, investigation.id))
    .all() as BidTargetAttemptRow[];

  eventData.sort((left, right) => {
    const leftPrimary = left.eventTimestamp ?? left.createdAt;
    const rightPrimary = right.eventTimestamp ?? right.createdAt;
    if (leftPrimary !== rightPrimary) {
      return leftPrimary.localeCompare(rightPrimary);
    }
    return left.createdAt.localeCompare(right.createdAt);
  });
  targetAttemptData.sort((left, right) => left.sequence - right.sequence);
  const sourceRow =
    investigation.importRunId
      ? await getImportSourceRowContextForBidId({
          importRunId: investigation.importRunId,
          bidId: investigation.bidId,
        })
      : null;
  const sourceContext: InvestigationSourceContext | null = sourceRow
    ? {
        fileName: sourceRow.fileName,
        rowNumber: sourceRow.rowNumber,
        bidDt: sourceRow.bidDt,
        bidAmount: sourceRow.bidAmount,
        reasonForReject: sourceRow.reasonForReject,
        rowJson: (sourceRow.rowJson ?? {}) as Record<string, unknown>,
      }
    : null;

  return toDetail(investigation, eventData, targetAttemptData, sourceContext);
}

export async function getDashboardStats(): Promise<DashboardStats> {
  const rows = (await listFilteredInvestigations({})).slice(0, 1000);
  const rootCauseCounts = new Map<string, number>();
  const campaignCounts = new Map<string, number>();
  const publisherCounts = new Map<string, number>();
  const targetCounts = new Map<string, number>();
  const outcomeCounts = new Map<string, number>();

  let acceptedCount = 0;
  let rejectedCount = 0;
  let zeroBidCount = 0;

  for (const row of rows) {
    incrementMetric(rootCauseCounts, row.rootCause);
    incrementMetric(campaignCounts, row.campaignName);
    incrementMetric(publisherCounts, row.publisherName);
    incrementMetric(targetCounts, row.targetName);
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
  return listFilteredInvestigations(input);
}

export async function getInvestigationListItemsByIds(ids: string[]) {
  const uniqueIds = Array.from(new Set(ids.filter(Boolean)));

  if (uniqueIds.length === 0) {
    return [];
  }

  const db = getDb();
  const rows: BidInvestigationRow[] = [];

  for (const chunk of splitIntoChunks(uniqueIds, SQLITE_IN_ARRAY_CHUNK_SIZE)) {
    const chunkRows = db
      .select()
      .from(bidInvestigations)
      .where(inArray(bidInvestigations.id, chunk))
      .all() as BidInvestigationRow[];
    rows.push(...chunkRows);
  }

  return rows.map(toListItem);
}
