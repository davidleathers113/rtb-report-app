import "server-only";

import { and, asc, desc, eq, gte, lte, sql } from "drizzle-orm";

import { getDb, getSqlite } from "@/lib/db/client";
import {
  importSourceFiles,
  importRunItems,
  importSourceRows,
  type ImportSourceFileRow,
  type ImportSourceRow,
} from "@/lib/db/schema";
import { createId, nowIso } from "@/lib/db/utils";

const ROW_CHUNK_SIZE = 1000;

function splitIntoChunks<T>(values: T[], chunkSize: number) {
  const chunks: T[][] = [];

  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }

  return chunks;
}

export async function createImportSourceFile(input: {
  importRunId: string;
  sourceType: string;
  fileName: string;
  rowCount: number;
  headerJson: string[];
  sourceMetadata?: Record<string, unknown>;
}): Promise<ImportSourceFileRow> {
  const db = getDb();
  const now = nowIso();
  const id = createId();

  db.insert(importSourceFiles)
    .values({
      id,
      importRunId: input.importRunId,
      sourceType: input.sourceType,
      fileName: input.fileName,
      rowCount: input.rowCount,
      headerJson: input.headerJson,
      sourceMetadata: input.sourceMetadata ?? {},
      createdAt: now,
      updatedAt: now,
    })
    .run();

  return {
    id,
    importRunId: input.importRunId,
    sourceType: input.sourceType,
    fileName: input.fileName,
    rowCount: input.rowCount,
    headerJson: input.headerJson,
    sourceMetadata: input.sourceMetadata ?? {},
    createdAt: now,
    updatedAt: now,
  };
}

export async function updateImportSourceFile(input: {
  id: string;
  rowCount: number;
  headerJson: string[];
  sourceMetadata?: Record<string, unknown>;
}) {
  const db = getDb();
  const now = nowIso();

  db.update(importSourceFiles)
    .set({
      rowCount: input.rowCount,
      headerJson: input.headerJson,
      sourceMetadata: input.sourceMetadata ?? {},
      updatedAt: now,
    })
    .where(eq(importSourceFiles.id, input.id))
    .run();
}

export async function insertImportSourceRows(input: {
  importRunId: string;
  importSourceFileId: string;
  rows: Array<
    Omit<
      ImportSourceRow,
      | "id"
      | "importSourceFileId"
      | "importRunId"
      | "createdAt"
      | "updatedAt"
    >
  >;
}) {
  if (input.rows.length === 0) {
    return;
  }

  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const chunks = splitIntoChunks(input.rows, ROW_CHUNK_SIZE);

  sqlite.transaction(() => {
    for (const chunk of chunks) {
      db.insert(importSourceRows)
        .values(
          chunk.map((row) => ({
            id: createId(),
            importSourceFileId: input.importSourceFileId,
            importRunId: input.importRunId,
            rowNumber: row.rowNumber,
            bidId: row.bidId ?? null,
            bidDt: row.bidDt ?? null,
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
            rowJson: row.rowJson ?? {},
            createdAt: now,
            updatedAt: now,
          })),
        )
        .run();
    }
  })();
}

export async function insertImportSourceRowsWithRunItemsBatch(input: {
  importRunId: string;
  importSourceFileId: string;
  rows: Array<
    Omit<
      ImportSourceRow,
      | "id"
      | "importSourceFileId"
      | "importRunId"
      | "createdAt"
      | "updatedAt"
    >
  >;
  bidIds: string[];
  startPosition: number;
}) {
  if (input.rows.length === 0 && input.bidIds.length === 0) {
    return {
      insertedRowCount: 0,
      insertedBidIdCount: 0,
      nextPosition: input.startPosition,
    };
  }

  const db = getDb();
  const sqlite = getSqlite();
  const now = nowIso();
  const rowChunks = splitIntoChunks(input.rows, ROW_CHUNK_SIZE);
  const bidIdChunks = splitIntoChunks(input.bidIds, ROW_CHUNK_SIZE);
  let nextPosition = input.startPosition;

  sqlite.transaction(() => {
    for (const chunk of rowChunks) {
      db.insert(importSourceRows)
        .values(
          chunk.map((row) => ({
            id: createId(),
            importSourceFileId: input.importSourceFileId,
            importRunId: input.importRunId,
            rowNumber: row.rowNumber,
            bidId: row.bidId ?? null,
            bidDt: row.bidDt ?? null,
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
            rowJson: row.rowJson ?? {},
            createdAt: now,
            updatedAt: now,
          })),
        )
        .run();
    }

    for (const chunk of bidIdChunks) {
      db.insert(importRunItems)
        .values(
          chunk.map((bidId) => {
            const position = nextPosition;
            nextPosition += 1;

            return {
              id: createId(),
              importRunId: input.importRunId,
              bidId,
              position,
              status: "queued" as const,
              createdAt: now,
              updatedAt: now,
            };
          }),
        )
        .run();
    }
  })();

  return {
    insertedRowCount: input.rows.length,
    insertedBidIdCount: input.bidIds.length,
    nextPosition,
  };
}

export async function getImportSourceRowForBidId(input: {
  importRunId: string;
  bidId: string;
}): Promise<ImportSourceRow | null> {
  const db = getDb();
  const row = db
    .select()
    .from(importSourceRows)
    .where(
      and(eq(importSourceRows.importRunId, input.importRunId), eq(importSourceRows.bidId, input.bidId)),
    )
    .orderBy(asc(importSourceRows.rowNumber))
    .get() as ImportSourceRow | undefined;

  return row ?? null;
}

export interface ImportSourceRowContext {
  fileName: string;
  rowNumber: number;
  bidId: string | null;
  bidDt: string | null;
  bidAmount: number | null;
  reasonForReject: string | null;
  rowJson: Record<string, unknown>;
}

export async function getImportSourceRowContextForBidId(input: {
  importRunId: string;
  bidId: string;
}): Promise<ImportSourceRowContext | null> {
  const db = getDb();
  const row = db
    .select({
      fileName: importSourceFiles.fileName,
      rowNumber: importSourceRows.rowNumber,
      bidId: importSourceRows.bidId,
      bidDt: importSourceRows.bidDt,
      bidAmount: importSourceRows.bidAmount,
      reasonForReject: importSourceRows.reasonForReject,
      rowJson: importSourceRows.rowJson,
    })
    .from(importSourceRows)
    .leftJoin(importSourceFiles, eq(importSourceRows.importSourceFileId, importSourceFiles.id))
    .where(
      and(eq(importSourceRows.importRunId, input.importRunId), eq(importSourceRows.bidId, input.bidId)),
    )
    .orderBy(asc(importSourceRows.rowNumber))
    .get() as ImportSourceRowContext | undefined;

  return row ?? null;
}

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
  rowJson: Record<string, unknown>;
}

export async function listImportSourceFiles(): Promise<ImportSourceFileSummary[]> {
  const db = getDb();
  const rows = db
    .select({
      id: importSourceFiles.id,
      importRunId: importSourceFiles.importRunId,
      fileName: importSourceFiles.fileName,
      rowCount: importSourceFiles.rowCount,
      createdAt: importSourceFiles.createdAt,
    })
    .from(importSourceFiles)
    .orderBy(desc(importSourceFiles.createdAt))
    .all() as ImportSourceFileSummary[];

  const seen = new Set<string>();
  const result: ImportSourceFileSummary[] = [];

  for (const row of rows) {
    if (seen.has(row.fileName)) {
      continue;
    }
    seen.add(row.fileName);
    result.push(row);
  }

  return result;
}

export async function listImportSourceRows(input: {
  fileName?: string;
  bidId?: string;
  startBidDt?: string;
  endBidDt?: string;
  limit: number;
  offset: number;
}) {
  const db = getDb();
  const filters = [];

  if (input.fileName) {
    filters.push(eq(importSourceFiles.fileName, input.fileName));
  }

  if (input.bidId) {
    filters.push(eq(importSourceRows.bidId, input.bidId));
  }

  if (input.startBidDt) {
    filters.push(gte(importSourceRows.bidDt, input.startBidDt));
  }

  if (input.endBidDt) {
    filters.push(lte(importSourceRows.bidDt, input.endBidDt));
  }

  const whereClause = filters.length > 0 ? and(...filters) : undefined;

  const rows = db
    .select({
      id: importSourceRows.id,
      importRunId: importSourceRows.importRunId,
      importSourceFileId: importSourceRows.importSourceFileId,
      fileName: importSourceFiles.fileName,
      rowNumber: importSourceRows.rowNumber,
      bidId: importSourceRows.bidId,
      bidDt: importSourceRows.bidDt,
      campaignName: importSourceRows.campaignName,
      publisherName: importSourceRows.publisherName,
      bidAmount: importSourceRows.bidAmount,
      reasonForReject: importSourceRows.reasonForReject,
      rowJson: importSourceRows.rowJson,
    })
    .from(importSourceRows)
    .leftJoin(importSourceFiles, eq(importSourceRows.importSourceFileId, importSourceFiles.id))
    .where(whereClause)
    .orderBy(desc(importSourceRows.bidDt), asc(importSourceRows.rowNumber))
    .limit(input.limit)
    .offset(input.offset)
    .all() as ImportSourceRowListItem[];

  const countRow = db
    .select({
      count: sql<number>`count(*)`,
    })
    .from(importSourceRows)
    .leftJoin(importSourceFiles, eq(importSourceRows.importSourceFileId, importSourceFiles.id))
    .where(whereClause)
    .get() as { count: number } | undefined;

  return {
    items: rows.map((row) => ({
      ...row,
      rowJson: (row.rowJson ?? {}) as Record<string, unknown>,
    })),
    total: countRow?.count ?? 0,
  };
}
