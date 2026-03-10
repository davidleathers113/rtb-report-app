import "server-only";

import { count } from "drizzle-orm";

import { getDb } from "@/lib/db/client";
import { importOpsEvents, type ImportOpsEventRow } from "@/lib/db/schema";
import { and, createId, desc, eq } from "@/lib/db/utils";
import type {
  ImportOpsEvent,
  ImportOpsEventPage,
  ImportOpsEventSeverity,
  ImportOpsEventSource,
  ImportOpsEventType,
} from "@/types/ops-event";

function mapOpsEvent(row: ImportOpsEventRow): ImportOpsEvent {
  return {
    id: row.id,
    eventType: row.eventType as ImportOpsEventType,
    severity: row.severity as ImportOpsEventSeverity,
    source: row.source as ImportOpsEventSource,
    scheduleId: row.scheduleId,
    importRunId: row.importRunId,
    message: row.message,
    metadataJson: (row.metadataJson ?? {}) as Record<string, unknown>,
    createdAt: row.createdAt,
  };
}

export async function createImportOpsEvent(input: {
  eventType: ImportOpsEventType;
  severity: ImportOpsEventSeverity;
  source: ImportOpsEventSource;
  scheduleId?: string | null;
  importRunId?: string | null;
  message: string;
  metadataJson?: Record<string, unknown>;
}) {
  const db = getDb();

  db.insert(importOpsEvents)
    .values({
      id: createId(),
      eventType: input.eventType,
      severity: input.severity,
      source: input.source,
      scheduleId: input.scheduleId ?? null,
      importRunId: input.importRunId ?? null,
      message: input.message,
      metadataJson: input.metadataJson ?? {},
    })
    .run();
}

export async function listImportOpsEvents(input?: {
  scheduleId?: string;
  importRunId?: string;
  limit?: number;
  offset?: number;
  eventType?: ImportOpsEventType | "all";
  severity?: ImportOpsEventSeverity | "all";
}): Promise<ImportOpsEventPage> {
  const db = getDb();
  const limit = Math.max(1, Math.min(input?.limit ?? 10, 100));
  const offset = Math.max(0, input?.offset ?? 0);
  const filters = [];

  if (input?.scheduleId) {
    filters.push(eq(importOpsEvents.scheduleId, input.scheduleId));
  }

  if (input?.importRunId) {
    filters.push(eq(importOpsEvents.importRunId, input.importRunId));
  }

  if (input?.eventType && input.eventType !== "all") {
    filters.push(eq(importOpsEvents.eventType, input.eventType));
  }

  if (input?.severity && input.severity !== "all") {
    filters.push(eq(importOpsEvents.severity, input.severity));
  }

  const whereClause = filters.length > 0 ? and(...filters) : undefined;
  const rows = db
    .select()
    .from(importOpsEvents)
    .where(whereClause)
    .orderBy(desc(importOpsEvents.createdAt))
    .limit(limit)
    .offset(offset)
    .all() as ImportOpsEventRow[];
  const total = db
    .select({ value: count() })
    .from(importOpsEvents)
    .where(whereClause)
    .get()?.value;

  return {
    items: rows.map((row) => mapOpsEvent(row)),
    total: total ?? 0,
    limit,
    offset,
    eventType: input?.eventType ?? "all",
    severity: input?.severity ?? "all",
  };
}
