"use client";

import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatDateTime, toSentenceCase } from "@/lib/utils";
import type { ImportRunDetail } from "@/types/import-run";

export type ImportRunListItem = Pick<
  ImportRunDetail,
  | "id"
  | "sourceType"
  | "status"
  | "isStalled"
  | "percentComplete"
  | "totalItems"
  | "completedCount"
  | "failedCount"
  | "notes"
  | "createdAt"
>;

function runStatusVariant(status: ImportRunDetail["status"]) {
  if (status === "completed") return "success";
  if (status === "completed_with_errors") return "warning";
  if (status === "failed" || status === "cancelled") return "destructive";
  if (status === "running") return "info";
  return "default";
}

export function ImportRunList(input: {
  items: ImportRunListItem[];
  emptyMessage?: string;
  onResume?: (importRunId: string) => void;
  resumingRunIds?: string[];
}) {
  const resumingRunIdSet = new Set(input.resumingRunIds ?? []);

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Created At</TableHead>
          <TableHead>Source</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Progress</TableHead>
          <TableHead>Total</TableHead>
          <TableHead>Completed</TableHead>
          <TableHead>Failed</TableHead>
          <TableHead>Notes</TableHead>
          <TableHead>Action</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {input.items.length === 0 ? (
          <TableRow>
            <TableCell colSpan={9} className="text-center py-8 text-slate-500">
              {input.emptyMessage ?? "No import runs found."}
            </TableCell>
          </TableRow>
        ) : (
          input.items.map((run) => (
            <TableRow key={run.id}>
              <TableCell className="whitespace-nowrap">
                {formatDateTime(run.createdAt)}
              </TableCell>
              <TableCell>
                <Badge variant="info">{toSentenceCase(run.sourceType)}</Badge>
              </TableCell>
              <TableCell>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={runStatusVariant(run.status)}>
                    {toSentenceCase(run.status)}
                  </Badge>
                  {run.isStalled ? <Badge variant="warning">Stalled</Badge> : null}
                </div>
              </TableCell>
              <TableCell>
                <div className="flex items-center gap-2">
                  <div className="w-16 bg-slate-100 rounded-full h-1.5 overflow-hidden">
                    <div 
                      className="bg-sky-600 h-full transition-all duration-500" 
                      style={{ width: `${run.percentComplete}%` }}
                    />
                  </div>
                  <span className="text-xs font-medium text-slate-600">
                    {run.percentComplete}%
                  </span>
                </div>
              </TableCell>
              <TableCell>{run.totalItems}</TableCell>
              <TableCell className="text-emerald-600 font-medium">
                {run.completedCount}
              </TableCell>
              <TableCell className={run.failedCount > 0 ? "text-rose-600 font-medium" : "text-slate-400"}>
                {run.failedCount}
              </TableCell>
              <TableCell className="max-w-[200px] truncate text-slate-500 italic">
                {run.notes || "-"}
              </TableCell>
              <TableCell>
                <div className="flex flex-wrap items-center gap-3">
                  {input.onResume && run.sourceType === "csv_direct_import" && run.isStalled ? (
                    <button
                      type="button"
                      className="font-medium text-sm text-emerald-700 hover:text-emerald-800 disabled:cursor-not-allowed disabled:text-slate-400"
                      onClick={() => input.onResume?.(run.id)}
                      disabled={resumingRunIdSet.has(run.id)}
                    >
                      {resumingRunIdSet.has(run.id) ? "Resuming..." : "Resume"}
                    </button>
                  ) : null}
                  <Link
                    href={`/import-runs/${run.id}`}
                    className="text-sky-700 hover:text-sky-800 font-medium text-sm"
                  >
                    View details
                  </Link>
                </div>
              </TableCell>
            </TableRow>
          ))
        )}
      </TableBody>
    </Table>
  );
}
