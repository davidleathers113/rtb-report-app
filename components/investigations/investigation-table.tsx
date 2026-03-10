"use client";

import Link from "next/link";
import { Fragment, useState } from "react";
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";

import { JsonView } from "@/components/shared/json-view";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { InvestigationDetail, InvestigationListItem } from "@/types/bid";
import { formatCurrency, formatDateTime, toSentenceCase } from "@/lib/utils";

function severityVariant(severity: InvestigationListItem["severity"]) {
  if (severity === "critical" || severity === "high") {
    return "destructive";
  }

  if (severity === "medium") {
    return "warning";
  }

  return "success";
}

function ownerVariant(ownerType: InvestigationListItem["ownerType"]) {
  if (ownerType === "buyer") {
    return "info";
  }

  if (ownerType === "publisher") {
    return "warning";
  }

  if (ownerType === "ringba_config") {
    return "default";
  }

  return "destructive";
}

function fetchStatusVariant(fetchStatus: InvestigationListItem["fetchStatus"]) {
  if (fetchStatus === "fetched") {
    return "success";
  }

  if (fetchStatus === "failed") {
    return "destructive";
  }

  return "warning";
}

function failureStageVariant(stage: InvestigationListItem["primaryFailureStage"]) {
  if (stage === "accepted") {
    return "success";
  }

  if (stage === "zero_bid") {
    return "warning";
  }

  if (stage === "target_rejected" || stage === "fetch_failed") {
    return "destructive";
  }

  return "default";
}

const columns: ColumnDef<InvestigationListItem>[] = [
  {
    id: "expand",
    header: "",
    cell: () => null,
  },
  {
    accessorKey: "bidId",
    header: "Bid ID",
    cell: ({ row }) => (
      <Link
        href={`/investigations/${row.original.bidId}`}
        className="font-medium text-sky-700 hover:text-sky-800"
      >
        {row.original.bidId}
      </Link>
    ),
  },
  {
    accessorKey: "bidDt",
    header: "Timestamp",
    cell: ({ row }) => formatDateTime(row.original.bidDt),
  },
  {
    accessorKey: "fetchStatus",
    header: "Fetch",
    cell: ({ row }) => (
      <Badge variant={fetchStatusVariant(row.original.fetchStatus)}>
        {toSentenceCase(row.original.fetchStatus)}
      </Badge>
    ),
  },
  {
    accessorKey: "campaignName",
    header: "Campaign",
  },
  {
    accessorKey: "publisherName",
    header: "Publisher",
  },
  {
    accessorKey: "targetName",
    header: "Target",
  },
  {
    accessorKey: "primaryFailureStage",
    header: "Failure Stage",
    cell: ({ row }) => (
      <Badge variant={failureStageVariant(row.original.primaryFailureStage)}>
        {toSentenceCase(row.original.primaryFailureStage)}
      </Badge>
    ),
  },
  {
    accessorKey: "primaryTargetName",
    header: "Failing Target",
    cell: ({ row }) => row.original.primaryTargetName ?? row.original.targetName ?? "-",
  },
  {
    accessorKey: "bidAmount",
    header: "Bid Amount",
    cell: ({ row }) => formatCurrency(row.original.bidAmount),
  },
  {
    accessorKey: "httpStatusCode",
    header: "HTTP Status",
    cell: ({ row }) => row.original.httpStatusCode ?? "-",
  },
  {
    accessorKey: "rootCause",
    header: "Root Cause",
    cell: ({ row }) => (
      <Badge variant="default">{toSentenceCase(row.original.rootCause)}</Badge>
    ),
  },
  {
    accessorKey: "primaryErrorCode",
    header: "Error Code",
    cell: ({ row }) => row.original.primaryErrorCode ?? "-",
  },
  {
    accessorKey: "ownerType",
    header: "Owner",
    cell: ({ row }) => (
      <Badge variant={ownerVariant(row.original.ownerType)}>
        {toSentenceCase(row.original.ownerType)}
      </Badge>
    ),
  },
  {
    accessorKey: "severity",
    header: "Severity",
    cell: ({ row }) => (
      <Badge variant={severityVariant(row.original.severity)}>
        {toSentenceCase(row.original.severity)}
      </Badge>
    ),
  },
  {
    accessorKey: "primaryErrorMessage",
    header: "Error Message",
    cell: ({ row }) => (
      <div className="max-w-sm text-sm text-slate-600">
        {row.original.primaryErrorMessage ?? row.original.lastError ?? row.original.explanation}
      </div>
    ),
  },
];

export function InvestigationTable({
  items,
}: {
  items: InvestigationListItem[];
}) {
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
  const [detailsByBidId, setDetailsByBidId] = useState<
    Record<string, InvestigationDetail | null>
  >({});
  const [loadingBidId, setLoadingBidId] = useState<string | null>(null);

  const table = useReactTable({
    data: items,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  async function toggleRow(bidId: string) {
    const isExpanded = expandedRows[bidId] ?? false;

    setExpandedRows((current) => ({
      ...current,
      [bidId]: !isExpanded,
    }));

    if (isExpanded || detailsByBidId[bidId]) {
      return;
    }

    setLoadingBidId(bidId);

    try {
      const response = await fetch(`/api/investigations/${encodeURIComponent(bidId)}`);
      const payload = (await response.json()) as
        | InvestigationDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(
          errorPayload.error ?? "Unable to load bid detail preview.",
        );
      }

      setDetailsByBidId((current) => ({
        ...current,
        [bidId]: payload as InvestigationDetail,
      }));
    } catch {
      setDetailsByBidId((current) => ({
        ...current,
        [bidId]: null,
      }));
    } finally {
      setLoadingBidId((current) => (current === bidId ? null : current));
    }
  }

  return (
    <Table>
      <TableHeader>
        {table.getHeaderGroups().map((headerGroup) => (
          <TableRow key={headerGroup.id}>
            {headerGroup.headers.map((header) => (
              <TableHead key={header.id}>
                {header.isPlaceholder
                  ? null
                  : flexRender(
                      header.column.columnDef.header,
                      header.getContext(),
                    )}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.map((row) => {
          const bidId = row.original.bidId;
          const isExpanded = expandedRows[bidId] ?? false;
          const detail = detailsByBidId[bidId];

          return (
            <Fragment key={row.id}>
              <TableRow key={row.id}>
                <TableCell className="w-12">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => toggleRow(bidId)}
                  >
                    {isExpanded ? "Hide" : "View"}
                  </Button>
                </TableCell>
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
              {isExpanded ? (
                <TableRow key={`${row.id}-detail`} className="bg-slate-50/80 hover:bg-slate-50/80">
                  <TableCell colSpan={columns.length + 1}>
                    {loadingBidId === bidId ? (
                      <div className="rounded-lg border border-dashed border-slate-200 bg-white p-4 text-sm text-slate-500">
                        Loading debug preview...
                      </div>
                    ) : detail ? (
                      <div className="space-y-4">
                        <div className="flex flex-wrap items-center gap-2">
                          <Badge variant={fetchStatusVariant(detail.fetchStatus)}>
                            {toSentenceCase(detail.fetchStatus)}
                          </Badge>
                          <Badge variant={failureStageVariant(detail.primaryFailureStage)}>
                            {toSentenceCase(detail.primaryFailureStage)}
                          </Badge>
                          {detail.primaryErrorCode !== null ? (
                            <Badge variant="destructive">Code {detail.primaryErrorCode}</Badge>
                          ) : null}
                          {detail.fetchedAt ? (
                            <span className="text-sm text-slate-500">
                              Fetched {formatDateTime(detail.fetchedAt)}
                            </span>
                          ) : null}
                          {detail.lastError ? (
                            <span className="text-sm text-rose-600">
                              {detail.lastError}
                            </span>
                          ) : null}
                        </div>
                        {detail.primaryTargetName || detail.primaryErrorMessage ? (
                          <div className="rounded-lg border border-slate-200 bg-white p-4 text-sm text-slate-700">
                            <span className="font-medium text-slate-900">Primary failure:</span>{" "}
                            {detail.primaryTargetName ?? detail.targetName ?? "Unknown target"} -{" "}
                            {detail.primaryErrorMessage ??
                              detail.reasonForReject ??
                              detail.explanation}
                          </div>
                        ) : null}
                        <div className="grid gap-4 lg:grid-cols-2">
                        <div className="space-y-2">
                          <p className="text-sm font-semibold text-slate-900">
                            Request Body
                          </p>
                          <JsonView value={detail.requestBody} />
                        </div>
                        <div className="space-y-2">
                          <p className="text-sm font-semibold text-slate-900">
                            Response Body
                          </p>
                          <JsonView value={detail.responseBody} />
                        </div>
                        <div className="space-y-2">
                          <p className="text-sm font-semibold text-slate-900">Evidence</p>
                          <div className="space-y-2 rounded-lg border border-slate-200 bg-white p-4">
                            {detail.evidence.map((item, index) => (
                              <div
                                key={`${item.field}-${index}`}
                                className="text-sm text-slate-600"
                              >
                                <span className="font-medium text-slate-900">
                                  {item.field}:
                                </span>{" "}
                                {String(item.value ?? "-")} - {item.description}
                              </div>
                            ))}
                          </div>
                        </div>
                        <div className="space-y-2">
                          <p className="text-sm font-semibold text-slate-900">
                            Raw Trace Preview
                          </p>
                          <JsonView value={detail.rawTraceJson} />
                        </div>
                      </div>
                      </div>
                    ) : (
                      <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-700">
                        Unable to load the expanded preview for this row.
                      </div>
                    )}
                  </TableCell>
                </TableRow>
              ) : null}
            </Fragment>
          );
        })}
      </TableBody>
    </Table>
  );
}
