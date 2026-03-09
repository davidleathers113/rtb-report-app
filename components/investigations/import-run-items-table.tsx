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
import type { ImportRunDetail } from "@/types/import-run";
import { formatDateTime, toSentenceCase } from "@/lib/utils";

function itemStatusVariant(status: ImportRunDetail["items"][number]["status"]) {
  if (status === "completed") {
    return "success";
  }

  if (status === "failed") {
    return "destructive";
  }

  return "warning";
}

function resolutionVariant(
  resolution: ImportRunDetail["items"][number]["resolution"],
) {
  if (resolution === "reused") {
    return "info";
  }

  if (resolution === "fetched") {
    return "success";
  }

  if (resolution === "failed") {
    return "destructive";
  }

  return "default";
}

export function ImportRunItemsTable({
  run,
}: {
  run: ImportRunDetail;
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Position</TableHead>
          <TableHead>Bid ID</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Resolution</TableHead>
          <TableHead>Attempts</TableHead>
          <TableHead>Finished</TableHead>
          <TableHead>Result</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {run.items.map((item) => (
          <TableRow key={item.id}>
            <TableCell>{item.position}</TableCell>
            <TableCell className="font-medium text-slate-900">{item.bidId}</TableCell>
            <TableCell>
              <Badge variant={itemStatusVariant(item.status)}>
                {toSentenceCase(item.status)}
              </Badge>
            </TableCell>
            <TableCell>
              {item.resolution ? (
                <Badge variant={resolutionVariant(item.resolution)}>
                  {toSentenceCase(item.resolution)}
                </Badge>
              ) : (
                "-"
              )}
            </TableCell>
            <TableCell>{item.attemptCount}</TableCell>
            <TableCell>{formatDateTime(item.completedAt)}</TableCell>
            <TableCell>
              {item.investigation ? (
                <Link
                  href={`/investigations/${item.investigation.bidId}`}
                  className="text-sky-700 hover:text-sky-800"
                >
                  View detail
                </Link>
              ) : item.errorMessage ? (
                <span className="text-sm text-rose-600">{item.errorMessage}</span>
              ) : (
                <span className="text-sm text-slate-500">Pending</span>
              )}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
