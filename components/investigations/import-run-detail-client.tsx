"use client";

import { useEffect, useState, useMemo } from "react";
import { ImportRunItemsTable } from "@/components/investigations/import-run-items-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { toSentenceCase, formatDateTime } from "@/lib/utils";
import type { ImportRunDetail } from "@/types/import-run";
import Link from "next/link";
import { ArrowLeft, RefreshCw, AlertCircle } from "lucide-react";

export function ImportRunDetailClient({
  initialRun,
}: {
  initialRun: ImportRunDetail;
}) {
  const [run, setRun] = useState<ImportRunDetail>(initialRun);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const isTerminal = ["completed", "completed_with_errors", "failed", "cancelled"].includes(
    run.status
  );

  async function refresh() {
    setIsRefreshing(true);
    setError(null);
    try {
      const response = await fetch(`/api/import-runs/${run.id}`);
      if (!response.ok) throw new Error("Failed to refresh run details.");
      const data = await response.json();
      setRun(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred.");
    } finally {
      setIsRefreshing(false);
    }
  }

  // Poll if not terminal
  useEffect(() => {
    if (isTerminal) return;

    const interval = setInterval(refresh, 3000);
    return () => clearInterval(interval);
  }, [isTerminal, run.id]);

  const stats = useMemo(() => {
    return [
      { label: "Total Items", value: run.totalItems, color: "text-slate-900" },
      { label: "Queued", value: run.queuedCount, color: "text-amber-600" },
      { label: "Running", value: run.runningCount, color: "text-sky-600" },
      { label: "Completed", value: run.completedCount, color: "text-emerald-600" },
      { label: "Failed", value: run.failedCount, color: "text-rose-600" },
    ];
  }, [run]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <Button variant="ghost" asChild className="-ml-4 text-slate-500">
          <Link href="/import-runs">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to History
          </Link>
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={refresh}
          disabled={isRefreshing}
        >
          <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      <div className="grid gap-6 md:grid-cols-3">
        <Card className="md:col-span-2">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="text-xl">Import Run Details</CardTitle>
                <CardDescription className="font-mono text-xs mt-1">
                  ID: {run.id}
                </CardDescription>
              </div>
              <Badge
                variant={
                  run.status === "completed"
                    ? "success"
                    : run.status === "running"
                    ? "info"
                    : run.status === "failed"
                    ? "destructive"
                    : "default"
                }
                className="text-sm px-3 py-1"
              >
                {toSentenceCase(run.status)}
              </Badge>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p className="text-slate-500 font-medium">Source Type</p>
                <p className="mt-1">{toSentenceCase(run.sourceType)}</p>
              </div>
              <div>
                <p className="text-slate-500 font-medium">Created At</p>
                <p className="mt-1">{formatDateTime(run.createdAt)}</p>
              </div>
              <div>
                <p className="text-slate-500 font-medium">Trigger</p>
                <p className="mt-1">{toSentenceCase(run.triggerType)}</p>
              </div>
              <div>
                <p className="text-slate-500 font-medium">Last Updated</p>
                <p className="mt-1">{formatDateTime(run.updatedAt)}</p>
              </div>
            </div>

            {run.notes && (
              <div className="pt-2 border-t border-slate-100">
                <p className="text-sm text-slate-500 font-medium">Notes</p>
                <p className="mt-1 text-sm text-slate-700 italic">{run.notes}</p>
              </div>
            )}

            {run.lastError && (
              <div className="mt-4 p-3 rounded-lg bg-rose-50 border border-rose-200 flex gap-3 text-rose-800">
                <AlertCircle className="h-5 w-5 shrink-0" />
                <div className="text-sm">
                  <p className="font-semibold">Last Error</p>
                  <p className="mt-0.5">{run.lastError}</p>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg text-slate-800">Progress Summary</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="space-y-2">
              <div className="flex justify-between text-sm font-medium">
                <span>{run.percentComplete}% Complete</span>
                <span>{run.completedCount + run.failedCount} / {run.totalItems}</span>
              </div>
              <div className="w-full bg-slate-100 rounded-full h-3 overflow-hidden">
                <div
                  className={`h-full transition-all duration-1000 ${
                    run.status === "failed" ? "bg-rose-500" : "bg-sky-600"
                  }`}
                  style={{ width: `${run.percentComplete}%` }}
                />
              </div>
            </div>

            <div className="divide-y divide-slate-100 border-t border-slate-100">
              {stats.map((stat) => (
                <div key={stat.label} className="flex justify-between py-2 text-sm">
                  <span className="text-slate-500 font-medium">{stat.label}</span>
                  <span className={`font-bold ${stat.color}`}>{stat.value}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Items in this Run</CardTitle>
          <CardDescription>
            Sample of items currently tracked in this batch.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ImportRunItemsTable run={run} />
        </CardContent>
      </Card>
    </div>
  );
}
