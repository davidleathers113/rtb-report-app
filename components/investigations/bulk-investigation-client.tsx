"use client";

import { useEffect, useMemo, useState } from "react";

import { MAX_CSV_BID_IDS } from "@/lib/import-runs/csv-constants";
import { ImportRunItemsTable } from "@/components/investigations/import-run-items-table";
import { InvestigationTable } from "@/components/investigations/investigation-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";
import { parseBidIds } from "@/lib/utils/bid-input";
import type { InvestigationListItem } from "@/types/bid";
import type { CsvPreviewResult, ImportRunDetail } from "@/types/import-run";

const terminalStatuses = new Set([
  "completed",
  "completed_with_errors",
  "failed",
  "cancelled",
]);

function progressTone(percentComplete: number) {
  if (percentComplete >= 100) {
    return "bg-emerald-500";
  }

  if (percentComplete >= 50) {
    return "bg-sky-500";
  }

  return "bg-amber-500";
}

export function BulkInvestigationClient() {
  const [rawBidIds, setRawBidIds] = useState("");
  const [forceRefresh, setForceRefresh] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isPreviewingCsv, setIsPreviewingCsv] = useState(false);
  const [isSubmittingCsv, setIsSubmittingCsv] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [activeRun, setActiveRun] = useState<ImportRunDetail | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvPreview, setCsvPreview] = useState<CsvPreviewResult | null>(null);
  const [selectedCsvColumnKey, setSelectedCsvColumnKey] = useState<string>("");

  const parsedCount = useMemo(() => parseBidIds(rawBidIds).length, [rawBidIds]);
  const isTerminal = activeRun ? terminalStatuses.has(activeRun.status) : false;
  const activeRunId = activeRun?.id ?? null;
  const processedInvestigations = useMemo(() => {
    if (!activeRun) {
      return [] as InvestigationListItem[];
    }

    return activeRun.items
      .map((item) => item.investigation)
      .filter((investigation): investigation is InvestigationListItem =>
        Boolean(investigation),
      );
  }, [activeRun]);

  useEffect(() => {
    if (!activeRunId || isTerminal) {
      return;
    }

    const importRunId = activeRunId;
    let isCancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    async function tick() {
      if (isCancelled) {
        return;
      }

      setIsProcessing(true);

      try {
        const response = await fetch(
          `/api/import-runs/${encodeURIComponent(importRunId)}/process`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              batchSize: 10,
              maxBatches: 2,
            }),
          },
        );

        const payload = (await response.json()) as
          | ImportRunDetail
          | { error?: string };

        if (!response.ok) {
          const errorPayload = payload as { error?: string };
          throw new Error(errorPayload.error ?? "Unable to process import run.");
        }

        if (!isCancelled) {
          setActiveRun(payload as ImportRunDetail);
        }
      } catch (error) {
        if (!isCancelled) {
          setErrorMessage(
            error instanceof Error
              ? error.message
              : "Unexpected import run processing error.",
          );
        }
      } finally {
        if (!isCancelled) {
          setIsProcessing(false);
          timeoutId = setTimeout(tick, 1500);
        }
      }
    }

    void tick();

    return () => {
      isCancelled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [activeRunId, isTerminal]);

  async function handleSubmit() {
    setIsSubmitting(true);
    setErrorMessage(null);

    try {
      const bidIds = parseBidIds(rawBidIds);

      if (bidIds.length === 0) {
        throw new Error("Enter at least one Bid ID before investigating.");
      }

      const response = await fetch("/api/investigations/bulk", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ bidIds, forceRefresh }),
      });

      const payload = (await response.json()) as
        | ImportRunDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to investigate bids.");
      }

      setActiveRun(payload as ImportRunDetail);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected investigation error.",
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  async function previewCsv(file: File, selectedColumnKey?: string) {
    setIsPreviewingCsv(true);
    setErrorMessage(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      if (selectedColumnKey) {
        formData.append("selectedColumnKey", selectedColumnKey);
      }

      const response = await fetch("/api/import-runs/csv/preview", {
        method: "POST",
        body: formData,
      });
      const payload = (await response.json()) as
        | CsvPreviewResult
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to preview CSV upload.");
      }

      const preview = payload as CsvPreviewResult;
      setCsvPreview(preview);
      setSelectedCsvColumnKey(preview.selectedColumnKey);
    } catch (error) {
      setCsvPreview(null);
      setSelectedCsvColumnKey("");
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected CSV preview error.",
      );
    } finally {
      setIsPreviewingCsv(false);
    }
  }

  async function handleCsvFileChange(file: File | null) {
    setCsvFile(file);
    setCsvPreview(null);
    setSelectedCsvColumnKey("");

    if (!file) {
      return;
    }

    await previewCsv(file);
  }

  async function handleCsvColumnChange(nextColumnKey: string) {
    setSelectedCsvColumnKey(nextColumnKey);

    if (!csvFile) {
      return;
    }

    await previewCsv(csvFile, nextColumnKey);
  }

  async function handleSubmitCsv() {
    if (!csvFile) {
      setErrorMessage("Upload and preview a CSV file before creating an import run.");
      return;
    }

    setIsSubmittingCsv(true);
    setErrorMessage(null);

    try {
      const formData = new FormData();
      formData.append("file", csvFile);
      formData.append("forceRefresh", String(forceRefresh));

      if (selectedCsvColumnKey) {
        formData.append("selectedColumnKey", selectedCsvColumnKey);
      }

      const response = await fetch("/api/import-runs/csv", {
        method: "POST",
        body: formData,
      });
      const payload = (await response.json()) as
        | ImportRunDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to create import run from CSV.");
      }

      setActiveRun(payload as ImportRunDetail);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected CSV import error.",
      );
    } finally {
      setIsSubmittingCsv(false);
    }
  }

  async function handleRetryFailed() {
    if (!activeRun) {
      return;
    }

    setErrorMessage(null);

    try {
      const response = await fetch(
        `/api/import-runs/${encodeURIComponent(activeRun.id)}/retry-failed`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ forceRefresh }),
        },
      );
      const payload = (await response.json()) as
        | ImportRunDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to retry failed items.");
      }

      setActiveRun(payload as ImportRunDetail);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected retry error.",
      );
    }
  }

  async function handleRerun() {
    if (!activeRun) {
      return;
    }

    setErrorMessage(null);

    try {
      const response = await fetch(
        `/api/import-runs/${encodeURIComponent(activeRun.id)}/rerun`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ forceRefresh }),
        },
      );
      const payload = (await response.json()) as
        | ImportRunDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to rerun import.");
      }

      setActiveRun(payload as ImportRunDetail);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected rerun error.",
      );
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Bulk Investigation</CardTitle>
          <CardDescription>
            Paste one Bid ID or many Bid IDs. Commas, spaces, and new lines are all
            accepted. Duplicates are removed before processing.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Textarea
            value={rawBidIds}
            onChange={(event) => setRawBidIds(event.target.value)}
            placeholder={"12345\n67890\nabc-bid-id"}
          />
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-sm text-slate-500">
                <span>Ready to process</span>
                <Badge variant="info">{parsedCount}</Badge>
              </div>
              <label className="flex items-center gap-2 text-sm text-slate-600">
                <input
                  type="checkbox"
                  checked={forceRefresh}
                  onChange={(event) => setForceRefresh(event.target.checked)}
                  className="h-4 w-4 rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                />
                Force refresh existing stored bids
              </label>
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                onClick={() => {
                  setRawBidIds("");
                  setForceRefresh(false);
                  setActiveRun(null);
                  setCsvFile(null);
                  setCsvPreview(null);
                  setSelectedCsvColumnKey("");
                  setErrorMessage(null);
                }}
              >
                Clear
              </Button>
              <Button onClick={handleSubmit} disabled={isSubmitting}>
                {isSubmitting ? "Creating Import Run..." : "Create Import Run"}
              </Button>
            </div>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <div className="space-y-2">
              <p className="text-sm font-semibold text-slate-900">Or upload a CSV</p>
              <p className="text-sm text-slate-500">
                Supports standard quoted CSV files up to {MAX_CSV_BID_IDS} Bid IDs.
                Common Bid ID headers are auto-detected.
              </p>
            </div>
            <div className="mt-4 space-y-4">
              <input
                type="file"
                accept=".csv,text/csv"
                onChange={(event) => {
                  const file = event.target.files?.[0] ?? null;
                  void handleCsvFileChange(file);
                }}
                className="block w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
              />

              {isPreviewingCsv ? (
                <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
                  Previewing CSV...
                </div>
              ) : null}

              {csvPreview ? (
                <div className="space-y-4 rounded-lg bg-slate-50 p-4">
                  <div className="grid gap-3 md:grid-cols-4">
                    <div>
                      <p className="text-xs uppercase tracking-wide text-slate-500">
                        Data Rows
                      </p>
                      <p className="mt-1 text-lg font-semibold text-slate-900">
                        {csvPreview.totalRows}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs uppercase tracking-wide text-slate-500">
                        Valid Bid IDs
                      </p>
                      <p className="mt-1 text-lg font-semibold text-slate-900">
                        {csvPreview.validBidIdCount}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs uppercase tracking-wide text-slate-500">
                        Duplicates Removed
                      </p>
                      <p className="mt-1 text-lg font-semibold text-slate-900">
                        {csvPreview.duplicateCount}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs uppercase tracking-wide text-slate-500">
                        Invalid Rows
                      </p>
                      <p className="mt-1 text-lg font-semibold text-slate-900">
                        {csvPreview.invalidRowCount}
                      </p>
                    </div>
                  </div>

                  {csvPreview.headerDetected && csvPreview.columnOptions.length > 1 ? (
                    <label className="block text-sm text-slate-700">
                      <span className="mb-2 block font-medium">Bid ID column</span>
                      <select
                        value={selectedCsvColumnKey}
                        onChange={(event) => {
                          void handleCsvColumnChange(event.target.value);
                        }}
                        className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                      >
                        {csvPreview.columnOptions.map((option) => (
                          <option key={option.key} value={option.key}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </label>
                  ) : null}

                  <div className="space-y-2">
                    <p className="text-sm font-medium text-slate-900">
                      Preview Bid IDs
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {csvPreview.previewBidIds.map((bidId) => (
                        <Badge key={bidId} variant="info">
                          {bidId}
                        </Badge>
                      ))}
                    </div>
                  </div>

                  {csvPreview.invalidRows.length > 0 ? (
                    <div className="space-y-2">
                      <p className="text-sm font-medium text-amber-800">
                        Invalid rows
                      </p>
                      <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
                        {csvPreview.invalidRows.map((row) => (
                          <div key={`${row.rowNumber}-${row.value}`}>
                            Row {row.rowNumber}: {row.value || "(blank)"} - {row.message}
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}

                  <div className="flex justify-end">
                    <Button onClick={handleSubmitCsv} disabled={isSubmittingCsv}>
                      {isSubmittingCsv
                        ? "Creating CSV Import Run..."
                        : "Create Import Run From CSV"}
                    </Button>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
          {errorMessage ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
              {errorMessage}
            </div>
          ) : null}
        </CardContent>
      </Card>

      {activeRun ? (
        <Card>
          <CardHeader>
            <CardTitle>Import Run Progress</CardTitle>
            <CardDescription>
              Import run `{activeRun.id}` is {activeRun.status}. {activeRun.completedCount}{" "}
              completed, {activeRun.failedCount} failed, {activeRun.reusedCount} reused,
              and {activeRun.fetchedCount} fetched so far.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm text-slate-600">
                <span>{activeRun.percentComplete}% complete</span>
                <span>
                  {activeRun.completedCount + activeRun.failedCount} /{" "}
                  {activeRun.totalItems}
                </span>
              </div>
              <div className="h-3 overflow-hidden rounded-full bg-slate-100">
                <div
                  className={`h-full rounded-full transition-all ${progressTone(
                    activeRun.percentComplete,
                  )}`}
                  style={{ width: `${activeRun.percentComplete}%` }}
                />
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
              <div className="rounded-lg bg-slate-50 p-3 text-sm">
                <p className="text-slate-500">Queued</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {activeRun.queuedCount}
                </p>
              </div>
              <div className="rounded-lg bg-slate-50 p-3 text-sm">
                <p className="text-slate-500">Running</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {activeRun.runningCount}
                </p>
              </div>
              <div className="rounded-lg bg-slate-50 p-3 text-sm">
                <p className="text-slate-500">Completed</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {activeRun.completedCount}
                </p>
              </div>
              <div className="rounded-lg bg-slate-50 p-3 text-sm">
                <p className="text-slate-500">Reused</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {activeRun.reusedCount}
                </p>
              </div>
              <div className="rounded-lg bg-slate-50 p-3 text-sm">
                <p className="text-slate-500">Fetched</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {activeRun.fetchedCount}
                </p>
              </div>
              <div className="rounded-lg bg-slate-50 p-3 text-sm">
                <p className="text-slate-500">Failed</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {activeRun.failedCount}
                </p>
              </div>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-sm text-slate-500">
                <Badge variant={isTerminal ? "default" : "warning"}>
                  {isProcessing && !isTerminal ? "Processing" : activeRun.status}
                </Badge>
                <span>
                  {activeRun.forceRefresh
                    ? "This run forces refreshes for existing investigations."
                    : "This run reuses existing investigations by default."}
                </span>
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  onClick={handleRetryFailed}
                  disabled={activeRun.failedCount === 0}
                >
                  Retry Failed Items
                </Button>
                <Button variant="outline" onClick={handleRerun}>
                  Rerun Import
                </Button>
              </div>
            </div>

            {activeRun.lastError ? (
              <div className="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
                {activeRun.lastError}
              </div>
            ) : null}

            <div className="space-y-3">
              <div>
                <h3 className="text-sm font-semibold text-slate-900">Run Items</h3>
                <p className="text-sm text-slate-500">
                  Per-bid processing status for this async import run.
                </p>
              </div>
              <ImportRunItemsTable run={activeRun} />
            </div>

            {processedInvestigations.length > 0 ? (
              <div className="space-y-3">
                <div>
                  <h3 className="text-sm font-semibold text-slate-900">
                    Processed Investigations
                  </h3>
                  <p className="text-sm text-slate-500">
                    Investigations that already resolved into stored bid records.
                  </p>
                </div>
                <InvestigationTable items={processedInvestigations} />
              </div>
            ) : null}
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
