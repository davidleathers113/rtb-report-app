"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import { ImportRunList, type ImportRunListItem } from "@/components/investigations/import-run-list";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { ApiErrorResponse } from "@/types/import-run";

interface StalledDirectImportRunsCardProps {
  initialRuns: ImportRunListItem[];
}

function getErrorMessage(payload: ApiErrorResponse | { error?: string } | null, fallback: string) {
  if (!payload) {
    return fallback;
  }

  return payload.error ?? fallback;
}

export function StalledDirectImportRunsCard(input: StalledDirectImportRunsCardProps) {
  const router = useRouter();
  const [hiddenRunIds, setHiddenRunIds] = useState<string[]>([]);
  const [resumingRunIds, setResumingRunIds] = useState<string[]>([]);
  const [isResumingAll, setIsResumingAll] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const visibleRuns = useMemo(() => {
    if (hiddenRunIds.length === 0) {
      return input.initialRuns;
    }

    const hiddenSet = new Set(hiddenRunIds);
    return input.initialRuns.filter((run) => !hiddenSet.has(run.id));
  }, [hiddenRunIds, input.initialRuns]);

  async function resumeRuns(importRunIds: string[], isBulk: boolean) {
    if (importRunIds.length === 0) {
      return;
    }

    setErrorMessage(null);
    if (isBulk) {
      setIsResumingAll(true);
    } else {
      setResumingRunIds((current) => {
        const next = new Set(current);
        importRunIds.forEach((id) => next.add(id));
        return Array.from(next);
      });
    }

    try {
      const response = await fetch("/api/import-runs/csv-direct/recover", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          importRunIds,
          stalledOnly: true,
          maxRuns: importRunIds.length,
        }),
      });
      const payload = (await response.json().catch(() => null)) as
        | ApiErrorResponse
        | { error?: string }
        | null;

      if (!response.ok) {
        throw new Error(getErrorMessage(payload, "Unable to resume stalled direct CSV runs."));
      }

      setHiddenRunIds((current) => {
        const next = new Set(current);
        importRunIds.forEach((id) => next.add(id));
        return Array.from(next);
      });
      router.refresh();
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected stalled resume error.",
      );
    } finally {
      if (isBulk) {
        setIsResumingAll(false);
      } else {
        setResumingRunIds((current) => {
          const next = new Set(current);
          importRunIds.forEach((id) => next.delete(id));
          return Array.from(next);
        });
      }
    }
  }

  return (
    <Card>
      <CardHeader className="flex flex-row flex-wrap items-start justify-between gap-4">
        <div className="space-y-1">
          <CardTitle>Stalled Direct CSV Runs</CardTitle>
          <CardDescription>
            Filtered view of stalled `csv_direct_import` runs. Resume one run at a time or
            resume the full stalled queue sequentially.
          </CardDescription>
        </div>
        <Button
          onClick={() => void resumeRuns(visibleRuns.map((run) => run.id), true)}
          disabled={isResumingAll || visibleRuns.length === 0}
        >
          {isResumingAll ? "Resuming All..." : "Resume All Stalled"}
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="text-sm text-slate-500">
          {visibleRuns.length} stalled direct CSV run{visibleRuns.length === 1 ? "" : "s"} found.
        </div>
        {errorMessage ? (
          <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            {errorMessage}
          </div>
        ) : null}
        <ImportRunList
          items={visibleRuns}
          emptyMessage="No stalled direct CSV runs found."
          onResume={(importRunId) => void resumeRuns([importRunId], false)}
          resumingRunIds={resumingRunIds}
        />
      </CardContent>
    </Card>
  );
}
