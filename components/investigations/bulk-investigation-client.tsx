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
import type {
  CsvPreviewResult,
  ImportRunDetail,
  RingbaRecentImportDiagnostics,
} from "@/types/import-run";
import type {
  ImportScheduleDetail,
  ImportScheduleHealthStatus,
  ImportScheduleRunHistoryPage,
  ImportScheduleRunHistoryStatusFilter,
  ImportScheduleRunSummary,
} from "@/types/import-schedule";
import type { ImportOpsEventPage } from "@/types/ops-event";

const terminalStatuses = new Set([
  "completed",
  "completed_with_errors",
  "failed",
  "cancelled",
]);

const ringbaRecentImportStages = [
  "creating_export",
  "polling_export",
  "downloading",
  "extracting",
  "parsing",
  "queued",
  "processing",
  "completed",
] as const;

const ringbaRecentImportStageLabels: Record<string, string> = {
  creating_export: "Creating export",
  polling_export: "Polling export",
  downloading: "Downloading",
  extracting: "Extracting",
  parsing: "Parsing",
  queued: "Queued",
  processing: "Processing",
  completed: "Completed",
  failed: "Failed",
};

function progressTone(percentComplete: number) {
  if (percentComplete >= 100) {
    return "bg-emerald-500";
  }

  if (percentComplete >= 50) {
    return "bg-sky-500";
  }

  return "bg-amber-500";
}

function getSourceStageLabel(sourceStage: string) {
  return ringbaRecentImportStageLabels[sourceStage] ?? sourceStage;
}

function getRingbaDiagnostics(sourceMetadata: Record<string, unknown>) {
  const diagnostics = sourceMetadata.diagnostics;

  if (diagnostics && typeof diagnostics === "object" && !Array.isArray(diagnostics)) {
    return diagnostics as RingbaRecentImportDiagnostics;
  }

  return {};
}

function formatTimestamp(value: string | null) {
  if (!value) {
    return "Never";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

function formatDuration(value: number | null) {
  if (value === null) {
    return "N/A";
  }

  if (value < 1000) {
    return `${value} ms`;
  }

  const seconds = Math.round(value / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return `${minutes}m ${remainingSeconds}s`;
}

function getScheduleHealthBadgeVariant(status: ImportScheduleHealthStatus) {
  if (status === "healthy") {
    return "success" as const;
  }

  if (status === "warning") {
    return "warning" as const;
  }

  if (status === "failing" || status === "stale") {
    return "destructive" as const;
  }

  return "default" as const;
}

function getRunStatusBadgeVariant(run: ImportScheduleRunSummary) {
  if (run.isStale) {
    return "destructive" as const;
  }

  if (run.status === "completed") {
    return "success" as const;
  }

  if (run.status === "completed_with_errors" || run.status === "failed") {
    return "destructive" as const;
  }

  if (run.status === "queued" || run.status === "running") {
    return "warning" as const;
  }

  return "default" as const;
}

function buildScheduleDrafts(schedules: ImportScheduleDetail[]) {
  return Object.fromEntries(
    schedules.map((schedule) => [
      schedule.id,
      {
        name: schedule.name,
        windowMinutes: schedule.windowMinutes,
        overlapMinutes: schedule.overlapMinutes,
        pauseReason: schedule.pauseReason ?? "",
        snoozeHours: 4,
      },
    ]),
  ) as Record<
    string,
    {
      name: string;
      windowMinutes: 5 | 15 | 60;
      overlapMinutes: number;
      pauseReason: string;
      snoozeHours: number;
    }
  >;
}

export function BulkInvestigationClient({
  initialSchedules,
  canManualTriggerSchedules,
}: {
  initialSchedules: ImportScheduleDetail[];
  canManualTriggerSchedules: boolean;
}) {
  const [rawBidIds, setRawBidIds] = useState("");
  const [forceRefresh, setForceRefresh] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isPreviewingCsv, setIsPreviewingCsv] = useState(false);
  const [isSubmittingCsv, setIsSubmittingCsv] = useState(false);
  const [isSubmittingRecentImport, setIsSubmittingRecentImport] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [activeRun, setActiveRun] = useState<ImportRunDetail | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvPreview, setCsvPreview] = useState<CsvPreviewResult | null>(null);
  const [selectedCsvColumnKey, setSelectedCsvColumnKey] = useState<string>("");
  const [recentWindowMinutes, setRecentWindowMinutes] = useState<5 | 15 | 60>(15);
  const [schedules, setSchedules] = useState<ImportScheduleDetail[]>(initialSchedules);
  const [isRefreshingSchedules, setIsRefreshingSchedules] = useState(false);
  const [isSubmittingSchedule, setIsSubmittingSchedule] = useState(false);
  const [isTriggeringSchedules, setIsTriggeringSchedules] = useState(false);
  const [scheduleName, setScheduleName] = useState("");
  const [scheduleAccountId, setScheduleAccountId] = useState(
    initialSchedules[0]?.accountId ?? "",
  );
  const [scheduleWindowMinutes, setScheduleWindowMinutes] = useState<5 | 15 | 60>(15);
  const [scheduleOverlapMinutes, setScheduleOverlapMinutes] = useState(2);
  const [scheduleDrafts, setScheduleDrafts] = useState(buildScheduleDrafts(initialSchedules));
  const [scheduleHealthFilter, setScheduleHealthFilter] = useState<
    "all" | "unhealthy" | "healthy" | "enabled" | "disabled"
  >("all");
  const [runHistoryPages, setRunHistoryPages] = useState<
    Record<string, ImportScheduleRunHistoryPage>
  >(
    Object.fromEntries(
      initialSchedules.map((schedule) => [
        schedule.id,
        {
          items: schedule.recentRuns,
          total: schedule.recentRunTotalCount,
          limit: 5,
          offset: 0,
          statusFilter: "all",
        },
      ]),
    ),
  );
  const [runHistoryLoading, setRunHistoryLoading] = useState<Record<string, boolean>>({});
  const [opsEventPages, setOpsEventPages] = useState<Record<string, ImportOpsEventPage>>(
    Object.fromEntries(
      initialSchedules.map((schedule) => [
        schedule.id,
        {
          items: schedule.recentOpsEvents,
          total: schedule.recentOpsEventTotalCount,
          limit: 5,
          offset: 0,
          eventType: "all",
          severity: "all",
        },
      ]),
    ),
  );
  const [opsEventLoading, setOpsEventLoading] = useState<Record<string, boolean>>({});
  const scheduleHealthSummary = useMemo(() => {
    const enabledCount = schedules.filter((schedule) => schedule.isEnabled).length;
    const unhealthyCount = schedules.filter((schedule) =>
      ["warning", "failing", "stale"].includes(schedule.healthStatus),
    ).length;
    const staleCount = schedules.filter((schedule) => schedule.healthStatus === "stale").length;
    const activeCount = schedules.filter((schedule) => Boolean(schedule.activeRun)).length;

    return {
      totalCount: schedules.length,
      enabledCount,
      disabledCount: schedules.length - enabledCount,
      unhealthyCount,
      staleCount,
      activeCount,
    };
  }, [schedules]);

  const parsedCount = useMemo(() => parseBidIds(rawBidIds).length, [rawBidIds]);
  const filteredSchedules = useMemo(() => {
    return schedules.filter((schedule) => {
      if (scheduleHealthFilter === "all") {
        return true;
      }
      if (scheduleHealthFilter === "enabled") {
        return schedule.isEnabled;
      }
      if (scheduleHealthFilter === "disabled") {
        return !schedule.isEnabled;
      }
      if (scheduleHealthFilter === "healthy") {
        return schedule.healthStatus === "healthy";
      }

      return ["warning", "failing", "stale"].includes(schedule.healthStatus);
    });
  }, [scheduleHealthFilter, schedules]);
  const isTerminal = activeRun ? terminalStatuses.has(activeRun.status) : false;
  const activeRunId = activeRun?.id ?? null;
  const isRingbaRecentRun = activeRun?.sourceType === "ringba_recent_import";
  const currentSourceStage = activeRun?.sourceStage ?? "queued";
  const ringbaDiagnostics = activeRun
    ? getRingbaDiagnostics(activeRun.sourceMetadata)
    : {};
  const currentSourceStageIndex = ringbaRecentImportStages.findIndex((stage) => {
    return stage === currentSourceStage;
  });
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

  async function refreshSchedules() {
    setIsRefreshingSchedules(true);

    try {
      const response = await fetch("/api/import-schedules");
      const payload = (await response.json()) as
        | ImportScheduleDetail[]
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to refresh import schedules.");
      }

      const nextSchedules = payload as ImportScheduleDetail[];
      setSchedules(nextSchedules);
      setScheduleDrafts(buildScheduleDrafts(nextSchedules));
      setRunHistoryPages((current) =>
        Object.fromEntries(
          nextSchedules.map((schedule) => [
            schedule.id,
            current[schedule.id] ?? {
              items: schedule.recentRuns,
              total: schedule.recentRunTotalCount,
              limit: 5,
              offset: 0,
              statusFilter: "all",
            },
          ]),
        ),
      );
      setOpsEventPages((current) =>
        Object.fromEntries(
          nextSchedules.map((schedule) => [
            schedule.id,
            current[schedule.id] ?? {
              items: schedule.recentOpsEvents,
              total: schedule.recentOpsEventTotalCount,
              limit: 5,
              offset: 0,
              eventType: "all",
              severity: "all",
            },
          ]),
        ),
      );
      if (!scheduleAccountId && nextSchedules[0]?.accountId) {
        setScheduleAccountId(nextSchedules[0].accountId);
      }
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected import schedule refresh error.",
      );
    } finally {
      setIsRefreshingSchedules(false);
    }
  }

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

  async function handleSubmitRecentImport() {
    setIsSubmittingRecentImport(true);
    setErrorMessage(null);

    try {
      const response = await fetch("/api/import-runs/ringba-recent", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          windowMinutes: recentWindowMinutes,
          forceRefresh,
        }),
      });
      const payload = (await response.json()) as
        | ImportRunDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(
          errorPayload.error ?? "Unable to create Ringba recent import run.",
        );
      }

      setActiveRun(payload as ImportRunDetail);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected Ringba import error.",
      );
    } finally {
      setIsSubmittingRecentImport(false);
    }
  }

  async function handleCreateSchedule() {
    setIsSubmittingSchedule(true);
    setErrorMessage(null);

    try {
      const response = await fetch("/api/import-schedules", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: scheduleName,
          isEnabled: true,
          accountId: scheduleAccountId,
          sourceType: "ringba_recent_import",
          windowMinutes: scheduleWindowMinutes,
          overlapMinutes: scheduleOverlapMinutes,
          maxConcurrentRuns: 1,
        }),
      });
      const payload = (await response.json()) as
        | ImportScheduleDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to create import schedule.");
      }

      setScheduleName("");
      setScheduleAccountId(scheduleAccountId);
      setScheduleWindowMinutes(15);
      setScheduleOverlapMinutes(2);
      await refreshSchedules();
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected schedule create error.",
      );
    } finally {
      setIsSubmittingSchedule(false);
    }
  }

  async function handleUpdateSchedule(
    scheduleId: string,
    patch: Partial<{
      name: string;
      isEnabled: boolean;
      windowMinutes: 5 | 15 | 60;
      overlapMinutes: number;
    }>,
  ) {
    setErrorMessage(null);

    try {
      const response = await fetch(`/api/import-schedules/${encodeURIComponent(scheduleId)}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(patch),
      });
      const payload = (await response.json()) as
        | ImportScheduleDetail
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to update import schedule.");
      }

      setSchedules((current) =>
        current.map((schedule) =>
          schedule.id === scheduleId ? (payload as ImportScheduleDetail) : schedule,
        ),
      );
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected schedule update error.",
      );
    }
  }

  async function performScheduleAction(
    scheduleId: string,
    body: Record<string, unknown>,
  ) {
    setErrorMessage(null);

    try {
      const response = await fetch(`/api/import-schedules/${encodeURIComponent(scheduleId)}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          actionSource: "manual_ui",
          ...body,
        }),
      });
      const payload = (await response.json()) as
        | { schedule?: ImportScheduleDetail | null; run?: ImportRunDetail | null; error?: string }
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to perform schedule action.");
      }

      const result = payload as {
        schedule?: ImportScheduleDetail | null;
        run?: ImportRunDetail | null;
      };
      if (result.run) {
        setActiveRun(result.run);
      }
      await refreshSchedules();
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected schedule action error.",
      );
    }
  }

  function updateScheduleDraft(
    scheduleId: string,
    patch: Partial<{
      name: string;
      windowMinutes: 5 | 15 | 60;
      overlapMinutes: number;
      pauseReason: string;
      snoozeHours: number;
    }>,
  ) {
    setScheduleDrafts((current) => ({
      ...current,
      [scheduleId]: {
        ...current[scheduleId],
        ...patch,
      },
    }));
  }

  async function handleTriggerSchedules() {
    setIsTriggeringSchedules(true);
    setErrorMessage(null);

    try {
      const response = await fetch("/api/import-schedules/trigger", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });
      const payload = (await response.json()) as
        | { processedRuns?: ImportRunDetail[]; error?: string }
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to trigger import schedules.");
      }

      const processedRuns = (payload as { processedRuns?: ImportRunDetail[] }).processedRuns ?? [];
      if (processedRuns.length > 0) {
        setActiveRun(processedRuns[0] ?? null);
      }

      await refreshSchedules();
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected schedule trigger error.",
      );
    } finally {
      setIsTriggeringSchedules(false);
    }
  }

  async function loadImportRun(importRunId: string) {
    setErrorMessage(null);

    try {
      const response = await fetch(`/api/import-runs/${encodeURIComponent(importRunId)}`);
      const payload = (await response.json()) as ImportRunDetail | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to load import run.");
      }

      setActiveRun(payload as ImportRunDetail);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected import run load error.",
      );
    }
  }

  async function loadScheduleRunHistory(
    scheduleId: string,
    input?: Partial<{
      offset: number;
      statusFilter: ImportScheduleRunHistoryStatusFilter;
      append: boolean;
    }>,
  ) {
    const current = runHistoryPages[scheduleId] ?? {
      items: [],
      total: 0,
      limit: 5,
      offset: 0,
      statusFilter: "all" as const,
    };
    const limit = current.limit;
    const offset = input?.offset ?? 0;
    const statusFilter = input?.statusFilter ?? current.statusFilter;

    setRunHistoryLoading((state) => ({
      ...state,
      [scheduleId]: true,
    }));
    setErrorMessage(null);

    try {
      const response = await fetch(
        `/api/import-schedules/${encodeURIComponent(scheduleId)}?view=history&limit=${limit}&offset=${offset}&status=${statusFilter}`,
      );
      const payload = (await response.json()) as
        | ImportScheduleRunHistoryPage
        | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to load schedule run history.");
      }

      const page = payload as ImportScheduleRunHistoryPage;
      setRunHistoryPages((state) => ({
        ...state,
        [scheduleId]: {
          ...page,
          items: input?.append ? [...current.items, ...page.items] : page.items,
        },
      }));
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected schedule run history error.",
      );
    } finally {
      setRunHistoryLoading((state) => ({
        ...state,
        [scheduleId]: false,
      }));
    }
  }

  async function loadScheduleOpsEvents(
    scheduleId: string,
    input?: Partial<{
      offset: number;
      eventType: string;
      severity: string;
      append: boolean;
    }>,
  ) {
    const current = opsEventPages[scheduleId] ?? {
      items: [],
      total: 0,
      limit: 5,
      offset: 0,
      eventType: "all" as const,
      severity: "all" as const,
    };
    const limit = current.limit;
    const offset = input?.offset ?? 0;
    const eventType = input?.eventType ?? current.eventType;
    const severity = input?.severity ?? current.severity;

    setOpsEventLoading((state) => ({
      ...state,
      [scheduleId]: true,
    }));
    setErrorMessage(null);

    try {
      const response = await fetch(
        `/api/import-schedules/${encodeURIComponent(scheduleId)}?view=events&limit=${limit}&offset=${offset}&eventType=${eventType}&severity=${severity}`,
      );
      const payload = (await response.json()) as ImportOpsEventPage | { error?: string };

      if (!response.ok) {
        const errorPayload = payload as { error?: string };
        throw new Error(errorPayload.error ?? "Unable to load ops events.");
      }

      const page = payload as ImportOpsEventPage;
      setOpsEventPages((state) => ({
        ...state,
        [scheduleId]: {
          ...page,
          items: input?.append ? [...current.items, ...page.items] : page.items,
        },
      }));
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Unexpected ops events error.",
      );
    } finally {
      setOpsEventLoading((state) => ({
        ...state,
        [scheduleId]: false,
      }));
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
                  setRecentWindowMinutes(15);
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
          <div className="rounded-lg border border-slate-200 p-4">
            <div className="space-y-2">
              <p className="text-sm font-semibold text-slate-900">
                Or import recent bids from Ringba
              </p>
              <p className="text-sm text-slate-500">
                Uses Ringba&apos;s RTB export API to discover recent Bid IDs, then
                queues them into the same async import-run pipeline.
              </p>
            </div>
            <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
              <div className="flex flex-wrap gap-2">
                {[5, 15, 60].map((minutes) => (
                  <Button
                    key={minutes}
                    variant={recentWindowMinutes === minutes ? "default" : "outline"}
                    onClick={() => setRecentWindowMinutes(minutes as 5 | 15 | 60)}
                  >
                    Last {minutes} min
                  </Button>
                ))}
              </div>
              <Button
                onClick={handleSubmitRecentImport}
                disabled={isSubmittingRecentImport}
              >
                {isSubmittingRecentImport
                  ? "Creating Ringba Import..."
                  : "Create Ringba Recent Import"}
              </Button>
            </div>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="space-y-2">
                <p className="text-sm font-semibold text-slate-900">
                  Scheduled recent imports
                </p>
                <p className="text-sm text-slate-500">
                  Create recurring Ringba recent-import schedules and trigger due
                  schedules through the same hardened source-stage pipeline.
                </p>
              </div>
              <Button
                variant="outline"
                onClick={handleTriggerSchedules}
                disabled={
                  isTriggeringSchedules ||
                  isRefreshingSchedules ||
                  !canManualTriggerSchedules
                }
              >
                {isTriggeringSchedules ? "Running Schedules..." : "Run Due Schedules Now"}
              </Button>
            </div>
            {!canManualTriggerSchedules ? (
              <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
                Manual browser-triggering is disabled while trigger-secret protection is
                enabled. Use your cron job or send a server-side request with
                `x-import-schedules-trigger-secret`.
              </div>
            ) : null}
            <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-5">
              <input
                value={scheduleName}
                onChange={(event) => setScheduleName(event.target.value)}
                placeholder="Schedule name"
                className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
              />
              <input
                value={scheduleAccountId}
                onChange={(event) => setScheduleAccountId(event.target.value)}
                placeholder="Ringba account id"
                className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
              />
              <select
                value={scheduleWindowMinutes}
                onChange={(event) =>
                  setScheduleWindowMinutes(Number(event.target.value) as 5 | 15 | 60)
                }
                className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
              >
                <option value={5}>Every 5 minutes</option>
                <option value={15}>Every 15 minutes</option>
                <option value={60}>Every 60 minutes</option>
              </select>
              <input
                type="number"
                min={0}
                max={15}
                value={scheduleOverlapMinutes}
                onChange={(event) =>
                  setScheduleOverlapMinutes(Number(event.target.value) || 0)
                }
                className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
              />
              <Button onClick={handleCreateSchedule} disabled={isSubmittingSchedule}>
                {isSubmittingSchedule ? "Creating Schedule..." : "Create Schedule"}
              </Button>
            </div>
            <div className="mt-4 space-y-3">
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                <div className="rounded-lg bg-slate-50 p-3 text-sm">
                  <p className="text-slate-500">Schedules</p>
                  <p className="mt-1 text-xl font-semibold text-slate-900">
                    {scheduleHealthSummary.totalCount}
                  </p>
                </div>
                <div className="rounded-lg bg-slate-50 p-3 text-sm">
                  <p className="text-slate-500">Enabled</p>
                  <p className="mt-1 text-xl font-semibold text-slate-900">
                    {scheduleHealthSummary.enabledCount}
                  </p>
                </div>
                <div className="rounded-lg bg-slate-50 p-3 text-sm">
                  <p className="text-slate-500">Active Runs</p>
                  <p className="mt-1 text-xl font-semibold text-slate-900">
                    {scheduleHealthSummary.activeCount}
                  </p>
                </div>
                <div className="rounded-lg bg-slate-50 p-3 text-sm">
                  <p className="text-slate-500">Unhealthy</p>
                  <p className="mt-1 text-xl font-semibold text-slate-900">
                    {scheduleHealthSummary.unhealthyCount}
                  </p>
                </div>
                <div className="rounded-lg bg-slate-50 p-3 text-sm">
                  <p className="text-slate-500">Stale</p>
                  <p className="mt-1 text-xl font-semibold text-slate-900">
                    {scheduleHealthSummary.staleCount}
                  </p>
                </div>
              </div>
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="space-y-1">
                  <p className="text-sm font-semibold text-slate-900">Operator view</p>
                  <p className="text-sm text-slate-500">
                    Unhealthy schedules are shown first with recent analytics and failures.
                  </p>
                </div>
                <select
                  value={scheduleHealthFilter}
                  onChange={(event) =>
                    setScheduleHealthFilter(
                      event.target.value as
                        | "all"
                        | "unhealthy"
                        | "healthy"
                        | "enabled"
                        | "disabled",
                    )
                  }
                  className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                >
                  <option value="all">All schedules</option>
                  <option value="unhealthy">Unhealthy only</option>
                  <option value="healthy">Healthy only</option>
                  <option value="enabled">Enabled only</option>
                  <option value="disabled">Disabled only</option>
                </select>
              </div>
              {filteredSchedules.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
                  No schedules match the current filter.
                </div>
              ) : (
                filteredSchedules.map((schedule) => {
                  const draft = scheduleDrafts[schedule.id] ?? {
                    name: schedule.name,
                    windowMinutes: schedule.windowMinutes,
                    overlapMinutes: schedule.overlapMinutes,
                    pauseReason: schedule.pauseReason ?? "",
                    snoozeHours: 4,
                  };
                  const activeScheduleRunId = schedule.activeRun?.id ?? null;
                  const latestFailedRun =
                    schedule.recentRuns.find((run) => {
                      return run.status === "failed" || run.status === "completed_with_errors";
                    }) ?? null;
                  const latestRun = schedule.recentRuns[0] ?? null;
                  const runHistoryPage = runHistoryPages[schedule.id] ?? {
                    items: schedule.recentRuns,
                    total: schedule.recentRunTotalCount,
                    limit: 5,
                    offset: 0,
                    statusFilter: "all" as const,
                  };
                  const opsEventPage = opsEventPages[schedule.id] ?? {
                    items: schedule.recentOpsEvents,
                    total: schedule.recentOpsEventTotalCount,
                    limit: 5,
                    offset: 0,
                    eventType: "all" as const,
                    severity: "all" as const,
                  };

                  return (
                    <div
                      key={schedule.id}
                      className={`rounded-lg border p-4 ${
                        schedule.healthStatus === "failing" || schedule.healthStatus === "stale"
                          ? "border-rose-200 bg-rose-50"
                          : schedule.healthStatus === "warning"
                            ? "border-amber-200 bg-amber-50"
                            : "border-slate-200 bg-slate-50"
                      }`}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="space-y-2">
                          <div className="flex flex-wrap items-center gap-2">
                            <input
                              value={draft.name}
                              onChange={(event) =>
                                updateScheduleDraft(schedule.id, {
                                  name: event.target.value,
                                })
                              }
                              className="rounded-md border border-slate-200 bg-white px-2 py-1 text-sm font-medium text-slate-900"
                            />
                            <Badge variant={schedule.isEnabled ? "default" : "info"}>
                              {schedule.isEnabled ? "Enabled" : "Disabled"}
                            </Badge>
                            <Badge variant={getScheduleHealthBadgeVariant(schedule.healthStatus)}>
                              {schedule.healthStatus}
                            </Badge>
                            {schedule.isPaused ? (
                              <Badge variant="warning">Paused</Badge>
                            ) : null}
                            {schedule.isCurrentAlertAcknowledged ? (
                              <Badge variant="info">Acknowledged</Badge>
                            ) : null}
                            {schedule.isAlertSnoozed ? (
                              <Badge variant="info">
                                Snoozed until {formatTimestamp(schedule.alertSnoozedUntil)}
                              </Badge>
                            ) : null}
                            {schedule.consecutiveFailureCount > 0 ? (
                              <Badge variant="destructive">
                                {schedule.consecutiveFailureCount} consecutive failure
                                {schedule.consecutiveFailureCount === 1 ? "" : "s"}
                              </Badge>
                            ) : null}
                          </div>
                          <p className="text-sm text-slate-600">{schedule.healthSummary}</p>
                          {schedule.currentAlertLabel ? (
                            <p className="text-sm text-slate-600">
                              Current alert: {schedule.currentAlertLabel}
                            </p>
                          ) : null}
                          <p className="text-sm text-slate-500">
                            Account `{schedule.accountId}` • overlap {schedule.overlapMinutes} min
                            • max concurrent {schedule.maxConcurrentRuns}
                          </p>
                          <div className="flex flex-wrap gap-2">
                            <select
                              value={draft.windowMinutes}
                              onChange={(event) =>
                                updateScheduleDraft(schedule.id, {
                                  windowMinutes: Number(event.target.value) as 5 | 15 | 60,
                                })
                              }
                              className="rounded-md border border-slate-200 bg-white px-2 py-1 text-sm text-slate-900"
                            >
                              <option value={5}>Every 5 min</option>
                              <option value={15}>Every 15 min</option>
                              <option value={60}>Every 60 min</option>
                            </select>
                            <input
                              type="number"
                              min={0}
                              max={15}
                              value={draft.overlapMinutes}
                              onChange={(event) =>
                                updateScheduleDraft(schedule.id, {
                                  overlapMinutes: Number(event.target.value) || 0,
                                })
                              }
                              className="w-28 rounded-md border border-slate-200 bg-white px-2 py-1 text-sm text-slate-900"
                            />
                          </div>
                          <div className="grid gap-2 text-sm text-slate-500 md:grid-cols-2">
                            <p>Last triggered: {formatTimestamp(schedule.lastTriggeredAt)}</p>
                            <p>Last success: {formatTimestamp(schedule.lastSucceededAt)}</p>
                            <p>Last failure: {formatTimestamp(schedule.lastFailedAt)}</p>
                            <p>
                              Active run:{" "}
                              {schedule.activeRun
                                ? `${schedule.activeRun.status} • ${schedule.activeRun.sourceStage}${schedule.activeRun.isStale ? " • stale" : ""}`
                                : "No active run"}
                            </p>
                          </div>
                          {schedule.lastError ? (
                            <p className="text-sm text-rose-700">{schedule.lastError}</p>
                          ) : null}
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {schedule.activeRun ? (
                            <Button
                              variant="outline"
                              onClick={() =>
                                activeScheduleRunId
                                  ? void loadImportRun(activeScheduleRunId)
                                  : undefined
                              }
                            >
                              Load Active Run
                            </Button>
                          ) : null}
                          <Button
                            variant="outline"
                            onClick={() =>
                              void performScheduleAction(schedule.id, {
                                action: schedule.isPaused ? "resume_schedule" : "pause_schedule",
                                reason: draft.pauseReason,
                              })
                            }
                          >
                            {schedule.isPaused ? "Resume" : "Pause"}
                          </Button>
                          <Button
                            variant="outline"
                            onClick={() =>
                              handleUpdateSchedule(schedule.id, {
                                isEnabled: !schedule.isEnabled,
                              })
                            }
                          >
                            {schedule.isEnabled ? "Disable" : "Enable"}
                          </Button>
                          <Button
                            variant="outline"
                            onClick={() =>
                              handleUpdateSchedule(schedule.id, {
                                name: draft.name,
                                windowMinutes: draft.windowMinutes,
                                overlapMinutes: draft.overlapMinutes,
                              })
                            }
                          >
                            Save
                          </Button>
                          <Button
                            variant="outline"
                            disabled={!schedule.currentAlertKey || schedule.isCurrentAlertAcknowledged}
                            onClick={() =>
                              void performScheduleAction(schedule.id, {
                                action: "acknowledge_alert",
                              })
                            }
                          >
                            Acknowledge
                          </Button>
                          <Button
                            variant="outline"
                            onClick={() =>
                              void performScheduleAction(schedule.id, {
                                action: "snooze_alert",
                                snoozedUntil: new Date(
                                  Date.now() + draft.snoozeHours * 60 * 60 * 1000,
                                ).toISOString(),
                              })
                            }
                          >
                            Snooze
                          </Button>
                          <Button
                            variant="outline"
                            disabled={!schedule.isAlertSnoozed}
                            onClick={() =>
                              void performScheduleAction(schedule.id, {
                                action: "clear_snooze",
                              })
                            }
                          >
                            Clear Snooze
                          </Button>
                          <Button
                            variant="outline"
                            onClick={() =>
                              void performScheduleAction(schedule.id, {
                                action: "run_now",
                                forceRefresh: false,
                              })
                            }
                          >
                            Run Now
                          </Button>
                          <Button
                            variant="outline"
                            disabled={!latestFailedRun}
                            onClick={() =>
                              latestFailedRun
                                ? void performScheduleAction(schedule.id, {
                                    action: "retry_failed_run",
                                    importRunId: latestFailedRun.id,
                                    forceRefresh,
                                  })
                                : undefined
                            }
                          >
                            Retry Latest Failed
                          </Button>
                          <Button
                            variant="outline"
                            disabled={!latestRun}
                            onClick={() =>
                              latestRun
                                ? void performScheduleAction(schedule.id, {
                                    action: "force_refresh_rerun",
                                    importRunId: latestRun.id,
                                  })
                                : undefined
                            }
                          >
                            Force Refresh Rerun
                          </Button>
                        </div>
                      </div>
                      <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                        <input
                          value={draft.pauseReason}
                          onChange={(event) =>
                            updateScheduleDraft(schedule.id, {
                              pauseReason: event.target.value,
                            })
                          }
                          placeholder="Pause reason"
                          className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                        />
                        <select
                          value={draft.snoozeHours}
                          onChange={(event) =>
                            updateScheduleDraft(schedule.id, {
                              snoozeHours: Number(event.target.value) || 4,
                            })
                          }
                          className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                        >
                          <option value={1}>Snooze 1 hour</option>
                          <option value={4}>Snooze 4 hours</option>
                          <option value={24}>Snooze 24 hours</option>
                        </select>
                        <div className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-500">
                          Ack: {formatTimestamp(schedule.alertAcknowledgedAt)} • Snooze:{" "}
                          {formatTimestamp(schedule.alertSnoozedUntil)}
                        </div>
                      </div>
                      <div className="mt-4 space-y-2">
                        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="text-slate-500">Successes</p>
                            <p className="mt-1 font-semibold text-slate-900">
                              {schedule.analytics.successfulRunCount}
                            </p>
                          </div>
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="text-slate-500">Failures</p>
                            <p className="mt-1 font-semibold text-slate-900">
                              {schedule.analytics.failedRunCount +
                                schedule.analytics.completedWithErrorsCount}
                            </p>
                          </div>
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="text-slate-500">Stale Runs</p>
                            <p className="mt-1 font-semibold text-slate-900">
                              {schedule.analytics.staleRunCount}
                            </p>
                          </div>
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="text-slate-500">Avg Run Duration</p>
                            <p className="mt-1 font-semibold text-slate-900">
                              {formatDuration(schedule.analytics.averageRunDurationMs)}
                            </p>
                          </div>
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="text-slate-500">Avg Export Ready</p>
                            <p className="mt-1 font-semibold text-slate-900">
                              {formatDuration(schedule.analytics.averageExportReadyLatencyMs)}
                            </p>
                          </div>
                        </div>
                        <div className="grid gap-3 md:grid-cols-2">
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="font-medium text-slate-900">Failure stages</p>
                            {schedule.analytics.sourceStageFailureBreakdown.length > 0 ? (
                              <div className="mt-2 space-y-1 text-slate-600">
                                {schedule.analytics.sourceStageFailureBreakdown.map((item) => (
                                  <div key={item.label}>
                                    {item.label}: {item.count}
                                  </div>
                                ))}
                              </div>
                            ) : (
                              <p className="mt-2 text-slate-500">No recent stage failures.</p>
                            )}
                          </div>
                          <div className="rounded-lg bg-white p-3 text-sm">
                            <p className="font-medium text-slate-900">Root causes</p>
                            {schedule.analytics.rootCauseSummary.length > 0 ? (
                              <div className="mt-2 space-y-1 text-slate-600">
                                {schedule.analytics.rootCauseSummary.map((item) => (
                                  <div key={item.label}>
                                    {item.label}: {item.count}
                                  </div>
                                ))}
                              </div>
                            ) : (
                              <p className="mt-2 text-slate-500">No recent root-cause data.</p>
                            )}
                          </div>
                        </div>
                        <div>
                          <h4 className="text-sm font-semibold text-slate-900">Recent scheduled runs</h4>
                          <p className="text-sm text-slate-500">
                            Recent scheduled runs for this schedule with status filtering.
                          </p>
                        </div>
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <select
                            value={runHistoryPage.statusFilter}
                            onChange={(event) =>
                              void loadScheduleRunHistory(schedule.id, {
                                offset: 0,
                                statusFilter: event.target
                                  .value as ImportScheduleRunHistoryStatusFilter,
                              })
                            }
                            className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                          >
                            <option value="all">All runs</option>
                            <option value="queued">Queued</option>
                            <option value="running">Running</option>
                            <option value="completed">Completed</option>
                            <option value="completed_with_errors">Completed with errors</option>
                            <option value="failed">Failed</option>
                            <option value="stale">Stale</option>
                          </select>
                          <p className="text-sm text-slate-500">
                            Showing {runHistoryPage.items.length} of {runHistoryPage.total}
                          </p>
                        </div>
                        {runHistoryPage.items.length === 0 ? (
                          <div className="rounded-lg border border-dashed border-slate-200 bg-white p-3 text-sm text-slate-500">
                            No scheduled runs yet.
                          </div>
                        ) : (
                          <div className="space-y-2">
                            {runHistoryPage.items.map((run) => (
                              <div
                                key={run.id}
                                className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-slate-200 bg-white p-3 text-sm"
                              >
                                <div className="space-y-1">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <span className="font-medium text-slate-900">{run.id}</span>
                                    <Badge variant={getRunStatusBadgeVariant(run)}>
                                      {run.isStale ? "stale" : run.status}
                                    </Badge>
                                    <Badge variant="info">{run.sourceStage}</Badge>
                                  </div>
                                  <p className="text-slate-500">
                                    Triggered {formatTimestamp(run.createdAt)} • processed{" "}
                                    {run.totalProcessed} / {run.totalFound}
                                  </p>
                                  <p className="text-slate-500">
                                    Duration {formatDuration(run.durationMs)} • export ready{" "}
                                    {formatDuration(run.exportReadyLatencyMs)}
                                  </p>
                                  {run.lastError ? (
                                    <p className="text-rose-700">{run.lastError}</p>
                                  ) : null}
                                </div>
                                <Button
                                  variant="outline"
                                  onClick={() => void loadImportRun(run.id)}
                                >
                                  Load Run
                                </Button>
                              </div>
                            ))}
                            {runHistoryPage.items.length < runHistoryPage.total ? (
                              <div className="flex justify-end">
                                <Button
                                  variant="outline"
                                  disabled={runHistoryLoading[schedule.id] === true}
                                  onClick={() =>
                                    void loadScheduleRunHistory(schedule.id, {
                                      append: true,
                                      offset: runHistoryPage.items.length,
                                      statusFilter: runHistoryPage.statusFilter,
                                    })
                                  }
                                >
                                  {runHistoryLoading[schedule.id] === true
                                    ? "Loading..."
                                    : "Load More Runs"}
                                </Button>
                              </div>
                            ) : null}
                          </div>
                        )}
                      </div>
                      <div className="mt-4 space-y-2">
                        <div>
                          <h4 className="text-sm font-semibold text-slate-900">Recent ops events</h4>
                          <p className="text-sm text-slate-500">
                            Audit trail for alerts, triggers, pauses, and remediation actions.
                          </p>
                        </div>
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div className="flex flex-wrap gap-2">
                            <select
                              value={opsEventPage.eventType}
                              onChange={(event) =>
                                void loadScheduleOpsEvents(schedule.id, {
                                  offset: 0,
                                  eventType: event.target.value,
                                  severity: opsEventPage.severity,
                                })
                              }
                              className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                            >
                              <option value="all">All event types</option>
                              <option value="trigger_attempted">Trigger attempted</option>
                              <option value="trigger_auth_failed">Trigger auth failed</option>
                              <option value="schedule_claimed">Schedule claimed</option>
                              <option value="schedule_skipped_overlap">Skipped overlap</option>
                              <option value="scheduled_run_created">Run created</option>
                              <option value="scheduled_run_succeeded">Run succeeded</option>
                              <option value="scheduled_run_failed">Run failed</option>
                              <option value="schedule_became_stale">Became stale</option>
                              <option value="alert_sent">Alert sent</option>
                              <option value="alert_failed">Alert failed</option>
                              <option value="alert_acknowledged">Alert acknowledged</option>
                              <option value="alert_snoozed">Alert snoozed</option>
                              <option value="alert_snooze_cleared">Snooze cleared</option>
                              <option value="schedule_paused">Schedule paused</option>
                              <option value="schedule_resumed">Schedule resumed</option>
                              <option value="operator_retry_failed_run">Retry failed run</option>
                              <option value="operator_force_refresh_rerun">Force rerun</option>
                              <option value="operator_run_now">Run now</option>
                            </select>
                            <select
                              value={opsEventPage.severity}
                              onChange={(event) =>
                                void loadScheduleOpsEvents(schedule.id, {
                                  offset: 0,
                                  eventType: opsEventPage.eventType,
                                  severity: event.target.value,
                                })
                              }
                              className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                            >
                              <option value="all">All severities</option>
                              <option value="info">Info</option>
                              <option value="warning">Warning</option>
                              <option value="error">Error</option>
                            </select>
                          </div>
                          <p className="text-sm text-slate-500">
                            Showing {opsEventPage.items.length} of {opsEventPage.total}
                          </p>
                        </div>
                        {opsEventPage.items.length === 0 ? (
                          <div className="rounded-lg border border-dashed border-slate-200 bg-white p-3 text-sm text-slate-500">
                            No ops events yet.
                          </div>
                        ) : (
                          <div className="space-y-2">
                            {opsEventPage.items.map((event) => (
                              <div
                                key={event.id}
                                className="rounded-lg border border-slate-200 bg-white p-3 text-sm"
                              >
                                <div className="flex flex-wrap items-center gap-2">
                                  <Badge
                                    variant={
                                      event.severity === "error"
                                        ? "destructive"
                                        : event.severity === "warning"
                                          ? "warning"
                                          : "info"
                                    }
                                  >
                                    {event.severity}
                                  </Badge>
                                  <Badge variant="default">{event.eventType}</Badge>
                                  <span className="text-slate-500">
                                    {formatTimestamp(event.createdAt)} • {event.source}
                                  </span>
                                </div>
                                <p className="mt-2 text-slate-900">{event.message}</p>
                              </div>
                            ))}
                            {opsEventPage.items.length < opsEventPage.total ? (
                              <div className="flex justify-end">
                                <Button
                                  variant="outline"
                                  disabled={opsEventLoading[schedule.id] === true}
                                  onClick={() =>
                                    void loadScheduleOpsEvents(schedule.id, {
                                      append: true,
                                      offset: opsEventPage.items.length,
                                      eventType: opsEventPage.eventType,
                                      severity: opsEventPage.severity,
                                    })
                                  }
                                >
                                  {opsEventLoading[schedule.id] === true
                                    ? "Loading..."
                                    : "Load More Events"}
                                </Button>
                              </div>
                            ) : null}
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })
              )}
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

            {isRingbaRecentRun ? (
              <div className="space-y-3 rounded-lg border border-slate-200 bg-slate-50 p-4">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <h3 className="text-sm font-semibold text-slate-900">
                      Ringba source progress
                    </h3>
                    <p className="text-sm text-slate-500">
                      {getSourceStageLabel(currentSourceStage)}
                    </p>
                  </div>
                  <Badge variant={isTerminal ? "default" : "warning"}>
                    {getSourceStageLabel(currentSourceStage)}
                  </Badge>
                </div>
                <div className="flex flex-wrap gap-2">
                  {ringbaRecentImportStages.map((stage, index) => {
                    const isActive = index === currentSourceStageIndex;
                    const isComplete =
                      currentSourceStageIndex >= 0 && index < currentSourceStageIndex;

                    return (
                      <Badge
                        key={stage}
                        variant={isActive || isComplete ? "default" : "info"}
                      >
                        {getSourceStageLabel(stage)}
                      </Badge>
                    );
                  })}
                </div>
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Window Start</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {activeRun.sourceWindowStart ?? "Not set"}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Window End</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {activeRun.sourceWindowEnd ?? "Not set"}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Export Job ID</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {activeRun.exportJobId ?? "Pending"}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Export Rows</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {activeRun.exportRowCount}
                    </p>
                  </div>
                </div>
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Poll Count</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.exportPollCount ?? 0}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Ready Latency</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {typeof ringbaDiagnostics.exportReadyLatencyMs === "number"
                        ? `${ringbaDiagnostics.exportReadyLatencyMs} ms`
                        : "Pending"}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Download Size</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {typeof ringbaDiagnostics.downloadSizeBytes === "number"
                        ? `${ringbaDiagnostics.downloadSizeBytes} bytes`
                        : "Pending"}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Download Status</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {activeRun.exportDownloadStatus ?? "Pending"}
                    </p>
                  </div>
                </div>
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Parsed Rows</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.parsedRowCount ?? 0}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Extracted Bid IDs</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.extractedBidIdCount ?? 0}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Deduped Bid IDs</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.dedupedBidIdCount ?? 0}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Inserted Items</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.insertedItemCount ?? 0}
                    </p>
                  </div>
                </div>
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Duplicates Removed</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.duplicateBidIdsRemoved ?? 0}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Invalid Bid IDs</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.invalidBidIdCount ?? 0}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Checkpoint</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.checkpointBidDt ?? "None"}
                    </p>
                  </div>
                  <div className="rounded-lg bg-white p-3 text-sm">
                    <p className="text-slate-500">Export File</p>
                    <p className="mt-1 font-medium text-slate-900">
                      {ringbaDiagnostics.exportFileName ?? "Pending"}
                    </p>
                  </div>
                </div>
                {ringbaDiagnostics.sampleBidIds && ringbaDiagnostics.sampleBidIds.length > 0 ? (
                  <div className="space-y-2">
                    <p className="text-sm font-medium text-slate-900">Sample Bid IDs</p>
                    <div className="flex flex-wrap gap-2">
                      {ringbaDiagnostics.sampleBidIds.map((bidId) => (
                        <Badge key={bidId} variant="info">
                          {bidId}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ) : null}
                {ringbaDiagnostics.sourceStageError ? (
                  <div className="rounded-lg border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
                    {ringbaDiagnostics.failedStage
                      ? `${getSourceStageLabel(ringbaDiagnostics.failedStage)}: `
                      : null}
                    {ringbaDiagnostics.sourceStageError}
                  </div>
                ) : null}
              </div>
            ) : null}

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
