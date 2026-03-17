"use client";

import { useState, useMemo } from "react";
import Link from "next/link";
import { DashboardChartsPanel } from "@/components/dashboard/dashboard-charts-panel";
import { InvestigationTable } from "@/components/investigations/investigation-table";
import { InvestigationFilterBar, FilterValues } from "@/components/investigations/investigation-filter-bar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCw, Download, Plus } from "lucide-react";
import { formatNumber, toSentenceCase } from "@/lib/utils";
import type { DashboardStats } from "@/types/bid";

function MetricCard({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone?: "default" | "warning" | "destructive" | "success";
}) {
  const toneClass =
    tone === "warning"
      ? "bg-amber-50 text-amber-700 border-amber-100"
      : tone === "destructive"
        ? "bg-rose-50 text-rose-700 border-rose-100"
        : tone === "success"
          ? "bg-emerald-50 text-emerald-700 border-emerald-100"
          : "bg-sky-50 text-sky-700 border-sky-100";

  return (
    <Card className={`${toneClass} shadow-sm`}>
      <CardHeader className="pb-2">
        <CardDescription className="text-current/80 font-medium">{label}</CardDescription>
      </CardHeader>
      <CardContent>
        <p className="text-3xl font-bold">{formatNumber(value)}</p>
      </CardContent>
    </Card>
  );
}

function TopList({
  title,
  items,
}: {
  title: string;
  items: Array<{ label: string; value: number }>;
}) {
  return (
    <Card className="shadow-sm">
      <CardHeader>
        <CardTitle className="text-lg">{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {items.length === 0 ? (
          <p className="text-sm text-slate-500 py-4 text-center italic">No data matching filters.</p>
        ) : (
          items.map((item) => (
            <div
              key={item.label}
              className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2 border border-slate-100"
            >
              <span className="text-sm text-slate-700 truncate mr-2" title={item.label}>
                {toSentenceCase(item.label)}
              </span>
              <Badge variant="default" className="font-mono bg-white border border-slate-200">
                {formatNumber(item.value)}
              </Badge>
            </div>
          ))
        )}
      </CardContent>
    </Card>
  );
}

export function DashboardClient({ initialStats }: { initialStats: DashboardStats }) {
  const [stats, setStats] = useState<DashboardStats>(initialStats);
  const [filters, setFilters] = useState<FilterValues>({});
  const [isLoading, setIsLoading] = useState(false);

  const fetchStats = async (currentFilters: FilterValues) => {
    setIsLoading(true);
    try {
      const params = new URLSearchParams();
      Object.entries(currentFilters).forEach(([key, value]) => {
        if (value) params.append(key, value);
      });

      const response = await fetch(`/api/investigations/stats?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to fetch stats");
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error("Error fetching dashboard stats:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleFilterChange = (newFilters: FilterValues) => {
    setFilters(newFilters);
    void fetchStats(newFilters);
  };

  const exportUrl = useMemo(() => {
    const params = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) params.append(key, value);
    });
    return `/api/investigations/export?${params.toString()}`;
  }, [filters]);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h2 className="text-3xl font-bold text-slate-900 tracking-tight italic">Investigation Dashboard</h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
            Monitor processed Ringba bid traces, review root causes, and jump directly into individual investigations.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {isLoading && <RefreshCw className="h-4 w-4 animate-spin text-slate-400 mr-2" />}
          <Button asChild variant="outline" size="sm">
            <Link href={exportUrl}>
              <Download className="mr-2 h-4 w-4" />
              Export CSV
            </Link>
          </Button>
          <Button asChild size="sm">
            <Link href="/investigations">
              <Plus className="mr-2 h-4 w-4" />
              Investigate New
            </Link>
          </Button>
        </div>
      </div>

      <InvestigationFilterBar onFilterChange={handleFilterChange} initialFilters={filters} />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Total Investigated" value={stats.totalInvestigated} />
        <MetricCard label="Accepted" value={stats.acceptedCount} tone="success" />
        <MetricCard label="Rejected" value={stats.rejectedCount} tone="destructive" />
        <MetricCard label="Zero Bid" value={stats.zeroBidCount} tone="warning" />
      </div>

      <DashboardChartsPanel
        errorsByCategory={stats.errorsByCategory}
        bidsByOutcome={stats.bidsByOutcome}
        issuesOverTime={stats.issuesOverTime}
      />

      <div className="grid gap-6 xl:grid-cols-3">
        <TopList title="Top Root Causes" items={stats.topRootCauses} />
        <TopList title="Most Affected Campaigns" items={stats.topCampaigns} />
        <TopList title="Most Affected Publishers" items={stats.topPublishers} />
      </div>

      <div className="grid gap-6 xl:grid-cols-3">
        <TopList title="Most Affected Targets" items={stats.topTargets} />
        <Card className="xl:col-span-2 shadow-sm">
          <CardHeader>
            <CardTitle className="text-lg">Recent Investigations</CardTitle>
            <CardDescription>
              Latest persisted bid investigations matching your current filters.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <InvestigationTable items={stats.recentInvestigations} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
