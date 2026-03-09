import Link from "next/link";

import { DashboardChartsPanel } from "@/components/dashboard/dashboard-charts-panel";
import { InvestigationTable } from "@/components/investigations/investigation-table";
import { AppShell } from "@/components/layout/app-shell";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { getDashboardStats } from "@/lib/db/investigations";
import { formatNumber, toSentenceCase } from "@/lib/utils";

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
      ? "bg-amber-50 text-amber-700"
      : tone === "destructive"
        ? "bg-rose-50 text-rose-700"
        : tone === "success"
          ? "bg-emerald-50 text-emerald-700"
          : "bg-sky-50 text-sky-700";

  return (
    <Card className={toneClass}>
      <CardHeader className="pb-2">
        <CardDescription className="text-current/80">{label}</CardDescription>
      </CardHeader>
      <CardContent>
        <p className="text-3xl font-semibold">{formatNumber(value)}</p>
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
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {items.length === 0 ? (
          <p className="text-sm text-slate-500">No data yet.</p>
        ) : (
          items.map((item) => (
            <div
              key={item.label}
              className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2"
            >
              <span className="text-sm text-slate-700">
                {toSentenceCase(item.label)}
              </span>
              <Badge variant="default">{formatNumber(item.value)}</Badge>
            </div>
          ))
        )}
      </CardContent>
    </Card>
  );
}

export default async function Home() {
  const stats =
    (await getDashboardStats().catch(() => null)) ?? {
      totalInvestigated: 0,
      acceptedCount: 0,
      rejectedCount: 0,
      zeroBidCount: 0,
      topRootCauses: [],
      topCampaigns: [],
      topPublishers: [],
      topTargets: [],
      errorsByCategory: [],
      bidsByOutcome: [],
      issuesOverTime: [],
      recentInvestigations: [],
    };

  return (
    <AppShell currentPath="/">
      <div className="space-y-8">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h2 className="text-3xl font-semibold text-slate-900">
              Investigation Dashboard
            </h2>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
              Monitor processed Ringba bid traces, review root causes, and jump
              directly into individual investigations without working in spreadsheets.
            </p>
          </div>
          <div className="flex gap-2">
            <Button asChild variant="outline">
              <Link href="/api/investigations/export">Export CSV</Link>
            </Button>
            <Button asChild>
              <Link href="/investigations">Investigate New Bids</Link>
            </Button>
          </div>
        </div>

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
          <Card className="xl:col-span-2">
            <CardHeader>
              <CardTitle>Recent Investigations</CardTitle>
              <CardDescription>
                Latest persisted bid investigations with diagnosis summaries.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <InvestigationTable items={stats.recentInvestigations} />
            </CardContent>
          </Card>
        </div>
      </div>
    </AppShell>
  );
}
