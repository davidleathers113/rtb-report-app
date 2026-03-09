"use client";

import dynamic from "next/dynamic";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { DashboardMetric, DashboardTimePoint } from "@/types/bid";

const DashboardCharts = dynamic(
  () =>
    import("@/components/dashboard/dashboard-charts").then(
      (module) => module.DashboardCharts,
    ),
  {
    ssr: false,
    loading: () => (
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Errors By Category</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80 rounded-lg bg-slate-100" />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Bids By Outcome</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80 rounded-lg bg-slate-100" />
          </CardContent>
        </Card>
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Issues Over Time</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80 rounded-lg bg-slate-100" />
          </CardContent>
        </Card>
      </div>
    ),
  },
);

export function DashboardChartsPanel(props: {
  errorsByCategory: DashboardMetric[];
  bidsByOutcome: DashboardMetric[];
  issuesOverTime: DashboardTimePoint[];
}) {
  return <DashboardCharts {...props} />;
}
