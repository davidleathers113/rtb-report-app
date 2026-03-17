export const runtime = "nodejs";

import { DashboardClient } from "@/components/dashboard/dashboard-client";
import { AppShell } from "@/components/layout/app-shell";
import { getDashboardStats } from "@/lib/db/investigations";

export default async function DashboardPage() {
  const stats = await getDashboardStats().catch(() => ({
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
  }));

  return (
    <AppShell currentPath="/">
      <DashboardClient initialStats={stats} />
    </AppShell>
  );
}
