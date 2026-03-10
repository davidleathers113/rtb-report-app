import Link from "next/link";

import { BulkInvestigationClient } from "@/components/investigations/bulk-investigation-client";
import { InvestigationTable } from "@/components/investigations/investigation-table";
import { AppShell } from "@/components/layout/app-shell";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { getInvestigations } from "@/lib/db/investigations";
import { getImportSchedules } from "@/lib/import-schedules/service";

export default async function InvestigationsPage() {
  const recentInvestigations =
    (await getInvestigations({
      page: 1,
      pageSize: 25,
    }).catch(() => null)) ?? {
      items: [],
      total: 0,
      page: 1,
      pageSize: 25,
    };
  const importSchedules = (await getImportSchedules().catch(() => [])) ?? [];
  const canManualTriggerSchedules =
    process.env.NODE_ENV !== "production" ||
    !(process.env.IMPORT_SCHEDULES_TRIGGER_SECRET?.trim());

  return (
    <AppShell currentPath="/investigations">
      <div className="space-y-8">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h2 className="text-3xl font-semibold text-slate-900">
              Bulk And Single Bid Investigation
            </h2>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
              Paste Bid IDs, create an async import run, and track progress while
              stored investigations are reused or fetched safely in the background.
            </p>
          </div>
          <Button asChild variant="outline">
            <Link href="/api/investigations/export">Export Current Results</Link>
          </Button>
        </div>

        <BulkInvestigationClient
          initialSchedules={importSchedules}
          canManualTriggerSchedules={canManualTriggerSchedules}
        />

        <Card>
          <CardHeader>
            <CardTitle>Recent Stored Investigations</CardTitle>
            <CardDescription>
              Persisted investigations from the database, ready for review and export.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <InvestigationTable items={recentInvestigations.items} />
          </CardContent>
        </Card>
      </div>
    </AppShell>
  );
}
