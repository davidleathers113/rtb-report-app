export const runtime = "nodejs";

import { BulkInvestigationClient } from "@/components/investigations/bulk-investigation-client";
import { InvestigationsListClient } from "@/components/investigations/investigations-list-client";
import { AppShell } from "@/components/layout/app-shell";
import { getInvestigations } from "@/lib/db/investigations";
import { getImportSchedules } from "@/lib/import-schedules/service";

export default async function InvestigationsPage() {
  const initialInvestigations =
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
        <div>
          <h2 className="text-3xl font-semibold text-slate-900 italic">
            Bid Investigations
          </h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
            Research specific Bid IDs or review entire batches from recent CSV uploads and Ringba API synchronizations.
          </p>
        </div>

        <BulkInvestigationClient
          initialSchedules={importSchedules}
          canManualTriggerSchedules={canManualTriggerSchedules}
        />

        <InvestigationsListClient initialData={initialInvestigations} />
      </div>
    </AppShell>
  );
}
