export const runtime = "nodejs";

import { ImportRunList } from "@/components/investigations/import-run-list";
import { StalledDirectImportRunsCard } from "@/components/investigations/stalled-direct-import-runs-card";
import { AppShell } from "@/components/layout/app-shell";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { getImportRuns } from "@/lib/db/import-runs";

export default async function ImportRunsPage() {
  const emptyResult = { items: [], total: 0, page: 1, pageSize: 50 };
  const result = await getImportRuns({
    page: 1,
    pageSize: 50,
  }).catch(() => emptyResult);
  const stalledDirectCsvRuns = await getImportRuns({
    page: 1,
    pageSize: 50,
    sourceType: "csv_direct_import",
    stalledDirectCsvOnly: true,
  }).catch(() => emptyResult);

  return (
    <AppShell currentPath="/import-runs">
      <div className="space-y-8">
        <div>
          <h2 className="text-3xl font-semibold text-slate-900">Import History</h2>
          <p className="mt-2 text-sm leading-6 text-slate-600 max-w-3xl">
            Review all bulk data ingestion attempts, track background processing for large files, and diagnose run-level failures.
          </p>
        </div>

        <StalledDirectImportRunsCard initialRuns={stalledDirectCsvRuns.items} />

        <Card>
          <CardHeader>
            <CardTitle>Recent Import Runs</CardTitle>
            <CardDescription>
              A history of manual uploads, Ringba API fetches, and scheduled synchronizations.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ImportRunList items={result.items} />
          </CardContent>
        </Card>
      </div>
    </AppShell>
  );
}
