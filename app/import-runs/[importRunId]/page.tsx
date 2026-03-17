export const runtime = "nodejs";

import { notFound } from "next/navigation";
import { ImportRunDetailClient } from "@/components/investigations/import-run-detail-client";
import { AppShell } from "@/components/layout/app-shell";
import { getImportRunDetail } from "@/lib/db/import-runs";

export default async function ImportRunDetailPage({
  params,
}: {
  params: Promise<{ importRunId: string }>;
}) {
  const { importRunId } = await params;
  const run = await getImportRunDetail(importRunId, { itemLimit: 100 });

  if (!run) {
    notFound();
  }

  return (
    <AppShell currentPath="/import-runs">
      <ImportRunDetailClient initialRun={run} />
    </AppShell>
  );
}
