export const runtime = "nodejs";

import Link from "next/link";

import { BidDetailView } from "@/components/investigations/bid-detail-view";
import { RefreshInvestigationButton } from "@/components/investigations/refresh-investigation-button";
import { AppShell } from "@/components/layout/app-shell";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getInvestigationByBidId } from "@/lib/db/investigations";

export default async function InvestigationDetailPage({
  params,
}: {
  params: Promise<{ bidId: string }>;
}) {
  const { bidId } = await params;
  const investigation = await getInvestigationByBidId(
    decodeURIComponent(bidId),
  ).catch(() => null);

  if (!investigation) {
    return (
      <AppShell currentPath="/investigations">
        <Card>
          <CardHeader>
            <CardTitle>Investigation Unavailable</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm leading-6 text-slate-600">
              This bid has not been investigated yet, or the stored investigation
              is currently unavailable.
            </p>
            <Button asChild variant="outline">
              <Link href="/investigations">Back To Bulk Investigation</Link>
            </Button>
          </CardContent>
        </Card>
      </AppShell>
    );
  }

  return (
    <AppShell currentPath="/investigations">
      <div className="mb-6 flex items-center justify-between gap-4">
        <div>
          <h2 className="text-3xl font-semibold text-slate-900">
            Investigation Detail
          </h2>
          <p className="mt-2 text-sm text-slate-600">
            Deep-dive debugging view for bid `{investigation.bidId}`.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button asChild variant="outline">
            <Link href="/investigations">Back To Bulk Investigation</Link>
          </Button>
          <RefreshInvestigationButton bidId={investigation.bidId} />
        </div>
      </div>
      <BidDetailView investigation={investigation} />
    </AppShell>
  );
}
