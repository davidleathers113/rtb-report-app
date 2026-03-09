import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { JsonView } from "@/components/shared/json-view";
import type { InvestigationDetail } from "@/types/bid";
import {
  formatCurrency,
  formatDateTime,
  toSentenceCase,
} from "@/lib/utils";

function severityVariant(severity: InvestigationDetail["severity"]) {
  if (severity === "critical" || severity === "high") {
    return "destructive";
  }

  if (severity === "medium") {
    return "warning";
  }

  return "success";
}

function fetchStatusVariant(fetchStatus: InvestigationDetail["fetchStatus"]) {
  if (fetchStatus === "fetched") {
    return "success";
  }

  if (fetchStatus === "failed") {
    return "destructive";
  }

  return "warning";
}

export function BidDetailView({
  investigation,
}: {
  investigation: InvestigationDetail;
}) {
  return (
    <div className="space-y-6">
      <div className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Overview</CardTitle>
            <CardDescription>
              Bid `{investigation.bidId}` investigated on{" "}
              {formatDateTime(investigation.importedAt)}.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              <div>
                <dt className="text-sm text-slate-500">Bid Time</dt>
                <dd className="font-medium text-slate-900">
                  {formatDateTime(investigation.bidDt)}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Campaign</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.campaignName ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Publisher</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.publisherName ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Target</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.targetName ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Buyer</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.buyerName ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Bid Amount</dt>
                <dd className="font-medium text-slate-900">
                  {formatCurrency(investigation.bidAmount)}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Winning Bid</dt>
                <dd className="font-medium text-slate-900">
                  {formatCurrency(investigation.winningBid)}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">HTTP Status</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.httpStatusCode ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Reject Reason</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.reasonForReject ?? "-"}
                </dd>
              </div>
            </dl>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Diagnosis</CardTitle>
            <CardDescription>
              Plain-English explanation with owner and suggested next step.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap gap-2">
              <Badge variant="info">
                {toSentenceCase(investigation.rootCause)}
              </Badge>
              <Badge variant={fetchStatusVariant(investigation.fetchStatus)}>
                {toSentenceCase(investigation.fetchStatus)}
              </Badge>
              <Badge variant={severityVariant(investigation.severity)}>
                {toSentenceCase(investigation.severity)}
              </Badge>
              <Badge variant="default">
                {toSentenceCase(investigation.ownerType)}
              </Badge>
            </div>
            <p className="text-sm leading-6 text-slate-700">
              {investigation.explanation}
            </p>
            <div>
              <p className="text-sm font-medium text-slate-900">Suggested Fix</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.suggestedFix}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Confidence</p>
              <p className="mt-1 text-sm text-slate-600">
                {Math.round(investigation.confidence * 100)}%
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Fetch Lifecycle</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                Started {formatDateTime(investigation.fetchStartedAt)}. Last fetched{" "}
                {formatDateTime(investigation.fetchedAt)}. Attempts{" "}
                {investigation.fetchAttemptCount}.
              </p>
              {investigation.lastError ? (
                <p className="mt-2 text-sm leading-6 text-rose-600">
                  Last error: {investigation.lastError}
                </p>
              ) : null}
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Evidence</p>
              <ul className="mt-2 space-y-2 text-sm text-slate-600">
                {investigation.evidence.map((item, index) => (
                  <li key={`${item.field}-${index}`} className="rounded-lg bg-slate-50 p-3">
                    <span className="font-medium text-slate-900">{item.field}:</span>{" "}
                    {String(item.value ?? "-")} - {item.description}
                  </li>
                ))}
              </ul>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Request Body</CardTitle>
          </CardHeader>
          <CardContent>
            <JsonView value={investigation.requestBody} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Response Body</CardTitle>
          </CardHeader>
          <CardContent>
            <JsonView value={investigation.responseBody} />
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Event Timeline</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {investigation.events.length === 0 ? (
            <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
              No event timeline was present in the Ringba response.
            </div>
          ) : (
            investigation.events.map((event) => (
              <div
                key={event.id ?? `${event.eventName}-${event.eventTimestamp}`}
                className="rounded-lg border border-slate-200 p-4"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="font-medium text-slate-900">{event.eventName}</p>
                  <p className="text-sm text-slate-500">
                    {formatDateTime(event.eventTimestamp)}
                  </p>
                </div>
                <div className="mt-3">
                  <JsonView
                    value={event.eventValsJson ?? event.eventStrValsJson}
                    emptyLabel="No event payload available."
                  />
                </div>
              </div>
            ))
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Raw Trace JSON</CardTitle>
        </CardHeader>
        <CardContent>
          <JsonView value={investigation.rawTraceJson} />
        </CardContent>
      </Card>
    </div>
  );
}
