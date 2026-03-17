import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { JsonView } from "@/components/shared/json-view";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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

function parseStatusVariant(parseStatus: InvestigationDetail["parseStatus"]) {
  if (parseStatus === "complete") {
    return "success";
  }

  if (parseStatus === "text_fallback") {
    return "warning";
  }

  if (parseStatus === "partial" || parseStatus === "shape_unknown") {
    return "destructive";
  }

  return "default";
}

function failureStageVariant(stage: InvestigationDetail["primaryFailureStage"]) {
  if (stage === "accepted") {
    return "success";
  }

  if (stage === "zero_bid") {
    return "warning";
  }

  if (stage === "target_rejected" || stage === "fetch_failed") {
    return "destructive";
  }

  return "default";
}

function classificationVariant(category: InvestigationDetail["outcomeReasonCategory"]) {
  if (category === "accepted") {
    return "success";
  }

  if (
    category === "missing_caller_id" ||
    category === "missing_required_field" ||
    category === "request_invalid" ||
    category === "rate_limited"
  ) {
    return "destructive";
  }

  if (category) {
    return "warning";
  }

  return "default";
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
            <CardTitle>Failure Summary</CardTitle>
            <CardDescription>
              The highest-signal target, stage, and error extracted from the Ringba bid trace.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap gap-2">
              <Badge variant={investigation.outcome === "accepted" ? "success" : "default"}>
                {toSentenceCase(investigation.outcome)}
              </Badge>
              <Badge variant={failureStageVariant(investigation.primaryFailureStage)}>
                {toSentenceCase(investigation.primaryFailureStage)}
              </Badge>
              {investigation.outcomeReasonCategory ? (
                <Badge variant={classificationVariant(investigation.outcomeReasonCategory)}>
                  {toSentenceCase(investigation.outcomeReasonCategory)}
                </Badge>
              ) : null}
              {investigation.primaryErrorCode !== null ? (
                <Badge variant="destructive">Code {investigation.primaryErrorCode}</Badge>
              ) : null}
              <Badge variant={fetchStatusVariant(investigation.fetchStatus)}>
                {toSentenceCase(investigation.fetchStatus)}
              </Badge>
              <Badge variant={parseStatusVariant(investigation.parseStatus)}>
                Parse {toSentenceCase(investigation.parseStatus)}
              </Badge>
              {investigation.normalizationWarnings.length > 0 ? (
                <Badge variant="warning">
                  {investigation.normalizationWarnings.length} parser warning
                  {investigation.normalizationWarnings.length === 1 ? "" : "s"}
                </Badge>
              ) : null}
              <Badge variant={severityVariant(investigation.severity)}>
                {toSentenceCase(investigation.severity)}
              </Badge>
            </div>
            <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              <div>
                <dt className="text-sm text-slate-500">Primary Target</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.primaryTargetName ?? investigation.targetName ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Primary Buyer</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.primaryBuyerName ?? investigation.buyerName ?? "-"}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Bid Elapsed</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.bidElapsedMs ?? "-"}
                </dd>
              </div>
            </dl>
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-4">
              <p className="text-sm font-medium text-slate-900">Decisive Error</p>
              <p className="mt-2 text-sm leading-6 text-slate-700">
                {investigation.outcomeReasonMessage ??
                  investigation.primaryErrorMessage ??
                  investigation.errorMessage ??
                  investigation.reasonForReject ??
                  "No decisive error message was extracted."}
              </p>
            </div>
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
          </CardContent>
        </Card>
      </div>

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
                <dt className="text-sm text-slate-500">Bid Elapsed Ms</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.bidElapsedMs ?? "-"}
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
                <dt className="text-sm text-slate-500">Outcome Reason</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.outcomeReasonCategory
                    ? toSentenceCase(investigation.outcomeReasonCategory)
                    : "-"}
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
            <CardTitle>Fetch Lifecycle</CardTitle>
            <CardDescription>
              Trace freshness and investigation status.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <p className="text-sm font-medium text-slate-900">Started</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {formatDateTime(investigation.fetchStartedAt)}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Last Fetched</p>
              <p className="mt-1 text-sm text-slate-600">
                {formatDateTime(investigation.fetchedAt)}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Attempts</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.fetchAttemptCount}.
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Classification Source</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.classificationSource ?? "-"}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Classification Confidence</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.classificationConfidence === null
                  ? "-"
                  : `${Math.round(investigation.classificationConfidence * 100)}%`}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Parse Status</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {toSentenceCase(investigation.parseStatus)}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Parser Confidence</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.normalizationConfidence === null
                  ? "-"
                  : `${Math.round(investigation.normalizationConfidence * 100)}%`}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Schema Variant</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.schemaVariant ?? "-"}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-slate-900">Primary Error Source</p>
              <p className="mt-1 text-sm leading-6 text-slate-600">
                {investigation.primaryErrorCodeSource ?? "-"}
              </p>
            </div>
            {investigation.lastError ? (
              <p className="rounded-lg border border-rose-200 bg-rose-50 p-3 text-sm leading-6 text-rose-700">
                Last error: {investigation.lastError}
              </p>
            ) : null}
          </CardContent>
        </Card>
      </div>

      {investigation.sourceContext ? (
        <Card>
          <CardHeader>
            <CardTitle>Source Context</CardTitle>
            <CardDescription>
              Values preserved from the imported CSV source row for this bid.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-6 lg:grid-cols-2">
            <dl className="grid gap-4 sm:grid-cols-2">
              <div>
                <dt className="text-sm text-slate-500">Source File</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.sourceContext.fileName}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Row Number</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.sourceContext.rowNumber}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Source Bid Time</dt>
                <dd className="font-medium text-slate-900">
                  {formatDateTime(investigation.sourceContext.bidDt)}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-slate-500">Source Reject Reason</dt>
                <dd className="font-medium text-slate-900">
                  {investigation.sourceContext.reasonForReject ?? "-"}
                </dd>
              </div>
            </dl>
            <JsonView value={investigation.sourceContext.rowJson} />
          </CardContent>
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Target Attempts</CardTitle>
          <CardDescription>
            One row per ping target with the extracted request, response, and failure details.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {investigation.targetAttempts.length === 0 ? (
            <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
              No target-level attempts were extracted from the Ringba payload.
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>#</TableHead>
                  <TableHead>Target</TableHead>
                  <TableHead>Buyer</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Bid</TableHead>
                  <TableHead>HTTP</TableHead>
                  <TableHead>Error</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {investigation.targetAttempts.map((attempt) => (
                  <TableRow key={attempt.id ?? `${attempt.sequence}-${attempt.targetName}`}>
                    <TableCell>{attempt.sequence}</TableCell>
                    <TableCell>{attempt.targetName ?? "-"}</TableCell>
                    <TableCell>{attempt.targetBuyer ?? "-"}</TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-2">
                        <Badge variant={attempt.winning ? "success" : attempt.accepted ? "info" : "default"}>
                          {attempt.winning
                            ? "Winning"
                            : attempt.accepted
                              ? "Accepted"
                              : "Rejected"}
                        </Badge>
                        {attempt.errorCode !== null ? (
                          <Badge variant="destructive">Code {attempt.errorCode}</Badge>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell>{formatCurrency(attempt.bidAmount)}</TableCell>
                    <TableCell>{attempt.httpStatusCode ?? "-"}</TableCell>
                    <TableCell className="max-w-sm text-sm text-slate-600">
                      {attempt.errorMessage ??
                        attempt.rejectReason ??
                        attempt.summaryReason ??
                        attempt.errors[0] ??
                        "-"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Decisive Request Body</CardTitle>
          </CardHeader>
          <CardContent>
            <JsonView value={investigation.requestBody} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Decisive Response Body</CardTitle>
          </CardHeader>
          <CardContent>
            <JsonView value={investigation.responseBody} />
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Evidence</CardTitle>
        </CardHeader>
        <CardContent>
          <ul className="space-y-2 text-sm text-slate-600">
            {investigation.evidence.map((item, index) => (
              <li key={`${item.field}-${index}`} className="rounded-lg bg-slate-50 p-3">
                <span className="font-medium text-slate-900">{item.field}:</span>{" "}
                {String(item.value ?? "-")} - {item.description}
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Classification Warnings</CardTitle>
          <CardDescription>
            Conflicts or caveats captured while deriving the operator-facing classification.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {investigation.classificationWarnings.length === 0 ? (
            <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
              No classification warnings were recorded for this investigation.
            </div>
          ) : (
            <ul className="space-y-2 text-sm text-slate-600">
              {investigation.classificationWarnings.map((warning, index) => (
                <li
                  key={`${warning.code}-${warning.field ?? "none"}-${index}`}
                  className="rounded-lg bg-slate-50 p-3"
                >
                  <span className="font-medium text-slate-900">{warning.code}:</span>{" "}
                  {warning.message}
                  {warning.field ? ` (${warning.field})` : ""}
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Parser Warnings</CardTitle>
          <CardDescription>
            Drift and fallback signals captured during Ringba normalization.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {investigation.normalizationWarnings.length === 0 ? (
            <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
              No parser warnings were recorded for this investigation.
            </div>
          ) : (
            <ul className="space-y-2 text-sm text-slate-600">
              {investigation.normalizationWarnings.map((warning, index) => (
                <li
                  key={`${warning.code}-${warning.field ?? "none"}-${index}`}
                  className="rounded-lg bg-slate-50 p-3"
                >
                  <span className="font-medium text-slate-900">{warning.code}:</span>{" "}
                  {warning.message}
                  {warning.field ? ` (${warning.field})` : ""}
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>

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
