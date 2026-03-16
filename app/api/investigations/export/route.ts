export const runtime = "nodejs";

import { getInvestigationsForExport } from "@/lib/db/investigations";
import { buildCsv } from "@/lib/export/csv";
import { investigationsQuerySchema } from "@/lib/validation/investigations";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const parsed = investigationsQuerySchema
      .pick({
        rootCause: true,
        ownerType: true,
        search: true,
      })
      .parse({
      rootCause: searchParams.get("rootCause") ?? undefined,
      ownerType: searchParams.get("ownerType") ?? undefined,
      search: searchParams.get("search") ?? undefined,
      });

    const rows = await getInvestigationsForExport(parsed);
    const csv = buildCsv(
      rows.map((row) => ({
        bid_id: row.bidId,
        bid_dt: row.bidDt,
        campaign_name: row.campaignName,
        publisher_name: row.publisherName,
        target_name: row.targetName,
        bid_amount: row.bidAmount,
        http_status_code: row.httpStatusCode,
        root_cause: row.rootCause,
        owner_type: row.ownerType,
        outcome: row.outcome,
        outcome_reason_category: row.outcomeReasonCategory,
        outcome_reason_code: row.outcomeReasonCode,
        outcome_reason_message: row.outcomeReasonMessage,
        classification_source: row.classificationSource,
        classification_confidence: row.classificationConfidence,
        explanation: row.explanation,
        suggested_fix: row.suggestedFix,
        response_body: row.responseBody,
      })),
      [
        "bid_id",
        "bid_dt",
        "campaign_name",
        "publisher_name",
        "target_name",
        "bid_amount",
        "http_status_code",
        "root_cause",
        "owner_type",
        "outcome",
        "outcome_reason_category",
        "outcome_reason_code",
        "outcome_reason_message",
        "classification_source",
        "classification_confidence",
        "explanation",
        "suggested_fix",
        "response_body",
      ],
    );

    return new Response(csv, {
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition":
          'attachment; filename="bid-investigations-export.csv"',
      },
    });
  } catch (error) {
    return Response.json(
      {
        error:
          error instanceof Error ? error.message : "Unable to export investigations.",
      },
      { status: 400 },
    );
  }
}
