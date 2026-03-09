import { getInvestigationsForExport } from "@/lib/db/investigations";
import { buildCsv } from "@/lib/export/csv";
import { investigationsQuerySchema } from "@/lib/validation/investigations";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const parsed = investigationsQuerySchema.parse({
      page: 1,
      pageSize: 1000,
      rootCause: searchParams.get("rootCause") ?? undefined,
      ownerType: searchParams.get("ownerType") ?? undefined,
      search: searchParams.get("search") ?? undefined,
    });

    const rows = await getInvestigationsForExport(parsed);
    const csv = buildCsv(
      rows.map((row) => ({
        bid_id: row.bid_id,
        bid_dt: row.bid_dt,
        campaign_name: row.campaign_name,
        publisher_name: row.publisher_name,
        target_name: row.target_name,
        bid_amount: row.bid_amount,
        http_status_code: row.http_status_code,
        root_cause: row.root_cause,
        owner_type: row.owner_type,
        explanation: row.explanation,
        suggested_fix: row.suggested_fix,
        response_body: row.response_body,
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
