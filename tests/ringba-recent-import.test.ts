import { describe, expect, it } from "vitest";
import { zipSync } from "fflate";

import {
  extractCsvFromZip,
  parseRtbExportCsv,
} from "@/lib/import-runs/ringba-recent";

describe("Ringba recent import helpers", () => {
  it("extracts the CSV file from a Ringba export zip", () => {
    const csvText = "Bid ID,Bid Date\nRTB-1,03/09/2026 11:34:48 PM\n";
    const zipBytes = zipSync({
      "ringba-export.csv": new TextEncoder().encode(csvText),
    });

    const extracted = extractCsvFromZip(zipBytes);

    expect(extracted.fileName).toBe("ringba-export.csv");
    expect(extracted.csvText).toBe(csvText);
  });

  it("parses and dedupes bid ids from the RTB export CSV", () => {
    const result = parseRtbExportCsv(
      [
        "Bid ID,Bid Date,Campaign",
        "RTB-1,03/09/2026 11:34:48 PM,Alpha",
        "RTB-1,03/09/2026 11:34:49 PM,Alpha",
        "RTB-2,03/09/2026 11:35:48 PM,Beta",
      ].join("\n"),
    );

    expect(result.bidIds).toEqual(["RTB-1", "RTB-2"]);
    expect(result.duplicateCount).toBe(1);
    expect(result.rowCount).toBe(3);
    expect(result.latestBidDt).toBe("2026-03-09T23:35:48.000Z");
  });

  it("accepts alternate bid id header variants and BOM-prefixed csv text", () => {
    const result = parseRtbExportCsv(
      [
        "\uFEFFbid_id,bid_dt",
        " RTB-1 ,03/09/2026 11:34:48 PM",
        "RTB-2,03/09/2026 11:35:48 PM",
      ].join("\n"),
    );

    expect(result.bidIds).toEqual(["RTB-1", "RTB-2"]);
    expect(result.dedupedBidIdCount).toBe(2);
  });

  it("returns zero unique bid ids for duplicate-only empty values", () => {
    const result = parseRtbExportCsv(
      [
        "Bid ID,Bid Date",
        " ,03/09/2026 11:34:48 PM",
        " ,03/09/2026 11:35:48 PM",
      ].join("\n"),
    );

    expect(result.bidIds).toEqual([]);
    expect(result.rowCount).toBe(2);
  });
});
