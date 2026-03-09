import { describe, expect, it } from "vitest";

import { parseBidIds } from "@/lib/utils/bid-input";

describe("parseBidIds", () => {
  it("supports comma and newline separated input with dedupe", () => {
    expect(parseBidIds("abc,def\nabc ghi")).toEqual(["abc", "def", "ghi"]);
  });
});
