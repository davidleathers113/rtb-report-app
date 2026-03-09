export function parseBidIds(rawInput: string) {
  const values: string[] = [];
  let current = "";

  for (const character of rawInput) {
    const isSeparator =
      character === "," ||
      character === "\n" ||
      character === "\r" ||
      character === "\t" ||
      character === " ";

    if (isSeparator) {
      const trimmed = current.trim();
      if (trimmed) {
        values.push(trimmed);
      }
      current = "";
      continue;
    }

    current += character;
  }

  const trailingValue = current.trim();
  if (trailingValue) {
    values.push(trailingValue);
  }

  const deduped: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    deduped.push(value);
  }

  return deduped;
}
