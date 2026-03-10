import { and, asc, desc, eq, gt, inArray, isNull, like, lte, or, sql } from "drizzle-orm";

export { and, asc, desc, eq, gt, inArray, isNull, like, lte, or, sql };

export function createId() {
  return crypto.randomUUID();
}

export function nowIso() {
  return new Date().toISOString();
}

export function addSeconds(isoTimestamp: string, seconds: number) {
  return new Date(Date.parse(isoTimestamp) + seconds * 1000).toISOString();
}

export function addMinutes(isoTimestamp: string, minutes: number) {
  return new Date(Date.parse(isoTimestamp) + minutes * 60 * 1000).toISOString();
}

export function toTimestamp(value: string | null | undefined) {
  if (!value) {
    return null;
  }

  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? null : timestamp;
}

export function isLeaseActive(expiresAt: string | null | undefined, referenceIso: string) {
  const expiresAtMs = toTimestamp(expiresAt);
  const referenceMs = toTimestamp(referenceIso);
  if (expiresAtMs === null || referenceMs === null) {
    return false;
  }

  return expiresAtMs > referenceMs;
}

export function isLikePatternSafe(value: string) {
  return value.split("%").join("").split("_").join("").trim();
}
