import "server-only";

import type { RingbaBudgetProfileName } from "@/types/import-run";

function sleep(milliseconds: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function readPositiveIntEnv(name: string, fallback: number) {
  const rawValue = process.env[name]?.trim();
  if (!rawValue) {
    return fallback;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }

  return Math.trunc(parsed);
}

function randomBetween(minimum: number, maximum: number) {
  if (maximum <= minimum) {
    return minimum;
  }

  return minimum + Math.floor(Math.random() * (maximum - minimum + 1));
}

interface RingbaBudgetConfig {
  concurrency: number;
  requestsPerMinute: number;
  jitterMsMin: number;
  jitterMsMax: number;
}

interface RingbaBudgetProfileConfig {
  profileName: RingbaBudgetProfileName;
  concurrencyEnvName: string;
  requestsPerMinuteEnvName: string;
  jitterMinEnvName: string;
  jitterMaxEnvName: string;
  defaultConcurrency: number;
  defaultRequestsPerMinute: number;
  defaultJitterMsMin: number;
  defaultJitterMsMax: number;
}

let activeCount = 0;
const concurrencyWaiters: Array<() => void> = [];
let nextRequestWindowMs = 0;
let scheduleTail = Promise.resolve();

async function acquireConcurrency(maxConcurrency: number) {
  if (activeCount < maxConcurrency) {
    activeCount += 1;
    return;
  }

  await new Promise<void>((resolve) => {
    concurrencyWaiters.push(() => {
      activeCount += 1;
      resolve();
    });
  });
}

function releaseConcurrency() {
  activeCount = Math.max(0, activeCount - 1);
  const nextWaiter = concurrencyWaiters.shift();
  if (nextWaiter) {
    nextWaiter();
  }
}

async function reserveRequestWindow(config: RingbaBudgetConfig) {
  let releaseReservation: () => void = () => undefined;
  const previousReservation = scheduleTail;
  scheduleTail = new Promise<void>((resolve) => {
    releaseReservation = resolve;
  });

  await previousReservation;

  try {
    const spacingMs = Math.max(1, Math.ceil(60000 / Math.max(config.requestsPerMinute, 1)));
    const now = Date.now();
    const scheduledAt = Math.max(now, nextRequestWindowMs);
    nextRequestWindowMs = scheduledAt + spacingMs;

    const jitterMs = randomBetween(config.jitterMsMin, config.jitterMsMax);
    const waitMs = Math.max(0, scheduledAt - now) + jitterMs;
    if (waitMs > 0) {
      await sleep(waitMs);
    }
  } finally {
    releaseReservation();
  }
}

const RINGBA_BUDGET_PROFILES: Record<RingbaBudgetProfileName, RingbaBudgetProfileConfig> = {
  historical_backfill: {
    profileName: "historical_backfill",
    concurrencyEnvName: "RINGBA_BACKFILL_CONCURRENCY",
    requestsPerMinuteEnvName: "RINGBA_BACKFILL_REQUESTS_PER_MINUTE",
    jitterMinEnvName: "RINGBA_BACKFILL_JITTER_MS_MIN",
    jitterMaxEnvName: "RINGBA_BACKFILL_JITTER_MS_MAX",
    defaultConcurrency: 1,
    defaultRequestsPerMinute: 30,
    defaultJitterMsMin: 250,
    defaultJitterMsMax: 1000,
  },
  direct_csv_bulk: {
    profileName: "direct_csv_bulk",
    concurrencyEnvName: "RINGBA_DIRECT_CSV_CONCURRENCY",
    requestsPerMinuteEnvName: "RINGBA_DIRECT_CSV_REQUESTS_PER_MINUTE",
    jitterMinEnvName: "RINGBA_DIRECT_CSV_JITTER_MS_MIN",
    jitterMaxEnvName: "RINGBA_DIRECT_CSV_JITTER_MS_MAX",
    defaultConcurrency: 1,
    defaultRequestsPerMinute: 100,
    defaultJitterMsMin: 250,
    defaultJitterMsMax: 1000,
  },
};

function getBudgetConfig(profileName: RingbaBudgetProfileName): RingbaBudgetConfig {
  const profile = RINGBA_BUDGET_PROFILES[profileName];
  const jitterMsMin = readPositiveIntEnv(profile.jitterMinEnvName, profile.defaultJitterMsMin);
  const jitterMsMax = readPositiveIntEnv(profile.jitterMaxEnvName, profile.defaultJitterMsMax);

  return {
    concurrency: readPositiveIntEnv(profile.concurrencyEnvName, profile.defaultConcurrency),
    requestsPerMinute: readPositiveIntEnv(
      profile.requestsPerMinuteEnvName,
      profile.defaultRequestsPerMinute,
    ),
    jitterMsMin,
    jitterMsMax: Math.max(jitterMsMin, jitterMsMax),
  };
}

export async function withRingbaBudget<T>(
  profileName: RingbaBudgetProfileName,
  operation: () => Promise<T>,
) {
  const config = getBudgetConfig(profileName);
  await acquireConcurrency(config.concurrency);

  try {
    await reserveRequestWindow(config);
    return await operation();
  } finally {
    releaseConcurrency();
  }
}

export async function withHistoricalRingbaBudget<T>(operation: () => Promise<T>) {
  return withRingbaBudget("historical_backfill", operation);
}
