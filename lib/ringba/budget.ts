import "server-only";

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

function getBackfillBudgetConfig(): RingbaBudgetConfig {
  const jitterMsMin = readPositiveIntEnv("RINGBA_BACKFILL_JITTER_MS_MIN", 250);
  const jitterMsMax = readPositiveIntEnv("RINGBA_BACKFILL_JITTER_MS_MAX", 1000);

  return {
    concurrency: readPositiveIntEnv("RINGBA_BACKFILL_CONCURRENCY", 1),
    requestsPerMinute: readPositiveIntEnv("RINGBA_BACKFILL_REQUESTS_PER_MINUTE", 30),
    jitterMsMin,
    jitterMsMax: Math.max(jitterMsMin, jitterMsMax),
  };
}

export async function withHistoricalRingbaBudget<T>(operation: () => Promise<T>) {
  const config = getBackfillBudgetConfig();
  await acquireConcurrency(config.concurrency);

  try {
    await reserveRequestWindow(config);
    return await operation();
  } finally {
    releaseConcurrency();
  }
}
