import fs from "node:fs/promises";
import path from "node:path";

const API_BASE_URL = process.env.IMPORT_API_BASE_URL || "http://localhost:3000";
const CHUNK_ROW_LIMIT = 4000;
const PROCESS_BATCH_SIZE = 50;
const PROCESS_MAX_BATCHES = 10;
const PROCESS_RETRY_DELAY_MS = 1500;
const PROCESS_MAX_ATTEMPTS = 200;

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function splitLines(text) {
  const lines = [];
  let current = "";

  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];

    if (character === "\r") {
      if (text[index + 1] === "\n") {
        index += 1;
      }
      lines.push(current);
      current = "";
      continue;
    }

    if (character === "\n") {
      lines.push(current);
      current = "";
      continue;
    }

    current += character;
  }

  if (current || text.endsWith("\n") || text.endsWith("\r")) {
    lines.push(current);
  }

  return lines;
}

function isTerminalStatus(status) {
  return (
    status === "completed" ||
    status === "completed_with_errors" ||
    status === "failed" ||
    status === "cancelled"
  );
}

function chunkRows(rows, chunkSize) {
  const chunks = [];

  for (let index = 0; index < rows.length; index += chunkSize) {
    chunks.push(rows.slice(index, index + chunkSize));
  }

  return chunks;
}

async function createCsvImportRun(csvText, fileName) {
  const formData = new FormData();
  const file = new File([csvText], fileName, {
    type: "text/csv",
  });

  formData.append("file", file);
  formData.append("forceRefresh", "false");

  const response = await fetch(`${API_BASE_URL}/api/import-runs/csv`, {
    method: "POST",
    body: formData,
  });
  const payload = await response.json();

  if (!response.ok) {
    throw new Error(payload.error || `Unable to create CSV import run for ${fileName}.`);
  }

  return payload;
}

async function processImportRun(importRunId) {
  let lastDetail = null;

  for (let attempt = 1; attempt <= PROCESS_MAX_ATTEMPTS; attempt += 1) {
    const response = await fetch(
      `${API_BASE_URL}/api/import-runs/${encodeURIComponent(importRunId)}/process`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          batchSize: PROCESS_BATCH_SIZE,
          maxBatches: PROCESS_MAX_BATCHES,
        }),
      },
    );
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.error || `Unable to process import run ${importRunId}.`);
    }

    lastDetail = payload;

    if (isTerminalStatus(payload.status)) {
      return payload;
    }

    await sleep(PROCESS_RETRY_DELAY_MS);
  }

  throw new Error(
    `Import run ${importRunId} did not reach a terminal state after ${PROCESS_MAX_ATTEMPTS} attempts.`,
  );
}

async function importFile(filePath) {
  const absolutePath = path.resolve(filePath);
  const fileName = path.basename(absolutePath);
  const csvText = await fs.readFile(absolutePath, "utf8");
  const lines = splitLines(csvText);
  const header = lines[0] || "";
  const rawRows = lines.slice(1).filter((line) => line.trim().length > 0);

  if (!header) {
    throw new Error(`CSV file has no header row: ${absolutePath}`);
  }

  if (rawRows.length === 0) {
    throw new Error(`CSV file has no data rows: ${absolutePath}`);
  }

  const rowChunks = chunkRows(rawRows, CHUNK_ROW_LIMIT);
  const chunkResults = [];

  for (let index = 0; index < rowChunks.length; index += 1) {
    const rows = rowChunks[index];
    const chunkNumber = index + 1;
    const chunkFileName =
      rowChunks.length === 1 ? fileName : `${fileName.replace(".csv", "")}-part-${chunkNumber}.csv`;
    const chunkCsvText = `${header}\n${rows.join("\n")}`;

    process.stdout.write(
      `Importing ${fileName} chunk ${chunkNumber}/${rowChunks.length} (${rows.length} rows)...\n`,
    );

    const createdRun = await createCsvImportRun(chunkCsvText, chunkFileName);

    process.stdout.write(`Created import run ${createdRun.id}. Processing...\n`);

    const finishedRun = await processImportRun(createdRun.id);

    chunkResults.push({
      fileName: chunkFileName,
      importRunId: finishedRun.id,
      status: finishedRun.status,
      totalItems: finishedRun.totalItems,
      completedCount: finishedRun.completedCount,
      failedCount: finishedRun.failedCount,
      reusedCount: finishedRun.reusedCount,
      fetchedCount: finishedRun.fetchedCount,
      lastError: finishedRun.lastError,
    });

    process.stdout.write(
      `Finished ${chunkFileName}: status=${finishedRun.status}, completed=${finishedRun.completedCount}, failed=${finishedRun.failedCount}, reused=${finishedRun.reusedCount}, fetched=${finishedRun.fetchedCount}\n`,
    );
  }

  return {
    sourceFile: absolutePath,
    totalRows: rawRows.length,
    chunkCount: rowChunks.length,
    chunkResults,
  };
}

async function main() {
  const filePaths = process.argv.slice(2);

  if (filePaths.length === 0) {
    throw new Error("Pass one or more CSV file paths.");
  }

  const results = [];

  for (const filePath of filePaths) {
    results.push(await importFile(filePath));
  }

  process.stdout.write("\nImport summary:\n");
  process.stdout.write(`${JSON.stringify(results, null, 2)}\n`);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : "Unexpected CSV batch import error.";
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
