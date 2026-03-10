ALTER TABLE `bid_investigations` ADD `source_import_run_id` text REFERENCES `import_runs`(`id`) ON DELETE set null;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `source_import_source_file_id` text REFERENCES `import_source_files`(`id`) ON DELETE set null;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `source_import_source_row_id` text REFERENCES `import_source_rows`(`id`) ON DELETE set null;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `detail_source` text DEFAULT 'ringba_api' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `enrichment_state` text DEFAULT 'enriched' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `last_ringba_attempt_at` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `last_ringba_fetch_at` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `ringba_failure_count` integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `next_ringba_retry_at` text;--> statement-breakpoint
CREATE INDEX `bid_investigations_enrichment_state_idx` ON `bid_investigations` (`enrichment_state`);--> statement-breakpoint
CREATE INDEX `bid_investigations_next_ringba_retry_at_idx` ON `bid_investigations` (`next_ringba_retry_at`);--> statement-breakpoint
CREATE INDEX `bid_investigations_source_import_run_id_idx` ON `bid_investigations` (`source_import_run_id`);--> statement-breakpoint
UPDATE `bid_investigations`
SET
  `detail_source` = 'ringba_api',
  `enrichment_state` = CASE
    WHEN `fetch_status` = 'failed' THEN 'failed'
    WHEN `fetch_status` = 'pending' THEN 'fetching'
    ELSE 'enriched'
  END,
  `last_ringba_attempt_at` = COALESCE(`fetch_started_at`, `fetched_at`, `imported_at`),
  `last_ringba_fetch_at` = CASE
    WHEN `fetch_status` = 'fetched' THEN COALESCE(`fetched_at`, `imported_at`)
    ELSE NULL
  END,
  `ringba_failure_count` = CASE
    WHEN `fetch_status` = 'failed' THEN 1
    ELSE 0
  END;--> statement-breakpoint
UPDATE `bid_investigations`
SET
  `source_import_run_id` = `import_run_id`,
  `detail_source` = 'csv_direct',
  `enrichment_state` = 'csv_only',
  `last_ringba_attempt_at` = NULL,
  `last_ringba_fetch_at` = NULL,
  `ringba_failure_count` = 0,
  `next_ringba_retry_at` = NULL
WHERE `import_run_id` IN (
  SELECT `id`
  FROM `import_runs`
  WHERE `source_type` = 'csv_direct_import'
);--> statement-breakpoint
ALTER TABLE `import_schedules` ADD `source_metadata` text DEFAULT '{}' NOT NULL;
