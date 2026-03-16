ALTER TABLE `bid_investigations` ADD `parse_status` text DEFAULT 'not_attempted' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `normalization_version` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `schema_variant` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `normalization_confidence` real;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `normalization_warnings_json` text DEFAULT '[]' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `missing_critical_fields_json` text DEFAULT '[]' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `missing_optional_fields_json` text DEFAULT '[]' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `unknown_event_names_json` text DEFAULT '[]' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `raw_paths_used_json` text DEFAULT '{}' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_error_code_source` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_error_code_confidence` real;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_error_code_raw_match` text;--> statement-breakpoint
UPDATE `bid_investigations`
SET
  `parse_status` = CASE
    WHEN `fetch_status` = 'failed' THEN 'not_attempted'
    WHEN `primary_error_code` IS NOT NULL THEN 'text_fallback'
    WHEN `fetch_status` = 'fetched' THEN 'partial'
    ELSE 'not_attempted'
  END,
  `normalization_version` = CASE
    WHEN `fetch_status` = 'fetched' THEN 'ringba-normalizer-v2'
    ELSE NULL
  END,
  `schema_variant` = CASE
    WHEN `fetch_status` = 'fetched' THEN 'legacy_migrated'
    ELSE NULL
  END,
  `normalization_confidence` = CASE
    WHEN `fetch_status` = 'fetched' THEN 0.5
    ELSE NULL
  END,
  `normalization_warnings_json` = CASE
    WHEN `fetch_status` = 'fetched' THEN json_array('Legacy investigations were migrated before parser provenance was available.')
    ELSE json_array()
  END,
  `missing_critical_fields_json` = json_array(),
  `missing_optional_fields_json` = json_array(),
  `unknown_event_names_json` = json_array(),
  `raw_paths_used_json` = json('{}'),
  `primary_error_code_source` = CASE
    WHEN `primary_error_code` IS NOT NULL THEN 'legacy_migrated'
    ELSE NULL
  END,
  `primary_error_code_confidence` = CASE
    WHEN `primary_error_code` IS NOT NULL THEN 0.4
    ELSE NULL
  END,
  `primary_error_code_raw_match` = NULL;