ALTER TABLE `import_source_files` ADD `content_hash` text;
--> statement-breakpoint
ALTER TABLE `import_source_files` ADD `header_mapping_json` text DEFAULT '[]' NOT NULL;
--> statement-breakpoint
CREATE INDEX `import_source_files_content_hash_idx` ON `import_source_files` (`content_hash`);
--> statement-breakpoint
ALTER TABLE `import_source_rows` ADD `ingest_status` text DEFAULT 'queued' NOT NULL;
--> statement-breakpoint
ALTER TABLE `import_source_rows` ADD `ingest_error_code` text;
--> statement-breakpoint
ALTER TABLE `import_source_rows` ADD `ingest_error_message` text;
