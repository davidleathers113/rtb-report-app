CREATE TABLE `import_source_files` (
	`id` text PRIMARY KEY NOT NULL,
	`import_run_id` text,
	`source_type` text NOT NULL,
	`file_name` text NOT NULL,
	`row_count` integer DEFAULT 0 NOT NULL,
	`header_json` text DEFAULT '[]' NOT NULL,
	`source_metadata` text DEFAULT '{}' NOT NULL,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`import_run_id`) REFERENCES `import_runs`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `import_source_files_import_run_id_idx` ON `import_source_files` (`import_run_id`);--> statement-breakpoint
CREATE INDEX `import_source_files_source_type_idx` ON `import_source_files` (`source_type`);--> statement-breakpoint
CREATE TABLE `import_source_rows` (
	`id` text PRIMARY KEY NOT NULL,
	`import_source_file_id` text NOT NULL,
	`import_run_id` text NOT NULL,
	`row_number` integer NOT NULL,
	`bid_id` text,
	`bid_dt` text,
	`campaign_name` text,
	`campaign_id` text,
	`publisher_name` text,
	`publisher_id` text,
	`bid_amount` real,
	`winning_bid` real,
	`bid_rejected` integer,
	`reason_for_reject` text,
	`bid_did` text,
	`bid_expire_date` text,
	`expiration_seconds` integer,
	`winning_bid_call_accepted` integer,
	`winning_bid_call_rejected` integer,
	`bid_elapsed_ms` integer,
	`row_json` text DEFAULT '{}' NOT NULL,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`import_source_file_id`) REFERENCES `import_source_files`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`import_run_id`) REFERENCES `import_runs`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `import_source_rows_file_id_idx` ON `import_source_rows` (`import_source_file_id`);--> statement-breakpoint
CREATE INDEX `import_source_rows_import_run_id_idx` ON `import_source_rows` (`import_run_id`);--> statement-breakpoint
CREATE INDEX `import_source_rows_bid_id_idx` ON `import_source_rows` (`bid_id`);--> statement-breakpoint
CREATE INDEX `import_source_rows_bid_dt_idx` ON `import_source_rows` (`bid_dt`);
