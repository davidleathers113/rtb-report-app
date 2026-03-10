CREATE TABLE `bid_events` (
	`id` text PRIMARY KEY NOT NULL,
	`bid_investigation_id` text NOT NULL,
	`event_name` text NOT NULL,
	`event_timestamp` text,
	`event_vals_json` text,
	`event_str_vals_json` text,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`bid_investigation_id`) REFERENCES `bid_investigations`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `bid_events_bid_investigation_id_idx` ON `bid_events` (`bid_investigation_id`);--> statement-breakpoint
CREATE INDEX `bid_events_event_timestamp_idx` ON `bid_events` (`event_timestamp`);--> statement-breakpoint
CREATE TABLE `bid_investigations` (
	`id` text PRIMARY KEY NOT NULL,
	`import_run_id` text,
	`bid_id` text NOT NULL,
	`bid_dt` text,
	`campaign_name` text,
	`campaign_id` text,
	`publisher_name` text,
	`publisher_id` text,
	`target_name` text,
	`target_id` text,
	`buyer_name` text,
	`buyer_id` text,
	`bid_amount` real,
	`winning_bid` real,
	`is_zero_bid` integer DEFAULT false NOT NULL,
	`reason_for_reject` text,
	`http_status_code` integer,
	`parsed_error_message` text,
	`request_body` text,
	`response_body` text,
	`raw_trace_json` text DEFAULT '{}' NOT NULL,
	`outcome` text DEFAULT 'unknown' NOT NULL,
	`root_cause` text DEFAULT 'unknown_needs_review' NOT NULL,
	`root_cause_confidence` real DEFAULT 0 NOT NULL,
	`severity` text DEFAULT 'high' NOT NULL,
	`owner_type` text DEFAULT 'system' NOT NULL,
	`suggested_fix` text DEFAULT '' NOT NULL,
	`explanation` text DEFAULT '' NOT NULL,
	`evidence_json` text DEFAULT '[]' NOT NULL,
	`fetch_status` text DEFAULT 'pending' NOT NULL,
	`fetched_at` text,
	`fetch_started_at` text,
	`last_error` text,
	`refresh_requested_at` text,
	`lease_expires_at` text,
	`fetch_attempt_count` integer DEFAULT 0 NOT NULL,
	`imported_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`import_run_id`) REFERENCES `import_runs`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE UNIQUE INDEX `bid_investigations_bid_id_unique` ON `bid_investigations` (`bid_id`);--> statement-breakpoint
CREATE INDEX `bid_investigations_fetch_status_idx` ON `bid_investigations` (`fetch_status`);--> statement-breakpoint
CREATE INDEX `bid_investigations_imported_at_idx` ON `bid_investigations` (`imported_at`);--> statement-breakpoint
CREATE INDEX `bid_investigations_bid_dt_idx` ON `bid_investigations` (`bid_dt`);--> statement-breakpoint
CREATE INDEX `bid_investigations_import_run_id_idx` ON `bid_investigations` (`import_run_id`);--> statement-breakpoint
CREATE TABLE `import_ops_events` (
	`id` text PRIMARY KEY NOT NULL,
	`event_type` text NOT NULL,
	`severity` text NOT NULL,
	`source` text NOT NULL,
	`schedule_id` text,
	`import_run_id` text,
	`message` text NOT NULL,
	`metadata_json` text DEFAULT '{}' NOT NULL,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`schedule_id`) REFERENCES `import_schedules`(`id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`import_run_id`) REFERENCES `import_runs`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX `import_ops_events_schedule_created_idx` ON `import_ops_events` (`schedule_id`,`created_at`);--> statement-breakpoint
CREATE INDEX `import_ops_events_event_type_idx` ON `import_ops_events` (`event_type`);--> statement-breakpoint
CREATE INDEX `import_ops_events_severity_idx` ON `import_ops_events` (`severity`);--> statement-breakpoint
CREATE INDEX `import_ops_events_import_run_id_idx` ON `import_ops_events` (`import_run_id`);--> statement-breakpoint
CREATE TABLE `import_run_items` (
	`id` text PRIMARY KEY NOT NULL,
	`import_run_id` text NOT NULL,
	`bid_id` text NOT NULL,
	`position` integer NOT NULL,
	`status` text DEFAULT 'queued' NOT NULL,
	`resolution` text,
	`error_message` text,
	`investigation_id` text,
	`started_at` text,
	`completed_at` text,
	`attempt_count` integer DEFAULT 0 NOT NULL,
	`lease_expires_at` text,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`import_run_id`) REFERENCES `import_runs`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`investigation_id`) REFERENCES `bid_investigations`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE UNIQUE INDEX `import_run_items_import_run_position_unique` ON `import_run_items` (`import_run_id`,`position`);--> statement-breakpoint
CREATE UNIQUE INDEX `import_run_items_import_run_bid_unique` ON `import_run_items` (`import_run_id`,`bid_id`);--> statement-breakpoint
CREATE INDEX `import_run_items_import_run_status_idx` ON `import_run_items` (`import_run_id`,`status`);--> statement-breakpoint
CREATE INDEX `import_run_items_investigation_id_idx` ON `import_run_items` (`investigation_id`);--> statement-breakpoint
CREATE TABLE `import_runs` (
	`id` text PRIMARY KEY NOT NULL,
	`source_type` text NOT NULL,
	`trigger_type` text DEFAULT 'manual' NOT NULL,
	`schedule_id` text,
	`source_stage` text DEFAULT 'queued' NOT NULL,
	`status` text DEFAULT 'queued' NOT NULL,
	`force_refresh` integer DEFAULT false NOT NULL,
	`notes` text,
	`last_error` text,
	`total_found` integer DEFAULT 0 NOT NULL,
	`total_processed` integer DEFAULT 0 NOT NULL,
	`source_window_start` text,
	`source_window_end` text,
	`export_job_id` text,
	`export_row_count` integer DEFAULT 0 NOT NULL,
	`export_download_status` text,
	`source_metadata` text DEFAULT '{}' NOT NULL,
	`started_at` text,
	`completed_at` text,
	`processor_lease_expires_at` text,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`schedule_id`) REFERENCES `import_schedules`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX `import_runs_status_idx` ON `import_runs` (`status`);--> statement-breakpoint
CREATE INDEX `import_runs_created_at_idx` ON `import_runs` (`created_at`);--> statement-breakpoint
CREATE INDEX `import_runs_schedule_id_idx` ON `import_runs` (`schedule_id`,`trigger_type`);--> statement-breakpoint
CREATE INDEX `import_runs_source_stage_idx` ON `import_runs` (`source_stage`);--> statement-breakpoint
CREATE TABLE `import_schedules` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`is_enabled` integer DEFAULT true NOT NULL,
	`account_id` text NOT NULL,
	`source_type` text DEFAULT 'ringba_recent_import' NOT NULL,
	`window_minutes` integer NOT NULL,
	`overlap_minutes` integer DEFAULT 2 NOT NULL,
	`max_concurrent_runs` integer DEFAULT 1 NOT NULL,
	`last_triggered_at` text,
	`last_succeeded_at` text,
	`last_failed_at` text,
	`last_error` text,
	`consecutive_failure_count` integer DEFAULT 0 NOT NULL,
	`last_terminal_run_created_at` text,
	`alert_state` text DEFAULT '{}' NOT NULL,
	`paused_at` text,
	`pause_reason` text,
	`alert_acknowledged_at` text,
	`alert_acknowledged_key` text,
	`alert_snoozed_until` text,
	`trigger_lease_expires_at` text,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `import_schedules_enabled_idx` ON `import_schedules` (`is_enabled`,`paused_at`);--> statement-breakpoint
CREATE INDEX `import_schedules_last_triggered_at_idx` ON `import_schedules` (`last_triggered_at`);--> statement-breakpoint
CREATE INDEX `import_schedules_source_type_idx` ON `import_schedules` (`source_type`);--> statement-breakpoint
CREATE TABLE `import_source_checkpoints` (
	`source_key` text PRIMARY KEY NOT NULL,
	`source_type` text NOT NULL,
	`last_successful_bid_dt` text,
	`source_metadata` text DEFAULT '{}' NOT NULL,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `import_source_checkpoints_source_type_idx` ON `import_source_checkpoints` (`source_type`);