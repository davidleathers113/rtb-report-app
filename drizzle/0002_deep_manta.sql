CREATE TABLE `bid_target_attempts` (
	`id` text PRIMARY KEY NOT NULL,
	`bid_investigation_id` text NOT NULL,
	`sequence` integer NOT NULL,
	`event_name` text NOT NULL,
	`event_timestamp` text,
	`target_name` text,
	`target_id` text,
	`target_buyer` text,
	`target_buyer_id` text,
	`target_number` text,
	`target_group_name` text,
	`target_group_id` text,
	`target_sub_id` text,
	`target_buyer_sub_id` text,
	`request_url` text,
	`http_method` text,
	`request_status` text,
	`http_status_code` integer,
	`duration_ms` integer,
	`route_priority` integer,
	`route_weight` integer,
	`accepted` integer,
	`winning` integer,
	`bid_amount` real,
	`min_duration_seconds` integer,
	`reject_reason` text,
	`error_code` integer,
	`error_message` text,
	`errors_json` text DEFAULT '[]' NOT NULL,
	`request_body` text,
	`response_body` text,
	`summary_reason` text,
	`raw_event_json` text DEFAULT '{}' NOT NULL,
	`created_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	`updated_at` text DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) NOT NULL,
	FOREIGN KEY (`bid_investigation_id`) REFERENCES `bid_investigations`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `bid_target_attempts_bid_investigation_id_idx` ON `bid_target_attempts` (`bid_investigation_id`);--> statement-breakpoint
CREATE UNIQUE INDEX `bid_target_attempts_investigation_sequence_unique` ON `bid_target_attempts` (`bid_investigation_id`,`sequence`);--> statement-breakpoint
CREATE INDEX `bid_target_attempts_target_idx` ON `bid_target_attempts` (`target_name`,`target_buyer`);--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `bid_elapsed_ms` integer;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_failure_stage` text DEFAULT 'unknown' NOT NULL;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_target_name` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_target_id` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_buyer_name` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_buyer_id` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_error_code` integer;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `primary_error_message` text;