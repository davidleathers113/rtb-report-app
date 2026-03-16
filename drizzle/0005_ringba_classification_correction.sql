ALTER TABLE `bid_investigations` ADD `outcome_reason_category` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `outcome_reason_code` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `outcome_reason_message` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `classification_source` text;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `classification_confidence` real;--> statement-breakpoint
ALTER TABLE `bid_investigations` ADD `classification_warnings_json` text DEFAULT '[]' NOT NULL;--> statement-breakpoint
UPDATE `bid_investigations`
SET
  `outcome_reason_category` = CASE
    WHEN `outcome` = 'accepted' THEN 'accepted'
    WHEN `root_cause` = 'missing_caller_id' THEN 'missing_caller_id'
    WHEN `root_cause` = 'missing_zip_or_required_payload_field' THEN 'missing_required_field'
    WHEN `root_cause` = 'payload_validation_error' THEN 'request_invalid'
    WHEN `root_cause` = 'rate_limited' THEN 'rate_limited'
    WHEN `outcome` = 'zero_bid' THEN 'unknown_no_payable_bid'
    ELSE NULL
  END,
  `outcome_reason_code` = CASE
    WHEN `primary_error_code` IS NOT NULL THEN CAST(`primary_error_code` AS text)
    ELSE NULL
  END,
  `outcome_reason_message` = COALESCE(`primary_error_message`, `reason_for_reject`, `parsed_error_message`),
  `classification_source` = CASE
    WHEN `outcome` = 'accepted' THEN 'heuristic'
    WHEN `primary_error_code_source` = 'rejectReason_text' THEN 'reason_for_reject_text'
    WHEN `primary_error_code_source` IS NOT NULL THEN 'top_level_error'
    ELSE NULL
  END,
  `classification_confidence` = CASE
    WHEN `outcome` = 'accepted' THEN 0.7
    WHEN `primary_error_code_source` = 'rejectReason_text' THEN 0.5
    WHEN `outcome` = 'zero_bid' OR `outcome` = 'rejected' THEN 0.45
    ELSE NULL
  END,
  `classification_warnings_json` = CASE
    WHEN `outcome_reason_category` IS NOT NULL THEN json_array('Derived classification was backfilled from legacy investigation fields.')
    ELSE json_array()
  END;
