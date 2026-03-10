alter table public.import_schedules
  add column if not exists last_terminal_run_created_at timestamptz,
  add column if not exists alert_state jsonb not null default '{}'::jsonb;
