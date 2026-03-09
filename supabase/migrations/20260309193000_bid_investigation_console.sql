create extension if not exists pgcrypto;

create table if not exists public.import_runs (
  id uuid primary key default gen_random_uuid(),
  source_type text not null,
  status text not null default 'running',
  total_found integer not null default 0,
  total_processed integer not null default 0,
  notes text,
  started_at timestamptz not null default timezone('utc', now()),
  completed_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.bid_investigations (
  id uuid primary key default gen_random_uuid(),
  import_run_id uuid references public.import_runs(id) on delete set null,
  bid_id text not null unique,
  bid_dt timestamptz,
  campaign_name text,
  campaign_id text,
  publisher_name text,
  publisher_id text,
  target_name text,
  target_id text,
  buyer_name text,
  buyer_id text,
  bid_amount numeric(12, 4),
  winning_bid numeric(12, 4),
  is_zero_bid boolean not null default false,
  reason_for_reject text,
  http_status_code integer,
  parsed_error_message text,
  request_body jsonb,
  response_body jsonb,
  raw_trace_json jsonb not null default '{}'::jsonb,
  outcome text not null default 'unknown',
  root_cause text not null default 'unknown_needs_review',
  root_cause_confidence numeric(5, 4) not null default 0,
  severity text not null default 'medium',
  owner_type text not null default 'unknown',
  suggested_fix text not null default '',
  explanation text not null default '',
  evidence_json jsonb not null default '[]'::jsonb,
  imported_at timestamptz not null default timezone('utc', now()),
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.bid_events (
  id uuid primary key default gen_random_uuid(),
  bid_investigation_id uuid not null references public.bid_investigations(id) on delete cascade,
  event_name text not null,
  event_timestamp timestamptz,
  event_vals_json jsonb,
  event_str_vals_json jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists bid_investigations_bid_dt_idx
  on public.bid_investigations (bid_dt desc);

create index if not exists bid_investigations_root_cause_idx
  on public.bid_investigations (root_cause);

create index if not exists bid_investigations_owner_type_idx
  on public.bid_investigations (owner_type);

create index if not exists bid_investigations_outcome_idx
  on public.bid_investigations (outcome);

create index if not exists bid_investigations_imported_at_idx
  on public.bid_investigations (imported_at desc);

create index if not exists bid_investigations_campaign_id_idx
  on public.bid_investigations (campaign_id);

create index if not exists bid_investigations_publisher_id_idx
  on public.bid_investigations (publisher_id);

create index if not exists bid_investigations_target_id_idx
  on public.bid_investigations (target_id);

create index if not exists bid_events_investigation_id_idx
  on public.bid_events (bid_investigation_id);

create index if not exists bid_events_timestamp_idx
  on public.bid_events (event_timestamp);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists set_import_runs_updated_at on public.import_runs;
create trigger set_import_runs_updated_at
before update on public.import_runs
for each row
execute function public.set_updated_at();

drop trigger if exists set_bid_investigations_updated_at on public.bid_investigations;
create trigger set_bid_investigations_updated_at
before update on public.bid_investigations
for each row
execute function public.set_updated_at();

drop trigger if exists set_bid_events_updated_at on public.bid_events;
create trigger set_bid_events_updated_at
before update on public.bid_events
for each row
execute function public.set_updated_at();
