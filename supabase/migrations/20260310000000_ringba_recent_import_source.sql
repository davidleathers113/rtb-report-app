alter table public.import_runs
  add column if not exists source_stage text not null default 'queued',
  add column if not exists source_window_start timestamptz,
  add column if not exists source_window_end timestamptz,
  add column if not exists export_job_id text,
  add column if not exists export_row_count integer not null default 0,
  add column if not exists export_download_status text,
  add column if not exists source_metadata jsonb not null default '{}'::jsonb;

alter table public.import_runs
  drop constraint if exists import_runs_source_stage_check;

alter table public.import_runs
  add constraint import_runs_source_stage_check
  check (
    source_stage in (
      'creating_export',
      'polling_export',
      'downloading',
      'extracting',
      'parsing',
      'queued',
      'processing',
      'completed',
      'failed'
    )
  );

alter table public.import_runs
  drop constraint if exists import_runs_export_download_status_check;

alter table public.import_runs
  add constraint import_runs_export_download_status_check
  check (
    export_download_status is null
    or export_download_status in (
      'pending',
      'ready',
      'downloaded',
      'extracted',
      'parsed',
      'failed'
    )
  );

create index if not exists import_runs_source_type_idx
  on public.import_runs (source_type);

create index if not exists import_runs_source_stage_idx
  on public.import_runs (source_stage);

create table if not exists public.import_source_checkpoints (
  source_key text primary key,
  source_type text not null,
  last_successful_bid_dt timestamptz,
  source_metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

drop trigger if exists set_import_source_checkpoints_updated_at on public.import_source_checkpoints;
create trigger set_import_source_checkpoints_updated_at
before update on public.import_source_checkpoints
for each row
execute function public.set_updated_at();

create or replace function public.claim_import_run_processing(
  p_import_run_id uuid,
  p_lease_seconds integer default 60
)
returns table (
  id uuid,
  status text,
  should_process boolean,
  force_refresh boolean,
  total_items integer,
  total_processed integer,
  last_error text
)
language plpgsql
as $$
declare
  v_now timestamptz := timezone('utc', now());
  v_lease_expires_at timestamptz := v_now + make_interval(secs => greatest(p_lease_seconds, 30));
  v_run public.import_runs%rowtype;
  v_should_process boolean := false;
  v_total_items integer := 0;
  v_total_processed integer := 0;
  v_has_available_items boolean := false;
  v_source_stage text := 'queued';
  v_requires_source_processing boolean := false;
begin
  select *
  into v_run
  from public.import_runs
  where import_runs.id = p_import_run_id
  for update;

  if not found then
    raise exception 'Import run not found: %', p_import_run_id;
  end if;

  v_source_stage := coalesce(v_run.source_stage, 'queued');
  v_requires_source_processing := v_source_stage not in ('queued', 'processing', 'completed', 'failed');

  select count(*)
  into v_total_items
  from public.import_run_items
  where import_run_id = p_import_run_id;

  select count(*)
  into v_total_processed
  from public.import_run_items
  where import_run_id = p_import_run_id
    and status in ('completed', 'failed');

  select exists(
    select 1
    from public.import_run_items
    where import_run_id = p_import_run_id
      and (
        status = 'queued'
        or (status = 'running' and lease_expires_at is not null and lease_expires_at <= v_now)
      )
  )
  into v_has_available_items;

  if not v_requires_source_processing and not v_has_available_items then
    v_should_process := false;
  elsif v_run.status = 'running'
    and v_run.processor_lease_expires_at is not null
    and v_run.processor_lease_expires_at > v_now then
    v_should_process := false;
  elsif v_run.status in ('completed', 'completed_with_errors', 'cancelled') then
    v_should_process := false;
  else
    v_should_process := true;
  end if;

  if v_should_process then
    update public.import_runs
    set
      status = 'running',
      source_stage = case
        when v_source_stage = 'queued' and v_has_available_items then 'processing'
        else v_source_stage
      end,
      processor_lease_expires_at = v_lease_expires_at,
      started_at = coalesce(started_at, v_now),
      completed_at = null,
      last_error = null
    where import_runs.id = p_import_run_id
    returning * into v_run;
  end if;

  return query
  select
    v_run.id,
    v_run.status,
    v_should_process,
    v_run.force_refresh,
    v_total_items,
    v_total_processed,
    v_run.last_error;
end;
$$;
