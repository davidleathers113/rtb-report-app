alter table public.import_runs
  add column if not exists force_refresh boolean not null default false,
  add column if not exists last_error text,
  add column if not exists processor_lease_expires_at timestamptz;

create table if not exists public.import_run_items (
  id uuid primary key default gen_random_uuid(),
  import_run_id uuid not null references public.import_runs(id) on delete cascade,
  bid_id text not null,
  position integer not null,
  status text not null default 'queued',
  resolution text,
  error_message text,
  investigation_id uuid references public.bid_investigations(id) on delete set null,
  started_at timestamptz,
  completed_at timestamptz,
  attempt_count integer not null default 0,
  lease_expires_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (import_run_id, bid_id),
  unique (import_run_id, position)
);

alter table public.import_runs
  drop constraint if exists import_runs_status_check;

alter table public.import_runs
  add constraint import_runs_status_check
  check (status in ('queued', 'running', 'completed', 'completed_with_errors', 'failed', 'cancelled'));

alter table public.import_run_items
  drop constraint if exists import_run_items_status_check;

alter table public.import_run_items
  add constraint import_run_items_status_check
  check (status in ('queued', 'running', 'completed', 'failed'));

alter table public.import_run_items
  drop constraint if exists import_run_items_resolution_check;

alter table public.import_run_items
  add constraint import_run_items_resolution_check
  check (
    resolution is null
    or resolution in ('reused', 'fetched', 'failed', 'skipped')
  );

create index if not exists import_runs_status_idx
  on public.import_runs (status);

create index if not exists import_runs_processor_lease_idx
  on public.import_runs (processor_lease_expires_at);

create index if not exists import_run_items_run_id_idx
  on public.import_run_items (import_run_id);

create index if not exists import_run_items_status_idx
  on public.import_run_items (status);

create index if not exists import_run_items_lease_idx
  on public.import_run_items (lease_expires_at);

create index if not exists import_run_items_investigation_id_idx
  on public.import_run_items (investigation_id);

drop trigger if exists set_import_run_items_updated_at on public.import_run_items;
create trigger set_import_run_items_updated_at
before update on public.import_run_items
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
begin
  select *
  into v_run
  from public.import_runs
  where import_runs.id = p_import_run_id
  for update;

  if not found then
    raise exception 'Import run not found: %', p_import_run_id;
  end if;

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

  if not v_has_available_items then
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

create or replace function public.claim_import_run_items(
  p_import_run_id uuid,
  p_batch_size integer default 10,
  p_lease_seconds integer default 180
)
returns table (
  id uuid,
  bid_id text,
  position integer
)
language plpgsql
as $$
declare
  v_now timestamptz := timezone('utc', now());
  v_lease_expires_at timestamptz := v_now + make_interval(secs => greatest(p_lease_seconds, 30));
begin
  return query
  with next_items as (
    select import_run_items.id
    from public.import_run_items
    where import_run_items.import_run_id = p_import_run_id
      and (
        import_run_items.status = 'queued'
        or (
          import_run_items.status = 'running'
          and import_run_items.lease_expires_at is not null
          and import_run_items.lease_expires_at <= v_now
        )
      )
    order by import_run_items.position
    limit greatest(p_batch_size, 1)
    for update skip locked
  )
  update public.import_run_items
  set
    status = 'running',
    started_at = coalesce(import_run_items.started_at, v_now),
    lease_expires_at = v_lease_expires_at,
    attempt_count = import_run_items.attempt_count + 1
  where import_run_items.id in (select next_items.id from next_items)
  returning import_run_items.id, import_run_items.bid_id, import_run_items.position;
end;
$$;
