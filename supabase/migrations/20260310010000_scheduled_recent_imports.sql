create table if not exists public.import_schedules (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  is_enabled boolean not null default true,
  account_id text not null,
  source_type text not null default 'ringba_recent_import',
  window_minutes integer not null,
  overlap_minutes integer not null default 2,
  max_concurrent_runs integer not null default 1,
  last_triggered_at timestamptz,
  last_succeeded_at timestamptz,
  last_failed_at timestamptz,
  last_error text,
  trigger_lease_expires_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

alter table public.import_schedules
  drop constraint if exists import_schedules_source_type_check;

alter table public.import_schedules
  add constraint import_schedules_source_type_check
  check (source_type in ('ringba_recent_import'));

alter table public.import_schedules
  drop constraint if exists import_schedules_window_minutes_check;

alter table public.import_schedules
  add constraint import_schedules_window_minutes_check
  check (window_minutes in (5, 15, 60));

alter table public.import_schedules
  drop constraint if exists import_schedules_overlap_minutes_check;

alter table public.import_schedules
  add constraint import_schedules_overlap_minutes_check
  check (overlap_minutes >= 0 and overlap_minutes <= 15);

alter table public.import_schedules
  drop constraint if exists import_schedules_max_concurrent_runs_check;

alter table public.import_schedules
  add constraint import_schedules_max_concurrent_runs_check
  check (max_concurrent_runs >= 1 and max_concurrent_runs <= 3);

create index if not exists import_schedules_enabled_idx
  on public.import_schedules (is_enabled);

create index if not exists import_schedules_trigger_lease_idx
  on public.import_schedules (trigger_lease_expires_at);

drop trigger if exists set_import_schedules_updated_at on public.import_schedules;
create trigger set_import_schedules_updated_at
before update on public.import_schedules
for each row
execute function public.set_updated_at();

alter table public.import_runs
  add column if not exists trigger_type text not null default 'manual',
  add column if not exists schedule_id uuid references public.import_schedules(id) on delete set null;

alter table public.import_runs
  drop constraint if exists import_runs_trigger_type_check;

alter table public.import_runs
  add constraint import_runs_trigger_type_check
  check (trigger_type in ('manual', 'scheduled'));

create index if not exists import_runs_schedule_id_idx
  on public.import_runs (schedule_id);

create index if not exists import_runs_trigger_type_idx
  on public.import_runs (trigger_type);

create or replace function public.claim_due_import_schedules(
  p_limit integer default 10,
  p_lease_seconds integer default 120
)
returns table (
  id uuid,
  name text,
  is_enabled boolean,
  account_id text,
  source_type text,
  window_minutes integer,
  overlap_minutes integer,
  max_concurrent_runs integer,
  last_triggered_at timestamptz,
  last_succeeded_at timestamptz,
  last_failed_at timestamptz,
  last_error text
)
language plpgsql
as $$
declare
  v_now timestamptz := timezone('utc', now());
  v_lease_expires_at timestamptz := v_now + make_interval(secs => greatest(p_lease_seconds, 30));
begin
  return query
  with due_schedules as (
    select import_schedules.id
    from public.import_schedules
    where import_schedules.is_enabled = true
      and (
        import_schedules.trigger_lease_expires_at is null
        or import_schedules.trigger_lease_expires_at <= v_now
      )
      and (
        import_schedules.last_triggered_at is null
        or import_schedules.last_triggered_at <= v_now - make_interval(mins => greatest(import_schedules.window_minutes, 1))
      )
      and (
        select count(*)
        from public.import_runs
        where import_runs.schedule_id = import_schedules.id
          and import_runs.trigger_type = 'scheduled'
          and import_runs.status in ('queued', 'running')
      ) < import_schedules.max_concurrent_runs
    order by coalesce(import_schedules.last_triggered_at, 'epoch'::timestamptz), import_schedules.created_at
    limit greatest(p_limit, 1)
    for update skip locked
  )
  update public.import_schedules
  set trigger_lease_expires_at = v_lease_expires_at
  where import_schedules.id in (select due_schedules.id from due_schedules)
  returning
    import_schedules.id,
    import_schedules.name,
    import_schedules.is_enabled,
    import_schedules.account_id,
    import_schedules.source_type,
    import_schedules.window_minutes,
    import_schedules.overlap_minutes,
    import_schedules.max_concurrent_runs,
    import_schedules.last_triggered_at,
    import_schedules.last_succeeded_at,
    import_schedules.last_failed_at,
    import_schedules.last_error;
end;
$$;
