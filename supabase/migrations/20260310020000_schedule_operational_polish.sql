alter table public.import_schedules
  add column if not exists consecutive_failure_count integer not null default 0;

alter table public.import_schedules
  drop constraint if exists import_schedules_consecutive_failure_count_check;

alter table public.import_schedules
  add constraint import_schedules_consecutive_failure_count_check
  check (consecutive_failure_count >= 0);

create or replace function public.claim_due_import_schedules(
  p_limit integer default 10,
  p_lease_seconds integer default 120,
  p_stale_after_minutes integer default 30
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
  last_error text,
  consecutive_failure_count integer
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
          and not (
            import_runs.updated_at <= v_now - make_interval(mins => greatest(p_stale_after_minutes, 5))
            and (
              import_runs.processor_lease_expires_at is null
              or import_runs.processor_lease_expires_at <= v_now
            )
          )
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
    import_schedules.last_error,
    import_schedules.consecutive_failure_count;
end;
$$;
