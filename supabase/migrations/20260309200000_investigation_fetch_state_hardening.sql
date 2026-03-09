alter table public.bid_investigations
  add column if not exists fetch_status text not null default 'pending',
  add column if not exists fetched_at timestamptz,
  add column if not exists fetch_started_at timestamptz,
  add column if not exists last_error text,
  add column if not exists refresh_requested_at timestamptz,
  add column if not exists lease_expires_at timestamptz,
  add column if not exists fetch_attempt_count integer not null default 0;

update public.bid_investigations
set
  fetch_status = case
    when coalesce(last_error, '') <> '' then 'failed'
    when raw_trace_json <> '{}'::jsonb then 'fetched'
    else 'pending'
  end,
  fetched_at = case
    when raw_trace_json <> '{}'::jsonb and fetched_at is null then imported_at
    else fetched_at
  end,
  fetch_started_at = coalesce(fetch_started_at, created_at),
  fetch_attempt_count = case
    when fetch_attempt_count > 0 then fetch_attempt_count
    when raw_trace_json <> '{}'::jsonb then 1
    else 0
  end;

alter table public.bid_investigations
  drop constraint if exists bid_investigations_fetch_status_check;

alter table public.bid_investigations
  add constraint bid_investigations_fetch_status_check
  check (fetch_status in ('pending', 'fetched', 'failed'));

create index if not exists bid_investigations_fetch_status_idx
  on public.bid_investigations (fetch_status);

create index if not exists bid_investigations_lease_expires_at_idx
  on public.bid_investigations (lease_expires_at);

create or replace function public.claim_bid_investigation(
  p_bid_id text,
  p_import_run_id uuid default null,
  p_force_refresh boolean default false,
  p_lease_seconds integer default 120
)
returns table (
  id uuid,
  bid_id text,
  fetch_status text,
  should_fetch boolean,
  fetched_at timestamptz,
  last_error text,
  fetch_attempt_count integer,
  lease_expires_at timestamptz
)
language plpgsql
as $$
declare
  v_now timestamptz := timezone('utc', now());
  v_lease_expires_at timestamptz := v_now + make_interval(secs => greatest(p_lease_seconds, 30));
  v_row public.bid_investigations%rowtype;
  v_should_fetch boolean := false;
  v_has_trace boolean := false;
begin
  insert into public.bid_investigations (
    bid_id,
    import_run_id,
    fetch_status,
    refresh_requested_at
  )
  values (
    p_bid_id,
    p_import_run_id,
    'pending',
    case when p_force_refresh then v_now else null end
  )
  on conflict (bid_id) do nothing;

  select *
  into v_row
  from public.bid_investigations
  where bid_investigations.bid_id = p_bid_id
  for update;

  if p_import_run_id is not null and v_row.import_run_id is distinct from p_import_run_id then
    update public.bid_investigations
    set import_run_id = p_import_run_id
    where public.bid_investigations.id = v_row.id
    returning * into v_row;
  end if;

  if p_force_refresh and v_row.refresh_requested_at is distinct from v_now then
    update public.bid_investigations
    set refresh_requested_at = v_now
    where public.bid_investigations.id = v_row.id
    returning * into v_row;
  end if;

  v_has_trace := v_row.raw_trace_json <> '{}'::jsonb;

  if v_row.fetch_status = 'pending'
    and v_row.lease_expires_at is not null
    and v_row.lease_expires_at > v_now then
    v_should_fetch := false;
  elsif p_force_refresh then
    v_should_fetch := true;
  elsif v_row.fetch_status = 'failed' then
    v_should_fetch := true;
  elsif v_row.fetch_status <> 'fetched' then
    v_should_fetch := true;
  elsif v_row.fetched_at is null then
    v_should_fetch := true;
  elsif not v_has_trace then
    v_should_fetch := true;
  else
    v_should_fetch := false;
  end if;

  if v_should_fetch then
    update public.bid_investigations
    set
      fetch_status = 'pending',
      fetch_started_at = v_now,
      lease_expires_at = v_lease_expires_at,
      last_error = null,
      fetch_attempt_count = coalesce(fetch_attempt_count, 0) + 1,
      import_run_id = coalesce(p_import_run_id, import_run_id),
      refresh_requested_at = case
        when p_force_refresh then v_now
        else refresh_requested_at
      end
    where public.bid_investigations.id = v_row.id
    returning * into v_row;
  end if;

  return query
  select
    v_row.id,
    v_row.bid_id,
    v_row.fetch_status,
    v_should_fetch,
    v_row.fetched_at,
    v_row.last_error,
    v_row.fetch_attempt_count,
    v_row.lease_expires_at;
end;
$$;
