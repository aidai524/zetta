create table if not exists collector_tasks (
  id bigserial primary key,
  task_type text not null,
  source text not null,
  entity text not null,
  params jsonb not null default '{}'::jsonb,
  cursor jsonb not null default '{}'::jsonb,
  status text not null default 'pending',
  priority integer not null default 100,
  attempts integer not null default 0,
  lease_owner text,
  lease_expires_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table collector_tasks
  add column if not exists max_attempts integer not null default 3;

create index if not exists idx_collector_tasks_claim
  on collector_tasks (status, priority, id);

create index if not exists idx_collector_tasks_lease
  on collector_tasks (lease_expires_at)
  where lease_owner is not null;

create unique index if not exists idx_collector_tasks_unique_work
  on collector_tasks (task_type, source, entity, md5(params::text));

create table if not exists collector_runs (
  id bigserial primary key,
  task_id bigint references collector_tasks(id),
  node_id text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null,
  pages integer not null default 0,
  items bigint not null default 0,
  raw_paths text[] not null default '{}',
  error text
);

create index if not exists idx_collector_runs_finished
  on collector_runs (finished_at desc, status);

create table if not exists collector_dead_letters (
  id bigserial primary key,
  task_id bigint references collector_tasks(id),
  task_type text not null,
  source text not null,
  entity text not null,
  params jsonb not null default '{}'::jsonb,
  attempts integer not null default 0,
  node_id text not null,
  error text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_collector_dead_letters_created
  on collector_dead_letters (created_at desc);
