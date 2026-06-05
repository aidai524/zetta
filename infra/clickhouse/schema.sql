create database if not exists zetta;

create table if not exists zetta.raw_ingest_log
(
  collected_at DateTime64(3, 'UTC'),
  source LowCardinality(String),
  entity LowCardinality(String),
  request_url String,
  raw_path String,
  payload_hash String,
  item_count UInt64
)
engine = MergeTree
partition by toYYYYMM(collected_at)
order by (source, entity, collected_at, payload_hash);

create table if not exists zetta.dim_event
(
  event_id String,
  ticker String,
  slug String,
  title String,
  description String,
  category LowCardinality(String),
  active Bool,
  closed Bool,
  archived Bool,
  start_time Nullable(DateTime64(3, 'UTC')),
  end_time Nullable(DateTime64(3, 'UTC')),
  created_at Nullable(DateTime64(3, 'UTC')),
  updated_at Nullable(DateTime64(3, 'UTC')),
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by event_id;

create table if not exists zetta.dim_market
(
  market_id String,
  condition_id String,
  question String,
  slug String,
  event_id String,
  active Bool,
  closed Bool,
  archived Bool,
  accepting_orders Bool,
  volume Float64,
  liquidity Float64,
  start_time Nullable(DateTime64(3, 'UTC')),
  end_time Nullable(DateTime64(3, 'UTC')),
  created_at Nullable(DateTime64(3, 'UTC')),
  updated_at Nullable(DateTime64(3, 'UTC')),
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by market_id;

create table if not exists zetta.dim_outcome_token
(
  token_id String,
  market_id String,
  condition_id String,
  outcome String,
  outcome_index UInt16,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by (token_id, market_id);

create table if not exists zetta.dim_series
(
  series_id String,
  ticker String,
  slug String,
  title String,
  active Bool,
  closed Bool,
  archived Bool,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by series_id;

create table if not exists zetta.dim_tag
(
  tag_id String,
  label String,
  slug String,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by tag_id;

create table if not exists zetta.bridge_event_market
(
  event_id String,
  market_id String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by (event_id, market_id);

create table if not exists zetta.bridge_event_series
(
  event_id String,
  series_id String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by (event_id, series_id);

create table if not exists zetta.bridge_event_tag
(
  event_id String,
  tag_id String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
order by (event_id, tag_id);

create table if not exists zetta.fact_trade
(
  trade_id String,
  transaction_hash String,
  log_index UInt32,
  timestamp DateTime64(3, 'UTC'),
  market_id String,
  condition_id String,
  token_id String,
  user_address String,
  side LowCardinality(String),
  price Float64,
  size Float64,
  notional Float64,
  source LowCardinality(String),
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(timestamp)
order by (market_id, token_id, timestamp, transaction_hash, log_index);

create table if not exists zetta.fact_price_history
(
  token_id String,
  timestamp DateTime64(3, 'UTC'),
  price Float64,
  source LowCardinality(String),
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(timestamp)
order by (token_id, timestamp);

create table if not exists zetta.fact_orderbook_snapshot
(
  token_id String,
  captured_at DateTime64(3, 'UTC'),
  market String,
  asset_id String,
  best_bid Nullable(Float64),
  best_ask Nullable(Float64),
  bid_depth Float64,
  ask_depth Float64,
  bids_json String,
  asks_json String,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = MergeTree
partition by toYYYYMM(captured_at)
order by (token_id, captured_at);

create table if not exists zetta.fact_position_snapshot
(
  user_address String,
  token_id String,
  market_id String,
  captured_at DateTime64(3, 'UTC'),
  size Float64,
  avg_price Nullable(Float64),
  realized_pnl Nullable(Float64),
  unrealized_pnl Nullable(Float64),
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(captured_at)
order by (user_address, token_id, captured_at);

create table if not exists zetta.fact_user_activity
(
  activity_id String,
  user_address String,
  timestamp DateTime64(3, 'UTC'),
  activity_type LowCardinality(String),
  condition_id String,
  token_id String,
  transaction_hash String,
  side LowCardinality(String),
  price Float64,
  size Float64,
  notional Float64,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(timestamp)
order by (user_address, timestamp, activity_id);

create table if not exists zetta.fact_market_holder_snapshot
(
  condition_id String,
  token_id String,
  user_address String,
  captured_at DateTime64(3, 'UTC'),
  amount Float64,
  outcome_index UInt16,
  pseudonym String,
  name String,
  verified Bool,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(captured_at)
order by (condition_id, token_id, user_address, captured_at);

create table if not exists zetta.fact_market_position_snapshot
(
  condition_id String,
  token_id String,
  user_address String,
  captured_at DateTime64(3, 'UTC'),
  size Float64,
  avg_price Float64,
  curr_price Float64,
  current_value Float64,
  cash_pnl Float64,
  realized_pnl Float64,
  total_pnl Float64,
  total_bought Float64,
  outcome String,
  outcome_index UInt16,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(captured_at)
order by (condition_id, token_id, user_address, captured_at);

create table if not exists zetta.fact_open_interest_snapshot
(
  condition_id String,
  captured_at DateTime64(3, 'UTC'),
  value Float64,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by toYYYYMM(captured_at)
order by (condition_id, captured_at);

create table if not exists zetta.fact_chain_log
(
  chain_id UInt32,
  block_number UInt64,
  block_hash String,
  transaction_hash String,
  log_index UInt64,
  address String,
  topic0 String,
  topics_json String,
  data String,
  removed Bool,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by intDiv(block_number, 100000)
order by (chain_id, block_number, transaction_hash, log_index);

create table if not exists zetta.fact_exchange_fill
(
  chain_id UInt32,
  block_number UInt64,
  transaction_hash String,
  log_index UInt64,
  market_id String,
  condition_id String,
  token_id String,
  maker String,
  taker String,
  side LowCardinality(String),
  price Float64,
  size Float64,
  notional Float64,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by intDiv(block_number, 100000)
order by (market_id, token_id, block_number, transaction_hash, log_index);

create table if not exists zetta.fact_orders_matched
(
  chain_id UInt32,
  block_number UInt64,
  transaction_hash String,
  log_index UInt64,
  taker_order_hash String,
  taker_order_maker String,
  side LowCardinality(String),
  token_id String,
  maker_amount Float64,
  taker_amount Float64,
  price Float64,
  size Float64,
  notional Float64,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by intDiv(block_number, 100000)
order by (token_id, block_number, transaction_hash, log_index);

create table if not exists zetta.fact_fee_charged
(
  chain_id UInt32,
  block_number UInt64,
  transaction_hash String,
  log_index UInt64,
  receiver String,
  amount Float64,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by intDiv(block_number, 100000)
order by (receiver, block_number, transaction_hash, log_index);

create table if not exists zetta.fact_ctf_balance_movement
(
  chain_id UInt32,
  block_number UInt64,
  transaction_hash String,
  log_index UInt64,
  batch_index UInt32,
  operator String,
  from_address String,
  to_address String,
  token_id String,
  amount Float64,
  transfer_type LowCardinality(String),
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by intDiv(block_number, 100000)
order by (token_id, block_number, transaction_hash, log_index, batch_index);

create table if not exists zetta.fact_ctf_lifecycle_event
(
  chain_id UInt32,
  block_number UInt64,
  transaction_hash String,
  log_index UInt64,
  event_type LowCardinality(String),
  stakeholder String,
  collateral_token String,
  parent_collection_id String,
  condition_id String,
  partition_json String,
  amount Float64,
  raw_json String,
  ingested_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(ingested_at)
partition by intDiv(block_number, 100000)
order by (event_type, block_number, transaction_hash, log_index);

create table if not exists zetta.mart_market_1m
(
  token_id String,
  bucket DateTime('UTC'),
  open Float64,
  high Float64,
  low Float64,
  close Float64,
  volume Float64,
  trade_count UInt64,
  best_bid Nullable(Float64),
  best_ask Nullable(Float64),
  updated_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(updated_at)
partition by toYYYYMM(bucket)
order by (token_id, bucket);

create table if not exists zetta.mart_trader_profile
(
  user_address String,
  trade_count UInt64,
  buy_count UInt64,
  sell_count UInt64,
  traded_size Float64,
  traded_notional Float64,
  position_count UInt64,
  open_position_size Float64,
  current_value Float64,
  cash_pnl Float64,
  realized_pnl Float64,
  total_pnl Float64,
  first_trade_at Nullable(DateTime64(3, 'UTC')),
  last_trade_at Nullable(DateTime64(3, 'UTC')),
  last_position_at Nullable(DateTime64(3, 'UTC')),
  chain_fill_count UInt64,
  chain_traded_size Float64,
  chain_traded_notional Float64,
  chain_position_size Float64,
  chain_current_value Float64,
  chain_net_cashflow Float64,
  chain_mark_to_market_pnl Float64,
  last_chain_fill_block UInt64,
  updated_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(updated_at)
order by user_address;

alter table zetta.mart_trader_profile add column if not exists position_count UInt64 after traded_notional;
alter table zetta.mart_trader_profile add column if not exists open_position_size Float64 after position_count;
alter table zetta.mart_trader_profile add column if not exists current_value Float64 after open_position_size;
alter table zetta.mart_trader_profile add column if not exists cash_pnl Float64 after current_value;
alter table zetta.mart_trader_profile add column if not exists realized_pnl Float64 after cash_pnl;
alter table zetta.mart_trader_profile add column if not exists total_pnl Float64 after realized_pnl;
alter table zetta.mart_trader_profile add column if not exists last_position_at Nullable(DateTime64(3, 'UTC')) after last_trade_at;
alter table zetta.mart_trader_profile add column if not exists chain_fill_count UInt64 after last_position_at;
alter table zetta.mart_trader_profile add column if not exists chain_traded_size Float64 after chain_fill_count;
alter table zetta.mart_trader_profile add column if not exists chain_traded_notional Float64 after chain_traded_size;
alter table zetta.mart_trader_profile add column if not exists chain_position_size Float64 after chain_traded_notional;
alter table zetta.mart_trader_profile add column if not exists chain_current_value Float64 after chain_position_size;
alter table zetta.mart_trader_profile add column if not exists chain_net_cashflow Float64 after chain_current_value;
alter table zetta.mart_trader_profile add column if not exists chain_mark_to_market_pnl Float64 after chain_net_cashflow;
alter table zetta.mart_trader_profile add column if not exists last_chain_fill_block UInt64 after chain_mark_to_market_pnl;

create table if not exists zetta.mart_trader_chain_pnl
(
  user_address String,
  chain_fill_count UInt64,
  chain_traded_size Float64,
  chain_traded_notional Float64,
  chain_position_size Float64,
  chain_current_value Float64,
  chain_net_cashflow Float64,
  chain_mark_to_market_pnl Float64,
  last_chain_fill_block UInt64,
  updated_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(updated_at)
order by user_address;

create table if not exists zetta.mart_alert
(
  alert_id String,
  alert_type LowCardinality(String),
  severity LowCardinality(String),
  token_id String,
  market_id String,
  user_address String,
  occurred_at DateTime64(3, 'UTC'),
  metric_name LowCardinality(String),
  metric_value Float64,
  threshold Float64,
  message String,
  raw_json String,
  updated_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(updated_at)
partition by toYYYYMM(occurred_at)
order by (alert_type, token_id, occurred_at, alert_id);

create table if not exists zetta.mart_trade_reconciliation
(
  reconciliation_id String,
  transaction_hash String,
  token_id String,
  data_trade_id String,
  chain_log_index UInt64,
  status LowCardinality(String),
  side_data String,
  side_chain String,
  price_data Nullable(Float64),
  price_chain Nullable(Float64),
  size_data Nullable(Float64),
  size_chain Nullable(Float64),
  notional_data Nullable(Float64),
  notional_chain Nullable(Float64),
  price_delta Nullable(Float64),
  size_delta Nullable(Float64),
  notional_delta Nullable(Float64),
  checked_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(checked_at)
partition by toYYYYMM(checked_at)
order by (status, transaction_hash, token_id, reconciliation_id);

create table if not exists zetta.mart_settlement_audit
(
  condition_id String,
  market_id String,
  question String,
  market_closed Bool,
  redeem_count UInt64,
  redeemed_amount Float64,
  first_redeem_block UInt64,
  last_redeem_block UInt64,
  status LowCardinality(String),
  checked_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(checked_at)
order by (status, condition_id);

create table if not exists zetta.mart_collector_health
(
  bucket DateTime('UTC'),
  node_id String,
  status LowCardinality(String),
  runs UInt64,
  pages UInt64,
  items UInt64,
  errors UInt64,
  updated_at DateTime64(3, 'UTC')
)
engine = ReplacingMergeTree(updated_at)
partition by toYYYYMM(bucket)
order by (bucket, node_id, status);
