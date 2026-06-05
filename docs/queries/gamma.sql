-- Event discovery by category and recency.
select
  category,
  count() as events,
  max(updated_at) as latest_update
from dim_event
group by category
order by events desc;

-- Current market metadata with outcome tokens.
select
  m.market_id,
  m.event_id,
  m.question,
  groupArray((t.outcome_index, t.outcome, t.token_id)) as outcomes
from dim_market as m
left join dim_outcome_token as t on m.market_id = t.market_id
group by
  m.market_id,
  m.event_id,
  m.question
order by m.market_id
limit 100;

-- Event to series and tag context.
select
  e.event_id,
  e.title,
  groupArrayDistinct(s.title) as series,
  groupArrayDistinct(tag.label) as tags
from dim_event as e
left join bridge_event_series as es on e.event_id = es.event_id
left join dim_series as s on es.series_id = s.series_id
left join bridge_event_tag as et on e.event_id = et.event_id
left join dim_tag as tag on et.tag_id = tag.tag_id
group by
  e.event_id,
  e.title
order by e.event_id
limit 100;

-- Active markets with available CLOB token IDs.
select
  m.market_id,
  m.question,
  m.volume,
  m.liquidity,
  groupArray(t.token_id) as token_ids
from dim_market as m
inner join dim_outcome_token as t on m.market_id = t.market_id
where m.active = true and m.closed = false
group by
  m.market_id,
  m.question,
  m.volume,
  m.liquidity
order by m.volume desc
limit 100;

-- Deduplicated CLOB price history counts.
select
  token_id,
  uniqExact(timestamp) as points,
  min(timestamp) as first_seen,
  max(timestamp) as last_seen,
  min(price) as min_price,
  max(price) as max_price
from fact_price_history
group by token_id
order by points desc
limit 100;

-- Recent CLOB order book snapshots.
select
  token_id,
  captured_at,
  best_bid,
  best_ask,
  bid_depth,
  ask_depth
from fact_orderbook_snapshot
order by captured_at desc
limit 100;

-- Recent Data API trades.
select
  timestamp,
  side,
  token_id,
  user_address,
  price,
  size,
  notional
from fact_trade
order by timestamp desc
limit 100;

-- 1 minute price candles.
select
  token_id,
  bucket,
  open,
  high,
  low,
  close,
  trade_count
from mart_market_1m
order by bucket desc
limit 100;

-- Trader profile mart.
select
  user_address,
  trade_count,
  buy_count,
  sell_count,
  traded_size,
  traded_notional,
  position_count,
  current_value,
  cash_pnl,
  realized_pnl,
  total_pnl,
  chain_fill_count,
  chain_current_value,
  chain_net_cashflow,
  chain_mark_to_market_pnl,
  first_trade_at,
  last_trade_at,
  last_position_at
from mart_trader_profile final
order by traded_notional desc
limit 100;

-- Chain-derived trader PnL.
select
  user_address,
  chain_fill_count,
  chain_traded_notional,
  chain_position_size,
  chain_current_value,
  chain_net_cashflow,
  chain_mark_to_market_pnl,
  last_chain_fill_block
from mart_trader_chain_pnl final
order by abs(chain_mark_to_market_pnl) desc
limit 100;

-- Market position snapshots.
select
  condition_id,
  token_id,
  user_address,
  size,
  avg_price,
  curr_price,
  total_pnl
from fact_market_position_snapshot
order by total_pnl desc
limit 100;

-- Recent real-time alerts.
select
  alert_type,
  severity,
  token_id,
  user_address,
  metric_name,
  metric_value,
  threshold,
  message,
  occurred_at
from mart_alert
order by occurred_at desc
limit 100;

-- Raw Polygon logs by contract/topic.
select
  block_number,
  transaction_hash,
  log_index,
  address,
  topic0
from fact_chain_log
order by block_number desc, log_index asc
limit 100;

-- Decoded Polymarket exchange fills from Polygon logs.
select
  block_number,
  transaction_hash,
  log_index,
  side,
  token_id,
  maker,
  taker,
  price,
  size,
  notional
from fact_exchange_fill final
order by block_number desc, log_index asc
limit 100;

-- Decoded CTF Exchange matched order summaries.
select
  block_number,
  transaction_hash,
  log_index,
  side,
  token_id,
  taker_order_maker,
  price,
  size,
  notional
from fact_orders_matched final
order by block_number desc, log_index asc
limit 100;

-- Decoded CTF Exchange fees.
select
  block_number,
  transaction_hash,
  log_index,
  receiver,
  amount
from fact_fee_charged final
order by block_number desc, log_index asc
limit 100;

-- Decoded CTF ERC1155 balance movements.
select
  block_number,
  transaction_hash,
  log_index,
  batch_index,
  transfer_type,
  from_address,
  to_address,
  token_id,
  amount
from fact_ctf_balance_movement final
order by block_number desc, log_index asc, batch_index asc
limit 100;

-- Decoded CTF lifecycle events: split, merge, redeem.
select
  event_type,
  block_number,
  transaction_hash,
  log_index,
  stakeholder,
  collateral_token,
  condition_id,
  partition_json,
  amount
from fact_ctf_lifecycle_event final
order by block_number desc, log_index asc
limit 100;

-- Data API trade vs chain fill reconciliation.
select
  status,
  transaction_hash,
  token_id,
  side_data,
  side_chain,
  price_data,
  price_chain,
  size_data,
  size_chain,
  price_delta,
  size_delta
from mart_trade_reconciliation final
order by checked_at desc, status
limit 100;

-- Settlement and redeem audit status by condition.
select
  status,
  condition_id,
  market_id,
  question,
  market_closed,
  redeem_count,
  redeemed_amount,
  first_redeem_block,
  last_redeem_block
from mart_settlement_audit final
order by checked_at desc, status
limit 100;

-- Collector run health by node and minute.
select
  bucket,
  node_id,
  status,
  runs,
  pages,
  items,
  errors
from mart_collector_health final
order by bucket desc, node_id, status
limit 100;

-- Product API backing query: market search.
select
  market_id,
  event_id,
  question,
  active,
  closed,
  volume,
  liquidity
from dim_market final
where positionCaseInsensitive(question, 'election') > 0
order by volume desc
limit 25;
