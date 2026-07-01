from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from zetta.storage.clickhouse import ClickHouseWriter


@dataclass(frozen=True)
class MartBuildResult:
    mart: str
    rows: int


class MartBuilder:
    def __init__(self, *, clickhouse: ClickHouseWriter) -> None:
        self.clickhouse = clickhouse

    def build_market_1m(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_market_1m
            select
              token_id,
              toStartOfMinute(timestamp) as bucket,
              argMin(price, timestamp) as open,
              max(price) as high,
              min(price) as low,
              argMax(price, timestamp) as close,
              0.0 as volume,
              count() as trade_count,
              cast(null, 'Nullable(Float64)') as best_bid,
              cast(null, 'Nullable(Float64)') as best_ask,
              now64(3) as updated_at
            from
            (
              select
                token_id,
                timestamp,
                anyLast(price) as price
              from fact_price_history
              group by token_id, timestamp
            )
            group by token_id, bucket
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_market_1m").strip() or "0")
        return MartBuildResult(mart="market_1m", rows=rows)

    def build_trader_profiles(self) -> MartBuildResult:
        self.build_trader_chain_pnl()
        self.clickhouse.execute(
            """
            insert into mart_trader_profile
            (
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              position_count,
              open_position_size,
              current_value,
              cash_pnl,
              realized_pnl,
              total_pnl,
              first_trade_at,
              last_trade_at,
              last_position_at,
              chain_fill_count,
              chain_traded_size,
              chain_traded_notional,
              chain_position_size,
              chain_current_value,
              chain_net_cashflow,
              chain_mark_to_market_pnl,
              last_chain_fill_block,
              updated_at
            )
            select
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              position_count,
              open_position_size,
              current_value,
              cash_pnl,
              realized_pnl,
              total_pnl,
              first_trade_at,
              last_trade_at,
              last_position_at,
              chain_fill_count,
              chain_traded_size,
              chain_traded_notional,
              chain_position_size,
              chain_current_value,
              chain_net_cashflow,
              chain_mark_to_market_pnl,
              last_chain_fill_block,
              now64(3) as updated_at
            from
            (
              select
                user_address,
                sum(trade_count) as trade_count,
                sum(buy_count) as buy_count,
                sum(sell_count) as sell_count,
                sum(traded_size) as traded_size,
                sum(traded_notional) as traded_notional,
                sum(position_count) as position_count,
                sum(open_position_size) as open_position_size,
                sum(current_value) as current_value,
                sum(cash_pnl) as cash_pnl,
                sum(realized_pnl) as realized_pnl,
                sum(total_pnl) as total_pnl,
                min(first_trade_at) as first_trade_at,
                max(last_trade_at) as last_trade_at,
                max(last_position_at) as last_position_at,
                sum(chain_fill_count) as chain_fill_count,
                sum(chain_traded_size) as chain_traded_size,
                sum(chain_traded_notional) as chain_traded_notional,
                sum(chain_position_size) as chain_position_size,
                sum(chain_current_value) as chain_current_value,
                sum(chain_net_cashflow) as chain_net_cashflow,
                sum(chain_mark_to_market_pnl) as chain_mark_to_market_pnl,
                max(last_chain_fill_block) as last_chain_fill_block
              from
              (
                select
                  trader as user_address,
                  count() as trade_count,
                  countIf(side = 'BUY') as buy_count,
                  countIf(side = 'SELL') as sell_count,
                  sum(size) as traded_size,
                  sum(notional) as traded_notional,
                  min(timestamp) as first_trade_at,
                  max(timestamp) as last_trade_at,
                  0 as position_count,
                  0.0 as open_position_size,
                  0.0 as current_value,
                  0.0 as cash_pnl,
                  0.0 as realized_pnl,
                  0.0 as total_pnl,
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))') as last_position_at,
                  0 as chain_fill_count,
                  0.0 as chain_traded_size,
                  0.0 as chain_traded_notional,
                  0.0 as chain_position_size,
                  0.0 as chain_current_value,
                  0.0 as chain_net_cashflow,
                  0.0 as chain_mark_to_market_pnl,
                  0 as last_chain_fill_block
                from
                (
                  select
                    lower(user_address) as trader,
                    transaction_hash,
                    condition_id,
                    token_id,
                    timestamp,
                    side,
                    price,
                    size,
                    notional
                  from
                  (
                    select
                      normalized_user_address as user_address,
                      transaction_hash,
                      condition_id,
                      token_id,
                      timestamp,
                      side,
                      price,
                      size,
                      anyLast(notional) as notional
                    from
                    (
                      select
                        lower(user_address) as normalized_user_address,
                        transaction_hash,
                        condition_id,
                        token_id,
                        timestamp,
                        side,
                        price,
                        size,
                        notional
                      from fact_trade
                      where user_address != ''
                        and timestamp <= now64(3) + interval 10 minute
                    )
                    group by
                      normalized_user_address,
                      transaction_hash,
                      condition_id,
                      token_id,
                      timestamp,
                      side,
                      price,
                      size
                  )
                )
                where trader != ''
                group by trader
                union all
                select
                  user_address,
                  0 as trade_count,
                  0 as buy_count,
                  0 as sell_count,
                  0.0 as traded_size,
                  0.0 as traded_notional,
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))') as first_trade_at,
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))') as last_trade_at,
                  count() as position_count,
                  sum(size) as open_position_size,
                  sum(current_value) as current_value,
                  sum(cash_pnl) as cash_pnl,
                  sum(realized_pnl) as realized_pnl,
                  sum(total_pnl) as total_pnl,
                  max(latest_captured_at) as last_position_at,
                  0 as chain_fill_count,
                  0.0 as chain_traded_size,
                  0.0 as chain_traded_notional,
                  0.0 as chain_position_size,
                  0.0 as chain_current_value,
                  0.0 as chain_net_cashflow,
                  0.0 as chain_mark_to_market_pnl,
                  0 as last_chain_fill_block
                from
                (
                  select
                    user_address,
                    token_id,
                    argMax(size, captured_at) as size,
                    argMax(current_value, captured_at) as current_value,
                    argMax(cash_pnl, captured_at) as cash_pnl,
                    argMax(realized_pnl, captured_at) as realized_pnl,
                    argMax(total_pnl, captured_at) as total_pnl,
                    max(captured_at) as latest_captured_at
                  from fact_market_position_snapshot
                  where user_address != ''
                  group by user_address, token_id
                )
                group by user_address
                union all
                select
                  user_address,
                  0 as trade_count,
                  0 as buy_count,
                  0 as sell_count,
                  0.0 as traded_size,
                  0.0 as traded_notional,
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))') as first_trade_at,
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))') as last_trade_at,
                  0 as position_count,
                  0.0 as open_position_size,
                  0.0 as current_value,
                  0.0 as cash_pnl,
                  0.0 as realized_pnl,
                  0.0 as total_pnl,
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))') as last_position_at,
                  chain_fill_count,
                  chain_traded_size,
                  chain_traded_notional,
                  chain_position_size,
                  chain_current_value,
                  chain_net_cashflow,
                  chain_mark_to_market_pnl,
                  last_chain_fill_block
                from mart_trader_chain_pnl final
              )
              group by user_address
            )
            where user_address != ''
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_trader_profile").strip() or "0")
        return MartBuildResult(mart="trader_profile", rows=rows)

    def build_wallet_trade_rollup(self, *, since_hours: int = 48) -> MartBuildResult:
        if since_hours <= 0:
            raise ValueError("since_hours must be positive")
        self.clickhouse.execute(
            f"""
            insert into mart_wallet_trade_rollup
            select
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              buy_notional,
              sell_notional,
              first_trade_at,
              last_trade_at,
              traded_notional_24h,
              trade_count_24h,
              buy_notional_24h,
              sell_notional_24h,
              sell_notional_24h - buy_notional_24h as net_notional_24h,
              latest_action,
              multiIf(
                traded_notional >= 10000000, '10m_plus',
                traded_notional >= 5000000, '5m_plus',
                traded_notional >= 1000000, '1m_plus',
                traded_notional >= 100000, '100k_plus',
                'standard'
              ) as whale_tier,
              greatest(0, toUInt32(dateDiff('second', last_trade_at, now64(3)))) as data_lag_seconds,
              now64(3) as updated_at
            from
            (
              select
                user_address,
                count() as trade_count,
                countIf(side = 'BUY') as buy_count,
                countIf(side = 'SELL') as sell_count,
                sum(size) as traded_size,
                sum(notional) as traded_notional,
                sumIf(notional, side = 'BUY') as buy_notional,
                sumIf(notional, side = 'SELL') as sell_notional,
                min(timestamp) as first_trade_at,
                max(timestamp) as last_trade_at,
                sumIf(notional, timestamp >= now64(3) - interval 24 hour) as traded_notional_24h,
                countIf(timestamp >= now64(3) - interval 24 hour) as trade_count_24h,
                sumIf(notional, side = 'BUY' and timestamp >= now64(3) - interval 24 hour)
                  as buy_notional_24h,
                sumIf(notional, side = 'SELL' and timestamp >= now64(3) - interval 24 hour)
                  as sell_notional_24h,
                argMax(side, timestamp) as latest_action
              from
              (
                select
                  normalized_user_address as user_address,
                  transaction_hash,
                  condition_id,
                  token_id,
                  timestamp,
                  side,
                  price,
                  size,
                  anyLast(notional) as notional
                from
                (
                  select
                    lower(user_address) as normalized_user_address,
                    transaction_hash,
                    condition_id,
                    token_id,
                    timestamp,
                    side,
                    price,
                    size,
                    notional
                  from fact_trade_by_time
                  where user_address != ''
                    and timestamp <= now64(3) + interval 10 minute
                    and timestamp >= now64(3) - interval {since_hours} hour
                )
                group by
                  normalized_user_address,
                  transaction_hash,
                  condition_id,
                  token_id,
                  timestamp,
                  side,
                  price,
                  size
              )
              group by user_address
            )
            """
        )
        rows = int(
            self.clickhouse.query_text("select count() from mart_wallet_trade_rollup").strip()
            or "0"
        )
        return MartBuildResult(mart="wallet_trade_rollup", rows=rows)

    def build_wallet_screener(self) -> MartBuildResult:
        self.clickhouse.execute("truncate table mart_wallet_screener_next")
        self.clickhouse.execute(
            """
            insert into mart_wallet_screener_next
            (
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              max_single_trade_notional,
              first_trade_at,
              last_trade_at,
              position_count,
              positions_value,
              portfolio_value,
              available_balance,
              total_pnl,
              portfolio_captured_at,
              pnl_captured_at,
              pnl_roi,
              is_whale,
              is_smart,
              whale_reason,
              updated_at
            )
            select
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              max_single_trade_notional,
              first_trade_at,
              last_trade_at,
              position_count,
              positions_value,
              portfolio_value,
              available_balance,
              total_pnl,
              portfolio_captured_at,
              pnl_captured_at,
              pnl_roi,
              traded_notional >= 1000000 or max_single_trade_notional >= 100000 as is_whale,
              traded_notional >= 10000 and pnl_roi >= 0.55 as is_smart,
              multiIf(
                traded_notional >= 1000000 and max_single_trade_notional >= 100000,
                  'total_volume_and_single_trade',
                traded_notional >= 1000000, 'total_volume',
                max_single_trade_notional >= 100000, 'single_trade',
                ''
              ) as whale_reason,
              now64(3) as updated_at
            from
            (
              select
                trades.user_address as user_address,
                trades.trade_count as trade_count,
                trades.buy_count as buy_count,
                trades.sell_count as sell_count,
                trades.traded_size as traded_size,
                trades.traded_notional as traded_notional,
                trades.max_single_trade_notional as max_single_trade_notional,
                trades.first_trade_at as first_trade_at,
                trades.last_trade_at as last_trade_at,
                ifNull(portfolio.position_count, 0) as position_count,
                ifNull(portfolio.positions_value, 0.0) as positions_value,
                ifNull(portfolio.portfolio_value, 0.0) as portfolio_value,
                ifNull(portfolio.available_balance, 0.0) as available_balance,
                ifNull(pnl.total_pnl, 0.0) as total_pnl,
                if(
                  portfolio.user_address = '',
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))'),
                  portfolio.portfolio_captured_at
                ) as portfolio_captured_at,
                if(
                  pnl.user_address = '' or pnl.pnl_captured_at = toDateTime64(0, 3, 'UTC'),
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))'),
                  pnl.pnl_captured_at
                ) as pnl_captured_at,
                if(trades.traded_notional = 0, 0.0, ifNull(pnl.total_pnl, 0.0) / trades.traded_notional)
                  as pnl_roi
              from
              (
                select
                  coalesce(profile.user_address, rollup.user_address) as user_address,
                  if(
                    isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                    ifNull(profile.trade_count, 0) + ifNull(rollup.trade_count, 0),
                    greatest(ifNull(profile.trade_count, 0), ifNull(rollup.trade_count, 0))
                  ) as trade_count,
                  if(
                    isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                    ifNull(profile.buy_count, 0) + ifNull(rollup.buy_count, 0),
                    greatest(ifNull(profile.buy_count, 0), ifNull(rollup.buy_count, 0))
                  ) as buy_count,
                  if(
                    isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                    ifNull(profile.sell_count, 0) + ifNull(rollup.sell_count, 0),
                    greatest(ifNull(profile.sell_count, 0), ifNull(rollup.sell_count, 0))
                  ) as sell_count,
                  if(
                    isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                    ifNull(profile.traded_size, 0.0) + ifNull(rollup.traded_size, 0.0),
                    greatest(ifNull(profile.traded_size, 0.0), ifNull(rollup.traded_size, 0.0))
                  ) as traded_size,
                  if(
                    isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                    ifNull(profile.traded_notional, 0.0) + ifNull(rollup.traded_notional, 0.0),
                    greatest(ifNull(profile.traded_notional, 0.0), ifNull(rollup.traded_notional, 0.0))
                  ) as traded_notional,
                  ifNull(high_single.max_single_trade_notional, 0.0) as max_single_trade_notional,
                  multiIf(
                    isNull(profile.first_trade_at), rollup.first_trade_at,
                    isNull(rollup.first_trade_at), profile.first_trade_at,
                    least(profile.first_trade_at, rollup.first_trade_at)
                  ) as first_trade_at,
                  multiIf(
                    isNull(profile.last_trade_at), rollup.last_trade_at,
                    isNull(rollup.last_trade_at), profile.last_trade_at,
                    greatest(profile.last_trade_at, rollup.last_trade_at)
                  ) as last_trade_at
                from mart_trader_profile as profile final
                full outer join mart_wallet_trade_rollup as rollup final
                  on profile.user_address = rollup.user_address
                left join
                (
                  select
                    normalized_user_address as user_address,
                    max(notional) as max_single_trade_notional
                  from
                  (
                    select
                      lower(user_address) as normalized_user_address,
                      notional
                    from fact_trade
                    where user_address != ''
                      and notional >= 100000
                      and timestamp <= now64(3) + interval 10 minute
                  )
                  group by normalized_user_address
                ) as high_single
                  on coalesce(profile.user_address, rollup.user_address) = high_single.user_address
              ) as trades
              left join
              (
                select
                  normalized_user_address as user_address,
                  argMax(position_count, captured_at) as position_count,
                  argMax(positions_value, captured_at) as positions_value,
                  argMax(portfolio_value, captured_at) as portfolio_value,
                  argMax(available_balance, captured_at) as available_balance,
                  argMax(total_pnl, captured_at) as total_pnl,
                  max(captured_at) as portfolio_captured_at
                from
                (
                  select
                    lower(user_address) as normalized_user_address,
                    position_count,
                    positions_value,
                    portfolio_value,
                    available_balance,
                    total_pnl,
                    captured_at
                  from fact_wallet_portfolio_snapshot
                  where user_address != ''
                )
                group by normalized_user_address
              ) as portfolio
                on trades.user_address = portfolio.user_address
              left join
              (
                select
                  coalesce(portfolio.user_address, pnl_only.user_address) as user_address,
                  if(
                    ifNull(pnl_only.pnl_captured_at, toDateTime64(0, 3, 'UTC'))
                      > ifNull(portfolio.portfolio_captured_at, toDateTime64(0, 3, 'UTC')),
                    pnl_only.total_pnl,
                    portfolio.total_pnl
                  ) as total_pnl,
                  greatest(
                    ifNull(portfolio.portfolio_captured_at, toDateTime64(0, 3, 'UTC')),
                    ifNull(pnl_only.pnl_captured_at, toDateTime64(0, 3, 'UTC'))
                  ) as pnl_captured_at
                from
                (
                  select
                    normalized_user_address as user_address,
                    argMax(total_pnl, captured_at) as total_pnl,
                    max(captured_at) as portfolio_captured_at
                  from
                  (
                    select
                      lower(user_address) as normalized_user_address,
                      total_pnl,
                      captured_at
                    from fact_wallet_portfolio_snapshot
                    where user_address != ''
                  )
                  group by normalized_user_address
                ) as portfolio
                full outer join
                (
                  select
                    normalized_user_address as user_address,
                    argMax(total_pnl, captured_at) as total_pnl,
                    max(captured_at) as pnl_captured_at
                  from
                  (
                    select
                      lower(user_address) as normalized_user_address,
                      total_pnl,
                      captured_at
                    from fact_wallet_pnl_snapshot
                    where user_address != ''
                  )
                  group by normalized_user_address
                ) as pnl_only
                  on portfolio.user_address = pnl_only.user_address
              ) as pnl
                on trades.user_address = pnl.user_address
              where trades.user_address != ''
            )
            settings join_use_nulls = 1, max_threads = 4
            """
        )
        self.clickhouse.execute("exchange tables mart_wallet_screener and mart_wallet_screener_next")
        rows = int(
            self.clickhouse.query_text("select count() from mart_wallet_screener final").strip()
            or "0"
        )
        return MartBuildResult(mart="wallet_screener", rows=rows)

    def build_wallet_fifa_24h_pnl(self, *, window_hours: int = 24) -> MartBuildResult:
        if window_hours <= 0:
            raise ValueError("window_hours must be positive")
        self.build_fifa_trades(window_hours=max(192, window_hours * 2))
        self.build_wallet_fifa_chain_cashflow(window_days=max(7, (window_hours + 23) // 24))
        self.clickhouse.execute("truncate table mart_wallet_fifa_24h_pnl_next")
        self.clickhouse.execute(
            f"""
            insert into mart_wallet_fifa_24h_pnl_next
            (
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              buy_notional,
              sell_notional,
              trade_count_24h,
              buy_notional_24h,
              sell_notional_24h,
              traded_notional_24h,
              net_notional_24h,
              event_count,
              market_count,
              event_count_24h,
              market_count_24h,
              token_count,
              open_position_count,
              open_position_value_now,
              open_position_value_24h_ago,
              open_position_value_7d_ago,
              equity_now,
              equity_24h_ago,
              equity_7d_ago,
              total_pnl,
              total_pnl_roi,
              pnl_24h,
              pnl_base_24h,
              pnl_roi_24h,
              pnl_7d,
              pnl_base_7d,
              pnl_roi_7d,
              profitable_token_count,
              losing_token_count,
              win_rate,
              profitable_token_count_24h,
              losing_token_count_24h,
              win_rate_24h,
              profitable_token_count_7d,
              losing_token_count_7d,
              win_rate_7d,
              first_trade_at,
              last_trade_at,
              latest_action,
              missing_mark_count,
              negative_position_count,
              data_quality,
              updated_at
            )
            with
              now64(3) as as_of,
              as_of - toIntervalHour({window_hours}) as since,
              as_of - toIntervalDay(7) as since_7d,
              fifa_tokens as
              (
                select
                  token_id,
                  anyLast(market_id) as market_id
                from mart_fifa_trade final
                where token_id != ''
                group by token_id
              ),
              chain_cashflows as
              (
                select
                  user_address,
                  token_id,
                  anyLast(redeemed_value_now) as redeemed_value_now,
                  anyLast(redeemed_value_24h_ago) as redeemed_value_24h_ago,
                  anyLast(redeemed_value_7d_ago) as redeemed_value_7d_ago,
                  anyLast(fee_now) as fee_now,
                  anyLast(fee_24h_ago) as fee_24h_ago,
                  anyLast(fee_7d_ago) as fee_7d_ago
                from mart_wallet_fifa_chain_cashflow final
                group by user_address, token_id
              ),
              final_prices_now as
              (
                select
                  resolved.market_id as market_id,
                  tokens.token_id as token_id,
                  resolved.final_price as final_price
                from
                (
                  select
                    market_id,
                    condition_id,
                    price_index - 1 as outcome_index,
                    toFloat64OrZero(JSONExtractString(price_raw)) as final_price
                  from
                  (
                    select
                      market_id,
                      condition_id,
                      JSONExtractString(raw_json, 'outcomePrices') as prices
                    from dim_market as markets final
                    where market_id in (select market_id from fifa_tokens)
                      and closed = true
                      and JSONExtractString(raw_json, 'outcomePrices') != ''
                  )
                  array join
                    JSONExtractArrayRaw(prices) as price_raw,
                    arrayEnumerate(JSONExtractArrayRaw(prices)) as price_index
                ) as resolved
                inner join dim_outcome_token as tokens final
                  on tokens.market_id = resolved.market_id
                 and tokens.outcome_index = resolved.outcome_index
              ),
              final_prices_ago as
              (
                select
                  resolved.market_id as market_id,
                  tokens.token_id as token_id,
                  resolved.final_price as final_price
                from
                (
                  select
                    market_id,
                    condition_id,
                    price_index - 1 as outcome_index,
                    toFloat64OrZero(JSONExtractString(price_raw)) as final_price
                  from
                  (
                    select
                      market_id,
                      condition_id,
                      JSONExtractString(raw_json, 'outcomePrices') as prices
                    from dim_market as markets final
                    where market_id in (select market_id from fifa_tokens)
                      and closed = true
                      and end_time <= since
                      and JSONExtractString(raw_json, 'outcomePrices') != ''
                  )
                  array join
                    JSONExtractArrayRaw(prices) as price_raw,
                    arrayEnumerate(JSONExtractArrayRaw(prices)) as price_index
                ) as resolved
                inner join dim_outcome_token as tokens final
                  on tokens.market_id = resolved.market_id
                 and tokens.outcome_index = resolved.outcome_index
              ),
              final_prices_7d_ago as
              (
                select
                  resolved.market_id as market_id,
                  tokens.token_id as token_id,
                  resolved.final_price as final_price
                from
                (
                  select
                    market_id,
                    condition_id,
                    price_index - 1 as outcome_index,
                    toFloat64OrZero(JSONExtractString(price_raw)) as final_price
                  from
                  (
                    select
                      market_id,
                      condition_id,
                      JSONExtractString(raw_json, 'outcomePrices') as prices
                    from dim_market as markets final
                    where market_id in (select market_id from fifa_tokens)
                      and closed = true
                      and end_time <= since_7d
                      and JSONExtractString(raw_json, 'outcomePrices') != ''
                  )
                  array join
                    JSONExtractArrayRaw(prices) as price_raw,
                    arrayEnumerate(JSONExtractArrayRaw(prices)) as price_index
                ) as resolved
                inner join dim_outcome_token as tokens final
                  on tokens.market_id = resolved.market_id
                 and tokens.outcome_index = resolved.outcome_index
              ),
              marks as
              (
                select
                  token_ids.token_id as token_id,
                  multiIf(
                    final_now.final_price is not null,
                      cast(final_now.final_price, 'Nullable(Float64)'),
                    book_marks.book_now_count > 0,
                      cast((book_marks.book_best_bid_now + book_marks.book_best_ask_now) / 2, 'Nullable(Float64)'),
                    price_marks.price_now_count > 0,
                      cast(price_marks.price_now, 'Nullable(Float64)'),
                    cast(null, 'Nullable(Float64)')
                  ) as mark_price_now,
                  multiIf(
                    final_ago.final_price is not null,
                      cast(final_ago.final_price, 'Nullable(Float64)'),
                    book_marks.book_ago_count > 0,
                      cast((book_marks.book_best_bid_ago + book_marks.book_best_ask_ago) / 2, 'Nullable(Float64)'),
                    price_marks.price_ago_count > 0,
                      cast(price_marks.price_ago, 'Nullable(Float64)'),
                    cast(null, 'Nullable(Float64)')
                  ) as mark_price_24h_ago,
                  multiIf(
                    final_7d.final_price is not null,
                      cast(final_7d.final_price, 'Nullable(Float64)'),
                    book_marks.book_7d_count > 0,
                      cast((book_marks.book_best_bid_7d + book_marks.book_best_ask_7d) / 2, 'Nullable(Float64)'),
                    price_marks.price_7d_count > 0,
                      cast(price_marks.price_7d, 'Nullable(Float64)'),
                    cast(null, 'Nullable(Float64)')
                  ) as mark_price_7d_ago
                from fifa_tokens as token_ids
                left join final_prices_now as final_now
                  on token_ids.token_id = final_now.token_id
                left join final_prices_ago as final_ago
                  on token_ids.token_id = final_ago.token_id
                left join final_prices_7d_ago as final_7d
                  on token_ids.token_id = final_7d.token_id
                left join
                (
                  select
                    token_id,
                    argMaxIf(price, timestamp, timestamp <= as_of + toIntervalMinute(10))
                      as price_now,
                    countIf(timestamp <= as_of + toIntervalMinute(10)) as price_now_count,
                    argMaxIf(price, timestamp, timestamp <= since) as price_ago,
                    countIf(timestamp <= since) as price_ago_count,
                    argMaxIf(price, timestamp, timestamp <= since_7d) as price_7d,
                    countIf(timestamp <= since_7d) as price_7d_count
                  from fact_price_history
                  where token_id in (select token_id from fifa_tokens)
                    and timestamp <= as_of + toIntervalMinute(10)
                  group by token_id
                ) as price_marks on token_ids.token_id = price_marks.token_id
                left join
                (
                  select
                    token_id,
                    argMaxIf(best_bid, captured_at, captured_at <= as_of + toIntervalMinute(10))
                      as book_best_bid_now,
                    argMaxIf(best_ask, captured_at, captured_at <= as_of + toIntervalMinute(10))
                      as book_best_ask_now,
                    countIf(captured_at <= as_of + toIntervalMinute(10)) as book_now_count,
                    argMaxIf(best_bid, captured_at, captured_at <= since) as book_best_bid_ago,
                    argMaxIf(best_ask, captured_at, captured_at <= since) as book_best_ask_ago,
                    countIf(captured_at <= since) as book_ago_count,
                    argMaxIf(best_bid, captured_at, captured_at <= since_7d) as book_best_bid_7d,
                    argMaxIf(best_ask, captured_at, captured_at <= since_7d) as book_best_ask_7d,
                    countIf(captured_at <= since_7d) as book_7d_count
                  from fact_orderbook_snapshot
                  where token_id in (select token_id from fifa_tokens)
                    and best_bid is not null
                    and best_ask is not null
                    and captured_at <= as_of + toIntervalMinute(10)
                  group by token_id
                ) as book_marks on token_ids.token_id = book_marks.token_id
              )
            select
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              buy_notional,
              sell_notional,
              trade_count_24h,
              buy_notional_24h,
              sell_notional_24h,
              traded_notional_24h,
              sell_notional_24h - buy_notional_24h as net_notional_24h,
              event_count,
              market_count,
              event_count_24h,
              market_count_24h,
              token_count,
              open_position_count,
              open_position_value_now,
              open_position_value_24h_ago,
              open_position_value_7d_ago,
              equity_now,
              equity_24h_ago,
              equity_7d_ago,
              equity_now as total_pnl,
              if(buy_notional = 0, 0.0, equity_now / buy_notional) as total_pnl_roi,
              equity_now - equity_24h_ago as pnl_24h,
              open_position_value_24h_ago + buy_notional_24h as pnl_base_24h,
              if(
                open_position_value_24h_ago + buy_notional_24h = 0,
                0.0,
                (equity_now - equity_24h_ago)
                  / (open_position_value_24h_ago + buy_notional_24h)
              ) as pnl_roi_24h,
              equity_now - equity_7d_ago as pnl_7d,
              open_position_value_7d_ago + buy_notional_7d as pnl_base_7d,
              if(
                open_position_value_7d_ago + buy_notional_7d = 0,
                0.0,
                (equity_now - equity_7d_ago)
                  / (open_position_value_7d_ago + buy_notional_7d)
              ) as pnl_roi_7d,
              profitable_token_count,
              losing_token_count,
              if(
                profitable_token_count + losing_token_count = 0,
                0.0,
                profitable_token_count / (profitable_token_count + losing_token_count)
              ) as win_rate,
              profitable_token_count_24h,
              losing_token_count_24h,
              if(
                profitable_token_count_24h + losing_token_count_24h = 0,
                0.0,
                profitable_token_count_24h
                  / (profitable_token_count_24h + losing_token_count_24h)
              ) as win_rate_24h,
              profitable_token_count_7d,
              losing_token_count_7d,
              if(
                profitable_token_count_7d + losing_token_count_7d = 0,
                0.0,
                profitable_token_count_7d
                  / (profitable_token_count_7d + losing_token_count_7d)
              ) as win_rate_7d,
              first_trade_at,
              last_trade_at,
              latest_action,
              missing_mark_count,
              negative_position_count,
              multiIf(
                negative_position_count > 0, 'needs_chain_balance',
                missing_mark_count > 0, 'missing_mark_price',
                'estimate'
              ) as data_quality,
              as_of as updated_at
            from
            (
              select
                user_address,
                sum(trade_count) as trade_count,
                sum(buy_count) as buy_count,
                sum(sell_count) as sell_count,
                sum(traded_size) as traded_size,
                sum(traded_notional) as traded_notional,
                sum(buy_notional) as buy_notional,
                sum(sell_notional) as sell_notional,
                sum(token_trade_count_24h) as trade_count_24h,
                sum(token_buy_notional_24h) as buy_notional_24h,
                sum(token_sell_notional_24h) as sell_notional_24h,
                sum(token_traded_notional_24h) as traded_notional_24h,
                sum(token_buy_notional_7d) as buy_notional_7d,
                sum(token_sell_notional_7d) as sell_notional_7d,
                sum(token_traded_notional_7d) as traded_notional_7d,
                uniqExact(event_id) as event_count,
                uniqExact(market_id) as market_count,
                uniqExactIf(event_id, token_trade_count_24h > 0) as event_count_24h,
                uniqExactIf(market_id, token_trade_count_24h > 0) as market_count_24h,
                count() as token_count,
                countIf(position_size_now > 0.000001) as open_position_count,
                sum(token_open_position_value_now) as open_position_value_now,
                sum(token_open_position_value_24h_ago) as open_position_value_24h_ago,
                sum(token_open_position_value_7d_ago) as open_position_value_7d_ago,
                sum(net_cashflow_now + token_open_position_value_now) as equity_now,
                sum(net_cashflow_24h_ago + token_open_position_value_24h_ago) as equity_24h_ago,
                sum(net_cashflow_7d_ago + token_open_position_value_7d_ago) as equity_7d_ago,
                countIf(token_pnl_now > 0.000001) as profitable_token_count,
                countIf(token_pnl_now < -0.000001) as losing_token_count,
                countIf(token_trade_count_24h > 0 and token_pnl_now > 0.000001)
                  as profitable_token_count_24h,
                countIf(token_trade_count_24h > 0 and token_pnl_now < -0.000001)
                  as losing_token_count_24h,
                countIf(token_trade_count_7d > 0 and token_pnl_now > 0.000001)
                  as profitable_token_count_7d,
                countIf(token_trade_count_7d > 0 and token_pnl_now < -0.000001)
                  as losing_token_count_7d,
                min(first_trade_at) as first_trade_at,
                max(last_trade_at) as last_trade_at,
                argMax(token_latest_action, token_last_trade_at) as latest_action,
                sum(missing_mark) as missing_mark_count,
                sum(negative_position) as negative_position_count
              from
              (
                select
                  positions.event_id as event_id,
                  positions.market_id as market_id,
                  positions.user_address as user_address,
                  positions.trade_count as trade_count,
                  positions.buy_count as buy_count,
                  positions.sell_count as sell_count,
                  positions.traded_size as traded_size,
                  positions.traded_notional as traded_notional,
                  positions.buy_notional as buy_notional,
                  positions.sell_notional as sell_notional,
                  positions.trade_count_24h as token_trade_count_24h,
                  positions.buy_notional_24h as token_buy_notional_24h,
                  positions.sell_notional_24h as token_sell_notional_24h,
                  positions.traded_notional_24h as token_traded_notional_24h,
                  positions.trade_count_7d as token_trade_count_7d,
                  positions.buy_notional_7d as token_buy_notional_7d,
                  positions.sell_notional_7d as token_sell_notional_7d,
                  positions.traded_notional_7d as token_traded_notional_7d,
                  positions.position_size_now - ifNull(chain_cashflows.redeemed_value_now, 0.0)
                    as position_size_now,
                  positions.position_size_24h_ago
                    - ifNull(chain_cashflows.redeemed_value_24h_ago, 0.0)
                    as position_size_24h_ago,
                  positions.position_size_7d_ago
                    - ifNull(chain_cashflows.redeemed_value_7d_ago, 0.0)
                    as position_size_7d_ago,
                  positions.net_cashflow_now
                    + ifNull(chain_cashflows.redeemed_value_now, 0.0)
                    - ifNull(chain_cashflows.fee_now, 0.0)
                    as net_cashflow_now,
                  positions.net_cashflow_24h_ago
                    + ifNull(chain_cashflows.redeemed_value_24h_ago, 0.0)
                    - ifNull(chain_cashflows.fee_24h_ago, 0.0)
                    as net_cashflow_24h_ago,
                  positions.net_cashflow_7d_ago
                    + ifNull(chain_cashflows.redeemed_value_7d_ago, 0.0)
                    - ifNull(chain_cashflows.fee_7d_ago, 0.0)
                    as net_cashflow_7d_ago,
                  (
                    positions.net_cashflow_now
                    + ifNull(chain_cashflows.redeemed_value_now, 0.0)
                    - ifNull(chain_cashflows.fee_now, 0.0)
                  )
                    + if(
                      positions.position_size_now
                        - ifNull(chain_cashflows.redeemed_value_now, 0.0) > 0.000001,
                      (
                        positions.position_size_now
                        - ifNull(chain_cashflows.redeemed_value_now, 0.0)
                      ) * ifNull(marks.mark_price_now, 0.0),
                      0.0
                    ) as token_pnl_now,
                  if(
                    positions.position_size_now
                      - ifNull(chain_cashflows.redeemed_value_now, 0.0) > 0.000001,
                    (
                      positions.position_size_now
                      - ifNull(chain_cashflows.redeemed_value_now, 0.0)
                    ) * ifNull(marks.mark_price_now, 0.0),
                    0.0
                  ) as token_open_position_value_now,
                  if(
                    positions.position_size_24h_ago
                      - ifNull(chain_cashflows.redeemed_value_24h_ago, 0.0) > 0.000001,
                    (
                      positions.position_size_24h_ago
                      - ifNull(chain_cashflows.redeemed_value_24h_ago, 0.0)
                    ) * ifNull(marks.mark_price_24h_ago, 0.0),
                    0.0
                  ) as token_open_position_value_24h_ago,
                  if(
                    positions.position_size_7d_ago
                      - ifNull(chain_cashflows.redeemed_value_7d_ago, 0.0) > 0.000001,
                    (
                      positions.position_size_7d_ago
                      - ifNull(chain_cashflows.redeemed_value_7d_ago, 0.0)
                    ) * ifNull(marks.mark_price_7d_ago, 0.0),
                    0.0
                  ) as token_open_position_value_7d_ago,
                  positions.first_trade_at as first_trade_at,
                  positions.last_trade_at as last_trade_at,
                  positions.last_trade_at as token_last_trade_at,
                  positions.latest_action as token_latest_action,
                  if(
                    (
                      positions.position_size_now
                        - ifNull(chain_cashflows.redeemed_value_now, 0.0) > 0.000001
                      and marks.mark_price_now is null
                    )
                    or (
                      positions.position_size_24h_ago
                        - ifNull(chain_cashflows.redeemed_value_24h_ago, 0.0) > 0.000001
                      and marks.mark_price_24h_ago is null
                    )
                    or (
                      positions.position_size_7d_ago
                        - ifNull(chain_cashflows.redeemed_value_7d_ago, 0.0) > 0.000001
                      and marks.mark_price_7d_ago is null
                    ),
                    1,
                    0
                  ) as missing_mark,
                  if(
                    positions.position_size_now
                      - ifNull(chain_cashflows.redeemed_value_now, 0.0) < -0.000001
                    or positions.position_size_24h_ago
                      - ifNull(chain_cashflows.redeemed_value_24h_ago, 0.0) < -0.000001
                    or positions.position_size_7d_ago
                      - ifNull(chain_cashflows.redeemed_value_7d_ago, 0.0) < -0.000001,
                    1,
                    0
                  ) as negative_position
                from
                (
                  select
                    trades.event_id as event_id,
                    trades.market_id as market_id,
                    trades.condition_id as condition_id,
                    trades.token_id as token_id,
                    trades.user_address as user_address,
                    count() as trade_count,
                    countIf(trades.side = 'BUY') as buy_count,
                    countIf(trades.side = 'SELL') as sell_count,
                    sum(trades.size) as traded_size,
                    sum(trades.notional) as traded_notional,
                    sumIf(trades.notional, trades.side = 'BUY') as buy_notional,
                    sumIf(trades.notional, trades.side = 'SELL') as sell_notional,
                    countIf(trades.timestamp >= since) as trade_count_24h,
                    sumIf(
                      trades.notional,
                      trades.side = 'BUY' and trades.timestamp >= since
                    ) as buy_notional_24h,
                    sumIf(
                      trades.notional,
                      trades.side = 'SELL' and trades.timestamp >= since
                    ) as sell_notional_24h,
                    sumIf(trades.notional, trades.timestamp >= since) as traded_notional_24h,
                    countIf(trades.timestamp >= since_7d) as trade_count_7d,
                    sumIf(
                      trades.notional,
                      trades.side = 'BUY' and trades.timestamp >= since_7d
                    ) as buy_notional_7d,
                    sumIf(
                      trades.notional,
                      trades.side = 'SELL' and trades.timestamp >= since_7d
                    ) as sell_notional_7d,
                    sumIf(trades.notional, trades.timestamp >= since_7d) as traded_notional_7d,
                    sum(if(trades.side = 'BUY', trades.size, -trades.size))
                      as position_size_now,
                    sumIf(
                      if(trades.side = 'BUY', trades.size, -trades.size),
                      trades.timestamp < since
                    ) as position_size_24h_ago,
                    sumIf(
                      if(trades.side = 'BUY', trades.size, -trades.size),
                      trades.timestamp < since_7d
                    ) as position_size_7d_ago,
                    sum(if(trades.side = 'SELL', trades.notional, -trades.notional))
                      as net_cashflow_now,
                    sumIf(
                      if(trades.side = 'SELL', trades.notional, -trades.notional),
                      trades.timestamp < since
                    ) as net_cashflow_24h_ago,
                    sumIf(
                      if(trades.side = 'SELL', trades.notional, -trades.notional),
                      trades.timestamp < since_7d
                    ) as net_cashflow_7d_ago,
                    min(trades.timestamp) as first_trade_at,
                    max(trades.timestamp) as last_trade_at,
                    argMax(trades.side, trades.timestamp) as latest_action
                  from
                  mart_fifa_trade as trades final
                  where trades.user_address != ''
                    and trades.timestamp <= as_of + toIntervalMinute(10)
                  group by
                    trades.event_id,
                    trades.market_id,
                    trades.condition_id,
                    trades.token_id,
                    trades.user_address
                ) as positions
                left join marks on positions.token_id = marks.token_id
                left join chain_cashflows
                  on positions.token_id = chain_cashflows.token_id
                 and positions.user_address = chain_cashflows.user_address
              )
              group by user_address
            )
            where user_address != ''
            settings join_use_nulls = 1, max_threads = 4
            """
        )
        self.clickhouse.execute(
            "exchange tables mart_wallet_fifa_24h_pnl and mart_wallet_fifa_24h_pnl_next"
        )
        rows = int(
            self.clickhouse.query_text(
                "select count() from mart_wallet_fifa_24h_pnl final"
            ).strip()
            or "0"
        )
        return MartBuildResult(mart="wallet_fifa_24h_pnl", rows=rows)

    def build_wallet_fifa_chain_cashflow(self, *, window_days: int = 7) -> MartBuildResult:
        if window_days <= 0:
            raise ValueError("window_days must be positive")
        block_window = window_days * 24 * 1800
        day_block_window = 24 * 1800
        self.clickhouse.execute("truncate table mart_wallet_fifa_chain_cashflow")
        self.clickhouse.execute(
            f"""
            insert into mart_wallet_fifa_chain_cashflow
            with
              now64(3) as as_of,
              chain_bounds as
              (
                select
                  max_block,
                  toUInt64(greatest(toInt64(max_block) - {day_block_window}, 0))
                    as since_block,
                  toUInt64(greatest(toInt64(max_block) - {block_window}, 0))
                    as since_window_block
                from
                (
                  select toUInt64(ifNull(max(block_number), 0)) as max_block
                  from fact_ctf_balance_movement
                )
              ),
              recent_fifa_tokens as
              (
                select token_id
                from mart_fifa_trade final
                where token_id != ''
                  and timestamp >= as_of - toIntervalDay({window_days})
                group by token_id
              ),
              redeemed_positions as
              (
                select
                  lower(from_address) as user_address,
                  token_id,
                  sum(amount) as redeemed_value_now,
                  sumIf(amount, block_number < (select since_block from chain_bounds))
                    as redeemed_value_24h_ago,
                  0.0 as redeemed_value_7d_ago
                from fact_ctf_balance_movement final
                prewhere block_number >= (select since_window_block from chain_bounds)
                where token_id in (select token_id from recent_fifa_tokens)
                  and to_address = '0xada100db00ca00073811820692005400218fce1f'
                  and from_address not in ('', '0x0000000000000000000000000000000000000000')
                  and amount > 0.000001
                group by user_address, token_id
              ),
              chain_fees as
              (
                select
                  lower(maker) as user_address,
                  token_id,
                  sum(decoded_fee) as fee_now,
                  sumIf(decoded_fee, block_number < (select since_block from chain_bounds))
                    as fee_24h_ago,
                  0.0 as fee_7d_ago
                from
                (
                  select
                    block_number,
                    token_id,
                    maker,
                    JSONExtractFloat(JSONExtractRaw(raw_json, 'decoded'), 'fee')
                      / 1000000.0 as decoded_fee
                  from fact_exchange_fill final
                  prewhere block_number >= (select since_window_block from chain_bounds)
                  where token_id in (select token_id from recent_fifa_tokens)
                    and maker not in ('', '0x0000000000000000000000000000000000000000')
                )
                where decoded_fee > 0.0
                group by user_address, token_id
              )
            select
              user_address,
              token_id,
              sum(redeemed_value_now) as redeemed_value_now,
              sum(redeemed_value_24h_ago) as redeemed_value_24h_ago,
              sum(redeemed_value_7d_ago) as redeemed_value_7d_ago,
              sum(fee_now) as fee_now,
              sum(fee_24h_ago) as fee_24h_ago,
              sum(fee_7d_ago) as fee_7d_ago,
              as_of as updated_at
            from
            (
              select
                user_address,
                token_id,
                redeemed_value_now,
                redeemed_value_24h_ago,
                redeemed_value_7d_ago,
                0.0 as fee_now,
                0.0 as fee_24h_ago,
                0.0 as fee_7d_ago
              from redeemed_positions
              union all
              select
                user_address,
                token_id,
                0.0 as redeemed_value_now,
                0.0 as redeemed_value_24h_ago,
                0.0 as redeemed_value_7d_ago,
                fee_now,
                fee_24h_ago,
                fee_7d_ago
              from chain_fees
            )
            group by user_address, token_id
            """
        )
        rows = int(
            self.clickhouse.query_text(
                "select count() from mart_wallet_fifa_chain_cashflow final"
            ).strip()
            or "0"
        )
        return MartBuildResult(mart="wallet_fifa_chain_cashflow", rows=rows)

    def build_fifa_trades(self, *, window_hours: int = 72) -> MartBuildResult:
        if window_hours <= 0:
            raise ValueError("window_hours must be positive")
        self.clickhouse.execute(
            f"""
            alter table mart_fifa_trade
            delete where timestamp >= now64(3) - toIntervalHour({window_hours})
            """
        )
        self.clickhouse.execute(
            f"""
            insert into mart_fifa_trade
            with
              now64(3) as as_of,
              as_of - toIntervalHour({window_hours}) as refresh_since,
              existing_max as
              (
                select ifNull(max(timestamp), toDateTime64(0, 3, 'UTC')) as max_timestamp
                from mart_fifa_trade final
              ),
              fifa_bounds as
              (
                select
                  greatest(
                    toDateTime64('2026-01-01 00:00:00', 3, 'UTC'),
                    ifNull(min(markets.created_at), ifNull(min(markets.start_time), refresh_since))
                      - toIntervalDay(1)
                  ) as initial_min_trade_at,
                  least(
                    refresh_since,
                    if(
                      (select max_timestamp from existing_max) = toDateTime64(0, 3, 'UTC'),
                      refresh_since,
                      (select max_timestamp from existing_max) - toIntervalHour({window_hours})
                    )
                  ) as incremental_min_trade_at
                from dim_market as markets final
                left join dim_event as events final on markets.event_id = events.event_id
                where markets.condition_id != ''
                  and markets.market_id != ''
                  and (
                    startsWith(markets.slug, 'fifwc-')
                    or startsWith(ifNull(events.slug, ''), 'fifwc-')
                  )
              ),
              fifa_markets as
              (
                select
                  raw_condition_id as condition_id,
                  argMax(raw_market_id, raw_priority) as market_id,
                  argMax(raw_event_id, raw_priority) as event_id
                from
                (
                  select
                    markets.condition_id as raw_condition_id,
                    markets.market_id as raw_market_id,
                    markets.event_id as raw_event_id,
                    2 as raw_priority
                  from dim_market as markets final
                  left join dim_event as events final on markets.event_id = events.event_id
                  where markets.condition_id != ''
                    and markets.market_id != ''
                    and (
                      startsWith(markets.slug, 'fifwc-')
                      or startsWith(ifNull(events.slug, ''), 'fifwc-')
                    )
                  union all
                  select
                    condition_id as raw_condition_id,
                    if(
                      raw_market_slug != '',
                      raw_market_slug,
                      condition_id
                    ) as raw_market_id,
                    raw_event_slug as raw_event_id,
                    1 as raw_priority
                  from
                  (
                    select
                      condition_id,
                      argMax(raw_market_slug, raw_ingested_at) as raw_market_slug,
                      argMax(raw_event_slug, raw_ingested_at) as raw_event_slug
                    from
                    (
                      select
                        condition_id,
                        JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'market_slug')
                          as raw_market_slug,
                        JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'event_slug')
                          as raw_event_slug,
                        ingested_at as raw_ingested_at
                      from fact_trade_by_time
                      where condition_id != ''
                        and timestamp >= refresh_since
                        and (
                          startsWith(
                            JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'market_slug'),
                            'fifwc-'
                          )
                          or startsWith(
                            JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'event_slug'),
                            'fifwc-'
                          )
                        )
                    )
                    group by condition_id
                  )
                )
                group by raw_condition_id
              ),
              fifa_tokens as
              (
                select
                  raw_token_id as token_id,
                  anyLast(raw_market_id) as market_id
                from
                (
                  select
                    tokens.token_id as raw_token_id,
                    tokens.market_id as raw_market_id
                  from dim_outcome_token as tokens final
                  inner join
                  (
                    select market_id
                    from fifa_markets
                    group by market_id
                  ) as markets on tokens.market_id = markets.market_id
                  where tokens.token_id != ''
                  union all
                  select
                    token_id as raw_token_id,
                    anyLast(
                      if(
                        raw_market_slug != '',
                        raw_market_slug,
                        condition_id
                      )
                    ) as raw_market_id
                  from
                  (
                    select
                      token_id,
                      condition_id,
                      JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'market_slug')
                        as raw_market_slug
                    from fact_trade_by_time
                    where token_id != ''
                      and condition_id != ''
                      and timestamp >= refresh_since
                      and (
                        startsWith(
                          JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'market_slug'),
                          'fifwc-'
                        )
                        or startsWith(
                          JSONExtractString(JSONExtractRaw(raw_json, 'trade'), 'event_slug'),
                          'fifwc-'
                        )
                      )
                  )
                  group by token_id
                )
                group by raw_token_id
              )
            select
              dedup.trade_key as trade_key,
              dedup.timestamp as timestamp,
              markets.event_id as event_id,
              markets.market_id as market_id,
              dedup.condition_id as condition_id,
              dedup.token_id as token_id,
              dedup.user_address as user_address,
              dedup.side as side,
              dedup.price as price,
              dedup.size as size,
              dedup.notional as notional,
              dedup.ingested_at as ingested_at,
              as_of as updated_at
            from
            (
              select
                trade_key,
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_condition_id, raw_ingested_at) as condition_id,
                argMax(raw_token_id, raw_ingested_at) as token_id,
                argMax(raw_user_address, raw_ingested_at) as user_address,
                argMax(raw_side, raw_ingested_at) as side,
                argMax(raw_price, raw_ingested_at) as price,
                argMax(raw_size, raw_ingested_at) as size,
                argMax(raw_notional, raw_ingested_at) as notional,
                max(raw_ingested_at) as ingested_at
              from
              (
                select
                  concat(
                    toString(timestamp), ':', condition_id, ':', token_id, ':',
                    lower(user_address), ':', side, ':',
                    toString(round(price, 12)), ':',
                    toString(round(size, 6))
                  ) as trade_key,
                  timestamp as raw_timestamp,
                  condition_id as raw_condition_id,
                  token_id as raw_token_id,
                  lower(user_address) as raw_user_address,
                  side as raw_side,
                  price as raw_price,
                  size as raw_size,
                  notional as raw_notional,
                  ingested_at as raw_ingested_at
                from fact_trade_by_time
                where user_address != ''
                  and condition_id in (select condition_id from fifa_markets)
                  and token_id in (select token_id from fifa_tokens)
                  and timestamp >= if(
                    (select max_timestamp from existing_max) = toDateTime64(0, 3, 'UTC'),
                    (select initial_min_trade_at from fifa_bounds),
                    (select incremental_min_trade_at from fifa_bounds)
                  )
                  and timestamp <= as_of + toIntervalMinute(10)
              )
              group by trade_key
            ) as dedup
            inner join fifa_markets as markets on dedup.condition_id = markets.condition_id
            settings max_threads = 4
            """
        )
        rows = int(
            self.clickhouse.query_text("select count() from mart_fifa_trade final").strip()
            or "0"
        )
        return MartBuildResult(mart="fifa_trade", rows=rows)

    def build_trader_chain_pnl(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_trader_chain_pnl
            select
              user_address,
              chain_fill_count,
              chain_traded_size,
              chain_traded_notional,
              chain_position_size,
              chain_current_value,
              chain_net_cashflow,
              chain_net_cashflow + chain_current_value as chain_mark_to_market_pnl,
              last_chain_fill_block,
              now64(3) as updated_at
            from
            (
              select
                user_address,
                sum(chain_fill_count) as chain_fill_count,
                sum(chain_traded_size) as chain_traded_size,
                sum(chain_traded_notional) as chain_traded_notional,
                sum(chain_position_size) as chain_position_size,
                sum(chain_current_value) as chain_current_value,
                sum(chain_net_cashflow) as chain_net_cashflow,
                max(last_chain_fill_block) as last_chain_fill_block
              from
              (
              select
                user_address,
                count() as chain_fill_count,
                sum(size) as chain_traded_size,
                sum(notional) as chain_traded_notional,
                0.0 as chain_position_size,
                0.0 as chain_current_value,
                sum(if(side = 'SELL', notional, -notional)) as chain_net_cashflow,
                max(block_number) as last_chain_fill_block
              from
              (
                select
                  maker as user_address,
                  side,
                  size,
                  notional,
                  block_number
                from fact_exchange_fill final
                where maker != ''
                union all
                select
                  taker as user_address,
                  if(side = 'BUY', 'SELL', 'BUY') as side,
                  size,
                  notional,
                  block_number
                from fact_exchange_fill final
                where taker != ''
              )
              group by user_address
              union all
              select
                balances.user_address,
                0 as chain_fill_count,
                0.0 as chain_traded_size,
                0.0 as chain_traded_notional,
                sum(balances.balance) as chain_position_size,
                sum(balances.balance * ifNull(prices.price, 0.0)) as chain_current_value,
                0.0 as chain_net_cashflow,
                0 as last_chain_fill_block
              from
              (
                select
                  user_address,
                  token_id,
                  sum(delta) as balance
                from
                (
                  select
                    to_address as user_address,
                    token_id,
                    amount as delta
                  from fact_ctf_balance_movement final
                  where to_address != '' and to_address != '0x0000000000000000000000000000000000000000'
                  union all
                  select
                    from_address as user_address,
                    token_id,
                    -amount as delta
                  from fact_ctf_balance_movement final
                  where from_address != '' and from_address != '0x0000000000000000000000000000000000000000'
                )
                group by user_address, token_id
                having balance != 0
              ) as balances
              left join
              (
                select
                  token_id,
                  argMax(price, timestamp) as price
                from fact_price_history
                group by token_id
              ) as prices on balances.token_id = prices.token_id
              group by balances.user_address
              )
              where user_address != ''
              group by user_address
            )
            """
        )
        rows = int(
            self.clickhouse.query_text("select count() from mart_trader_chain_pnl").strip() or "0"
        )
        return MartBuildResult(mart="trader_chain_pnl", rows=rows)

    def build_event_wallet_pnl(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_event_wallet_pnl
            select
              event_id,
              user_address,
              event_title,
              category,
              market_count,
              token_count,
              trade_count,
              buy_count,
              sell_count,
              buy_size,
              sell_size,
              traded_size,
              buy_notional,
              sell_notional,
              traded_notional,
              net_cashflow,
              final_position_value,
              net_cashflow + final_position_value as realized_pnl,
              if(buy_notional = 0, 0.0, (net_cashflow + final_position_value) / buy_notional)
                as roi,
              first_trade_at,
              last_trade_at,
              closed_market_count,
              resolved_market_count,
              multiIf(
                resolved_market_count = 0, 'unresolved',
                resolved_market_count < closed_market_count, 'partial_resolution',
                'resolved'
              ) as settlement_status,
              multiIf(
                min_token_position < -0.000001, 'needs_chain_balance',
                resolved_market_count < closed_market_count, 'partial_resolution',
                'data_api_estimate'
              ) as data_quality,
              now64(3) as updated_at
            from
            (
              select
                wallet.event_id,
                wallet.user_address,
                anyLast(wallet.event_title) as event_title,
                anyLast(wallet.category) as category,
                uniqExact(wallet.market_id) as market_count,
                count() as token_count,
                sum(wallet.trade_count) as trade_count,
                sum(wallet.buy_count) as buy_count,
                sum(wallet.sell_count) as sell_count,
                sum(wallet.buy_size) as buy_size,
                sum(wallet.sell_size) as sell_size,
                sum(wallet.traded_size) as traded_size,
                sum(wallet.buy_notional) as buy_notional,
                sum(wallet.sell_notional) as sell_notional,
                sum(wallet.traded_notional) as traded_notional,
                sum(wallet.net_cashflow) as net_cashflow,
                sum(wallet.final_position_value) as final_position_value,
                min(wallet.first_trade_at) as first_trade_at,
                max(wallet.last_trade_at) as last_trade_at,
                anyIf(stats.closed_market_count, stats.event_id != '') as closed_market_count,
                anyIf(stats.resolved_market_count, stats.event_id != '') as resolved_market_count,
                min(wallet.position_size) as min_token_position
              from
              (
                select
                  event_id,
                  user_address,
                  anyLast(event_title) as event_title,
                  anyLast(category) as category,
                  anyLast(market_id) as market_id,
                  token_id,
                  count() as trade_count,
                  countIf(side = 'BUY') as buy_count,
                  countIf(side = 'SELL') as sell_count,
                  sumIf(size, side = 'BUY') as buy_size,
                  sumIf(size, side = 'SELL') as sell_size,
                  sum(size) as traded_size,
                  sumIf(notional, side = 'BUY') as buy_notional,
                  sumIf(notional, side = 'SELL') as sell_notional,
                  sum(notional) as traded_notional,
                  sum(if(side = 'SELL', notional, -notional)) as net_cashflow,
                  sum(if(side = 'BUY', size, -size)) as position_size,
                  sum(if(side = 'BUY', size, -size)) * anyLast(final_price)
                    as final_position_value,
                  min(timestamp) as first_trade_at,
                  max(timestamp) as last_trade_at
                  from
                  (
                    select
                      trades.trade_id as trade_id,
                      trades.timestamp as timestamp,
                      trades.token_id as token_id,
                      trades.user_address as user_address,
                      trades.side as side,
                      trades.price as price,
                      trades.size as size,
                      trades.notional as notional,
                      markets.market_id as market_id,
                      markets.event_id as event_id,
                      if(events.title = '', markets.question, events.title) as event_title,
                      events.category as category,
                      resolved.final_price as final_price
                  from
                  (
                    select
                      trade_id,
                      anyLast(raw_timestamp) as timestamp,
                      anyLast(raw_condition_id) as condition_id,
                      anyLast(raw_token_id) as token_id,
                      anyLast(raw_user_address) as user_address,
                      anyLast(raw_side) as side,
                      anyLast(raw_price) as price,
                      anyLast(raw_size) as size,
                      anyLast(raw_notional) as notional
                    from
                    (
                      select
                        trade_id,
                        timestamp as raw_timestamp,
                        condition_id as raw_condition_id,
                        token_id as raw_token_id,
                        user_address as raw_user_address,
                        side as raw_side,
                        price as raw_price,
                        size as raw_size,
                        notional as raw_notional
                      from fact_trade
                      where user_address != '' and condition_id != '' and token_id != ''
                    )
                    group by trade_id
                  ) as trades
                  inner join
                  (
                    select
                      raw_condition_id as condition_id,
                      anyLast(raw_market_id) as market_id,
                      anyLast(raw_event_id) as event_id,
                      anyLast(raw_question) as question
                    from
                    (
                      select
                        condition_id as raw_condition_id,
                        market_id as raw_market_id,
                        event_id as raw_event_id,
                        question as raw_question
                      from dim_market as m final
                      where condition_id != '' and event_id != '' and closed = true
                    )
                    group by condition_id
                  ) as markets on trades.condition_id = markets.condition_id
                  inner join
                  (
                    select
                      resolved_prices.market_id as market_id,
                      resolved_prices.condition_id as condition_id,
                      tokens.token_id as token_id,
                      resolved_prices.final_price as final_price
                    from
                    (
                      select
                        market_id,
                        condition_id,
                        price_index - 1 as outcome_index,
                        toFloat64OrZero(JSONExtractString(price_raw)) as final_price
                      from
                      (
                        select
                          market_id,
                          condition_id,
                          JSONExtractString(raw_json, 'outcomePrices') as prices
                        from dim_market as m final
                        where closed = true and prices != ''
                      )
                      array join
                        JSONExtractArrayRaw(prices) as price_raw,
                        arrayEnumerate(JSONExtractArrayRaw(prices)) as price_index
                    ) as resolved_prices
                    inner join dim_outcome_token as tokens final
                      on tokens.market_id = resolved_prices.market_id
                     and tokens.outcome_index = resolved_prices.outcome_index
                  ) as resolved
                    on resolved.market_id = markets.market_id
                   and resolved.condition_id = trades.condition_id
                   and resolved.token_id = trades.token_id
                  left join dim_event as events final on markets.event_id = events.event_id
                )
                group by event_id, user_address, token_id
              ) as wallet
              left join
              (
                select
                  event_id,
                  count() as closed_market_count,
                  countIf(winner_count = 1) as resolved_market_count
                from
                (
                  select
                    event_id,
                    market_id,
                    arrayCount(
                      price -> toFloat64OrZero(JSONExtractString(price)) >= 0.999,
                      JSONExtractArrayRaw(JSONExtractString(raw_json, 'outcomePrices'))
                    ) as winner_count
                  from dim_market as m final
                  where event_id != '' and closed = true
                )
                group by event_id
              ) as stats on wallet.event_id = stats.event_id
              group by wallet.event_id, wallet.user_address
            )
            where event_id != '' and user_address != ''
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_event_wallet_pnl").strip() or "0")
        return MartBuildResult(mart="event_wallet_pnl", rows=rows)

    def build_live_wallet_positions(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_live_wallet_position
            select
              positions.event_id,
              positions.market_id,
              positions.condition_id,
              positions.token_id,
              positions.outcome,
              positions.user_address,
              positions.trade_count,
              positions.buy_count,
              positions.sell_count,
              positions.buy_size,
              positions.sell_size,
              positions.position_size,
              positions.buy_notional,
              positions.sell_notional,
              positions.traded_notional,
              positions.net_cashflow,
              if(positions.buy_size = 0, cast(null, 'Nullable(Float64)'), positions.buy_notional / positions.buy_size)
                as avg_entry_price,
              marks.mark_price,
              marks.mark_price_source,
              marks.mark_price_at,
              positions.position_size * ifNull(marks.mark_price, 0.0) as current_value,
              positions.net_cashflow + positions.position_size * ifNull(marks.mark_price, 0.0)
                as unrealized_pnl_estimate,
              positions.first_trade_at,
              positions.last_trade_at,
              positions.net_size_24h,
              positions.net_notional_24h,
              positions.latest_action,
              positions.net_size_24h > 0 and positions.position_size > 0 as is_accumulating,
              multiIf(
                marks.mark_price_source = 'missing', 'missing_mark_price',
                positions.position_size < -0.000001, 'needs_chain_balance',
                'estimate'
              ) as data_quality,
              now64(3) as updated_at
            from
            (
              select
                markets.event_id as event_id,
                markets.market_id as market_id,
                trades.condition_id as condition_id,
                trades.token_id as token_id,
                anyLast(tokens.outcome) as outcome,
                trades.user_address as user_address,
                count() as trade_count,
                countIf(trades.side = 'BUY') as buy_count,
                countIf(trades.side = 'SELL') as sell_count,
                sumIf(trades.size, trades.side = 'BUY') as buy_size,
                sumIf(trades.size, trades.side = 'SELL') as sell_size,
                sum(if(trades.side = 'BUY', trades.size, -trades.size)) as position_size,
                sumIf(trades.notional, trades.side = 'BUY') as buy_notional,
                sumIf(trades.notional, trades.side = 'SELL') as sell_notional,
                sum(trades.notional) as traded_notional,
                sum(if(trades.side = 'SELL', trades.notional, -trades.notional)) as net_cashflow,
                min(trades.timestamp) as first_trade_at,
                max(trades.timestamp) as last_trade_at,
                sumIf(
                  if(trades.side = 'BUY', trades.size, -trades.size),
                  trades.timestamp >= now64(3) - interval 24 hour
                ) as net_size_24h,
                sumIf(
                  if(trades.side = 'BUY', trades.notional, -trades.notional),
                  trades.timestamp >= now64(3) - interval 24 hour
                ) as net_notional_24h,
                argMax(trades.side, trades.timestamp) as latest_action
              from
              (
                select
                  trade_id,
                  anyLast(raw_timestamp) as timestamp,
                  anyLast(raw_condition_id) as condition_id,
                  anyLast(raw_token_id) as token_id,
                  anyLast(raw_user_address) as user_address,
                  anyLast(raw_side) as side,
                  anyLast(raw_size) as size,
                  anyLast(raw_notional) as notional
                from
                (
                  select
                    trade_id,
                    timestamp as raw_timestamp,
                    condition_id as raw_condition_id,
                    token_id as raw_token_id,
                    user_address as raw_user_address,
                    side as raw_side,
                    size as raw_size,
                    notional as raw_notional
                  from fact_trade
                  where user_address != '' and condition_id != '' and token_id != ''
                )
                group by trade_id
              ) as trades
              inner join
              (
                select
                  raw_condition_id as condition_id,
                  anyLast(raw_market_id) as market_id,
                  anyLast(raw_event_id) as event_id
                from
                (
                  select
                    condition_id as raw_condition_id,
                    market_id as raw_market_id,
                    event_id as raw_event_id
                  from dim_market as m final
                  where condition_id != ''
                    and event_id != ''
                    and active = true
                    and closed = false
                    and archived = false
                )
                group by condition_id
              ) as markets on trades.condition_id = markets.condition_id
              left join dim_outcome_token as tokens final
                on tokens.market_id = markets.market_id
               and tokens.token_id = trades.token_id
              group by
                markets.event_id,
                markets.market_id,
                trades.condition_id,
                trades.token_id,
                trades.user_address
              having abs(position_size) > 0.000001
            ) as positions
            left join
            (
              select
                token_ids.token_id as token_id,
                multiIf(
                  latest_book.best_bid is not null and latest_book.best_ask is not null,
                    cast((latest_book.best_bid + latest_book.best_ask) / 2, 'Nullable(Float64)'),
                  latest_price.mark_at > toDateTime64(0, 3, 'UTC'),
                    cast(latest_price.price, 'Nullable(Float64)'),
                  cast(null, 'Nullable(Float64)')
                ) as mark_price,
                multiIf(
                  latest_book.best_bid is not null and latest_book.best_ask is not null,
                    'orderbook_mid',
                  latest_price.mark_at > toDateTime64(0, 3, 'UTC'),
                    'price_history',
                  'missing'
                ) as mark_price_source,
                multiIf(
                  latest_book.best_bid is not null and latest_book.best_ask is not null,
                    cast(latest_book.mark_at, 'Nullable(DateTime64(3, \\'UTC\\'))'),
                  latest_price.mark_at > toDateTime64(0, 3, 'UTC'),
                    cast(latest_price.mark_at, 'Nullable(DateTime64(3, \\'UTC\\'))'),
                  cast(null, 'Nullable(DateTime64(3, \\'UTC\\'))')
                ) as mark_price_at
              from
              (
                select token_id
                from dim_outcome_token as t final
                where token_id != ''
                group by token_id
              ) as token_ids
              left join
              (
                select
                  token_id,
                  argMax(price, ts) as price,
                  max(ts) as mark_at
                from
                (
                  select token_id, timestamp as ts, price
                  from fact_price_history final
                )
                group by token_id
              ) as latest_price on token_ids.token_id = latest_price.token_id
              left join
              (
                select
                  token_id,
                  argMax(best_bid, captured_at) as best_bid,
                  argMax(best_ask, captured_at) as best_ask,
                  max(captured_at) as mark_at
                from fact_orderbook_snapshot
                group by token_id
              ) as latest_book on token_ids.token_id = latest_book.token_id
            ) as marks on positions.token_id = marks.token_id
            """
        )
        rows = int(
            self.clickhouse.query_text("select count() from mart_live_wallet_position").strip()
            or "0"
        )
        return MartBuildResult(mart="live_wallet_position", rows=rows)

    def build_wallet_reputation(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_wallet_reputation
            select
              wallets.user_address,
              ifNull(pnl.completed_event_count, 0) as completed_event_count,
              ifNull(pnl.profitable_event_count, 0) as profitable_event_count,
              ifNull(pnl.losing_event_count, 0) as losing_event_count,
              if(completed_event_count = 0, 0.0, profitable_event_count / completed_event_count)
                as win_rate,
              ifNull(pnl.realized_pnl, 0.0) as realized_pnl,
              ifNull(pnl.positive_pnl, 0.0) as positive_pnl,
              ifNull(pnl.negative_pnl, 0.0) as negative_pnl,
              ifNull(pnl.buy_notional, 0.0) as buy_notional,
              ifNull(pnl.sell_notional, 0.0) as sell_notional,
              ifNull(pnl.traded_notional, 0.0) as traded_notional,
              ifNull(pnl.trade_count, 0) as trade_count,
              ifNull(pnl.avg_event_roi, 0.0) as avg_event_roi,
              ifNull(pnl.best_event_pnl, 0.0) as best_event_pnl,
              ifNull(pnl.worst_event_pnl, 0.0) as worst_event_pnl,
              ifNull(live.active_position_count, 0) as active_position_count,
              ifNull(live.active_event_count, 0) as active_event_count,
              ifNull(live.active_unrealized_pnl_estimate, 0.0)
                as active_unrealized_pnl_estimate,
              ifNull(category.favorite_category, '') as favorite_category,
              ifNull(category.favorite_category_notional, 0.0) as favorite_category_notional,
              bounds.first_trade_at,
              bounds.last_trade_at,
              now64(3) as updated_at
            from
            (
              select user_address
              from
              (
                select user_address from mart_event_wallet_pnl as p final
                union all
                select user_address from mart_live_wallet_position as l final
              )
              where user_address != ''
              group by user_address
            ) as wallets
            left join
            (
              select
                user_address,
                count() as completed_event_count,
                countIf(event_realized_pnl > 0) as profitable_event_count,
                countIf(event_realized_pnl < 0) as losing_event_count,
                sum(event_realized_pnl) as realized_pnl,
                sumIf(event_realized_pnl, event_realized_pnl > 0) as positive_pnl,
                sumIf(event_realized_pnl, event_realized_pnl < 0) as negative_pnl,
                sum(event_buy_notional) as buy_notional,
                sum(event_sell_notional) as sell_notional,
                sum(event_traded_notional) as traded_notional,
                sum(event_trade_count) as trade_count,
                avgIf(event_roi, isFinite(event_roi)) as avg_event_roi,
                max(event_realized_pnl) as best_event_pnl,
                min(event_realized_pnl) as worst_event_pnl
              from
              (
                select
                  user_address,
                  realized_pnl as event_realized_pnl,
                  buy_notional as event_buy_notional,
                  sell_notional as event_sell_notional,
                  traded_notional as event_traded_notional,
                  trade_count as event_trade_count,
                  roi as event_roi
                from mart_event_wallet_pnl as p final
                where user_address != ''
              )
              group by user_address
            ) as pnl on wallets.user_address = pnl.user_address
            left join
            (
              select
                user_address,
                count() as active_position_count,
                uniqExact(event_id) as active_event_count,
                sum(unrealized_pnl_estimate) as active_unrealized_pnl_estimate
              from mart_live_wallet_position as l final
              where user_address != ''
              group by user_address
            ) as live on wallets.user_address = live.user_address
            left join
            (
              select
                user_address,
                argMax(category, category_notional) as favorite_category,
                max(category_notional) as favorite_category_notional
              from
              (
                select
                  user_address,
                  category,
                  sum(traded_notional) as category_notional
                from mart_event_wallet_pnl as p final
                where user_address != ''
                group by user_address, category
              )
              group by user_address
            ) as category on wallets.user_address = category.user_address
            left join
            (
              select
                user_address,
                min(first_trade_at) as first_trade_at,
                max(last_trade_at) as last_trade_at
              from
              (
                select user_address, first_trade_at, last_trade_at
                from mart_event_wallet_pnl as p final
                union all
                select user_address, first_trade_at, last_trade_at
                from mart_live_wallet_position as l final
              )
              where user_address != ''
              group by user_address
            ) as bounds on wallets.user_address = bounds.user_address
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_wallet_reputation").strip() or "0")
        return MartBuildResult(mart="wallet_reputation", rows=rows)

    def build_event_anomaly_signals(
        self,
        *,
        large_trade_threshold: float = 1_000.0,
        liquidity_ratio_threshold: float = 0.10,
        coordinated_wallet_threshold: int = 5,
        coordinated_notional_threshold: float = 5_000.0,
        since_hours: int = 168,
    ) -> MartBuildResult:
        if since_hours <= 0:
            raise ValueError("since_hours must be positive")
        if coordinated_wallet_threshold <= 0:
            raise ValueError("coordinated_wallet_threshold must be positive")
        self.clickhouse.execute(
            f"""
            insert into mart_event_anomaly_signal
            select
              toString(cityHash64('large_trade_low_liquidity', trade_id)) as signal_id,
              'large_trade_low_liquidity' as signal_type,
              if(
                notional >= {large_trade_threshold * 5}
                  or notional / greatest(liquidity, 1.0) >= {liquidity_ratio_threshold * 2},
                'high',
                'medium'
              ) as severity,
              event_id,
              market_id,
              condition_id,
              token_id,
              outcome,
              user_address,
              timestamp as occurred_at,
              'trade_notional_to_liquidity' as metric_name,
              cast(if(liquidity <= 0, notional, notional / greatest(liquidity, 1.0)), 'Float64')
                as metric_value,
              cast(liquidity, 'Float64') as baseline_value,
              cast({liquidity_ratio_threshold}, 'Float64') as threshold,
              concat(
                '{{"trade_id":"', trade_id,
                '","notional":', toString(notional),
                ',"liquidity":', toString(liquidity),
                ',"price":', toString(price),
                ',"size":', toString(size),
                '}}'
              ) as evidence_json,
              'Large trade in a low-liquidity market; treat as an abnormal activity signal.'
                as message,
              'medium' as uncertainty,
              now64(3) as updated_at
            from
            (
              select
                trades.trade_id as trade_id,
                trades.timestamp as timestamp,
                trades.condition_id as condition_id,
                trades.token_id as token_id,
                trades.user_address as user_address,
                trades.price as price,
                trades.size as size,
                trades.notional as notional,
                markets.event_id as event_id,
                markets.market_id as market_id,
                markets.liquidity as liquidity,
                ifNull(tokens.outcome, '') as outcome
              from
              (
                select
                  trade_id,
                  anyLast(raw_timestamp) as timestamp,
                  anyLast(raw_condition_id) as condition_id,
                  anyLast(raw_token_id) as token_id,
                  anyLast(raw_user_address) as user_address,
                  anyLast(raw_price) as price,
                  anyLast(raw_size) as size,
                  anyLast(raw_notional) as notional
                from
                (
                  select
                    trade_id,
                    timestamp as raw_timestamp,
                    condition_id as raw_condition_id,
                    token_id as raw_token_id,
                    user_address as raw_user_address,
                    price as raw_price,
                    size as raw_size,
                    notional as raw_notional
                  from fact_trade
                  where user_address != ''
                    and condition_id != ''
                    and token_id != ''
                    and timestamp >= now64(3) - interval {since_hours} hour
                )
                group by trade_id
              ) as trades
              inner join
              (
                select
                  raw_condition_id as condition_id,
                  anyLast(raw_market_id) as market_id,
                  anyLast(raw_event_id) as event_id,
                  anyLast(raw_liquidity) as liquidity
                from
                (
                  select
                    condition_id as raw_condition_id,
                    market_id as raw_market_id,
                    event_id as raw_event_id,
                    liquidity as raw_liquidity
                  from dim_market as m final
                  where condition_id != '' and event_id != ''
                )
                group by condition_id
              ) as markets on trades.condition_id = markets.condition_id
              left join dim_outcome_token as tokens final
                on tokens.market_id = markets.market_id
               and tokens.token_id = trades.token_id
            )
            where notional >= {large_trade_threshold}
              and (
                liquidity <= 0
                or notional / greatest(liquidity, 1.0) >= {liquidity_ratio_threshold}
              )
            union all
            select
              toString(cityHash64('coordinated_like_buying', event_id, market_id, token_id, toString(bucket)))
                as signal_id,
              'coordinated_like_buying' as signal_type,
              if(
                wallet_count >= {coordinated_wallet_threshold * 2}
                  or buy_notional >= {coordinated_notional_threshold * 2},
                'high',
                'medium'
              ) as severity,
              event_id,
              market_id,
              condition_id,
              token_id,
              outcome,
              '' as user_address,
              bucket as occurred_at,
              'wallet_count' as metric_name,
              cast(wallet_count, 'Float64') as metric_value,
              cast(buy_notional, 'Float64') as baseline_value,
              cast({coordinated_wallet_threshold}, 'Float64') as threshold,
              concat(
                '{{"wallet_count":', toString(wallet_count),
                ',"buy_notional":', toString(buy_notional),
                ',"trade_count":', toString(trade_count),
                ',"window_minutes":10',
                '}}'
              ) as evidence_json,
              'Multiple wallets bought the same outcome in a short window; coordinated-like signal only.'
                as message,
              'high' as uncertainty,
              now64(3) as updated_at
            from
            (
              select
                markets.event_id as event_id,
                markets.market_id as market_id,
                trades.condition_id as condition_id,
                trades.token_id as token_id,
                anyLast(ifNull(tokens.outcome, '')) as outcome,
                toStartOfInterval(trades.timestamp, interval 10 minute) as bucket,
                uniqExact(trades.user_address) as wallet_count,
                sum(trades.notional) as buy_notional,
                count() as trade_count
              from
              (
                select
                  trade_id,
                  anyLast(raw_timestamp) as timestamp,
                  anyLast(raw_condition_id) as condition_id,
                  anyLast(raw_token_id) as token_id,
                  anyLast(raw_user_address) as user_address,
                  anyLast(raw_side) as side,
                  anyLast(raw_notional) as notional
                from
                (
                  select
                    trade_id,
                    timestamp as raw_timestamp,
                    condition_id as raw_condition_id,
                    token_id as raw_token_id,
                    user_address as raw_user_address,
                    side as raw_side,
                    notional as raw_notional
                  from fact_trade
                  where user_address != ''
                    and condition_id != ''
                    and token_id != ''
                    and timestamp >= now64(3) - interval {since_hours} hour
                )
                group by trade_id
              ) as trades
              inner join
              (
                select
                  raw_condition_id as condition_id,
                  anyLast(raw_market_id) as market_id,
                  anyLast(raw_event_id) as event_id
                from
                (
                  select
                    condition_id as raw_condition_id,
                    market_id as raw_market_id,
                    event_id as raw_event_id
                  from dim_market as m final
                  where condition_id != '' and event_id != ''
                )
                group by condition_id
              ) as markets on trades.condition_id = markets.condition_id
              left join dim_outcome_token as tokens final
                on tokens.market_id = markets.market_id
               and tokens.token_id = trades.token_id
              where trades.side = 'BUY'
              group by markets.event_id, markets.market_id, trades.condition_id, trades.token_id, bucket
              having wallet_count >= {coordinated_wallet_threshold}
                 and buy_notional >= {coordinated_notional_threshold}
            )
            union all
            select
              toString(cityHash64('closed_market_unresolved_outcome', market_id, toString(winner_count)))
                as signal_id,
              'closed_market_unresolved_outcome' as signal_type,
              if(winner_count > 1, 'high', 'medium') as severity,
              event_id,
              market_id,
              condition_id,
              '' as token_id,
              '' as outcome,
              '' as user_address,
              ifNull(updated_at, now64(3)) as occurred_at,
              'winner_count' as metric_name,
              cast(winner_count, 'Float64') as metric_value,
              cast(1.0, 'Float64') as baseline_value,
              cast(1.0, 'Float64') as threshold,
              concat(
                '{{"outcomePrices":', JSONExtractString(raw_json, 'outcomePrices'),
                ',"outcomes":', JSONExtractString(raw_json, 'outcomes'),
                '}}'
              ) as evidence_json,
              'Closed market does not have exactly one winning outcome in Gamma metadata.'
                as message,
              'medium' as uncertainty,
              now64(3) as updated_at
            from
            (
              select
                event_id,
                market_id,
                condition_id,
                raw_json,
                updated_at,
                arrayCount(
                  price -> toFloat64OrZero(JSONExtractString(price)) >= 0.999,
                  JSONExtractArrayRaw(JSONExtractString(raw_json, 'outcomePrices'))
                ) as winner_count
              from dim_market as m final
              where event_id != ''
                and closed = true
                and JSONExtractString(raw_json, 'outcomePrices') != ''
            )
            where winner_count != 1
            """
        )
        rows = int(
            self.clickhouse.query_text("select count() from mart_event_anomaly_signal").strip()
            or "0"
        )
        return MartBuildResult(mart="event_anomaly_signal", rows=rows)

    def build_analytics_core(self) -> list[MartBuildResult]:
        return [
            self.build_event_wallet_pnl(),
            self.build_live_wallet_positions(),
            self.build_wallet_reputation(),
            self.build_event_anomaly_signals(),
        ]

    def build_alerts(
        self,
        *,
        price_move_threshold: float = 0.10,
        spread_threshold: float = 0.05,
        whale_notional_threshold: float = 1_000.0,
        since_hours: int = 24,
    ) -> MartBuildResult:
        self.clickhouse.execute(
            f"""
            insert into mart_alert
            select
              toString(cityHash64('price_move', token_id, toString(occurred_at), toString(metric_value))) as alert_id,
              'price_move' as alert_type,
              if(metric_value >= {price_move_threshold * 2}, 'high', 'medium') as severity,
              token_id,
              '' as market_id,
              '' as user_address,
              occurred_at,
              'relative_price_change' as metric_name,
              metric_value,
              {price_move_threshold} as threshold,
              concat('Price moved ', toString(round(metric_value * 100, 2)), '% from previous minute close') as message,
              concat('{{"close":', toString(close), ',"previous_close":', toString(previous_close), '}}') as raw_json,
              now64(3) as updated_at
            from
            (
              select
                token_id,
                bucket as occurred_at,
                close,
                previous_close,
                if(previous_close = 0, 0.0, abs(close - previous_close) / previous_close) as metric_value
              from
              (
                select
                  token_id,
                  bucket,
                  close,
                  lagInFrame(close) over (
                    partition by token_id
                    order by bucket
                    rows between 1 preceding and current row
                  ) as previous_close
                from mart_market_1m
                where bucket >= now() - interval {since_hours} hour
              )
            )
            where previous_close > 0 and metric_value >= {price_move_threshold}
            """
        )
        self.clickhouse.execute(
            f"""
            insert into mart_alert
            select
              toString(cityHash64('wide_spread', token_id, toString(occurred_at), toString(spread))) as alert_id,
              'wide_spread' as alert_type,
              if(spread >= {spread_threshold * 2}, 'high', 'medium') as severity,
              token_id,
              market_id,
              '' as user_address,
              occurred_at,
              'spread_ratio' as metric_name,
              spread as metric_value,
              {spread_threshold} as threshold,
              concat('Order book spread is ', toString(round(spread * 100, 2)), '%') as message,
              concat(
                '{{"best_bid":', toString(best_bid),
                ',"best_ask":', toString(best_ask),
                ',"bid_depth":', toString(bid_depth),
                ',"ask_depth":', toString(ask_depth),
                '}}'
              ) as raw_json,
              now64(3) as updated_at
            from
            (
              select
                token_id,
                captured_at as occurred_at,
                market as market_id,
                best_bid,
                best_ask,
                bid_depth,
                ask_depth,
                (best_ask - best_bid) / best_ask as spread
              from fact_orderbook_snapshot
              where captured_at >= now() - interval {since_hours} hour
                and best_bid is not null
                and best_ask is not null
                and best_ask > 0
            )
            where spread >= {spread_threshold}
            """
        )
        self.clickhouse.execute(
            f"""
            insert into mart_alert
            select
              toString(cityHash64('whale_trade', trade_id)) as alert_id,
              'whale_trade' as alert_type,
              if(metric_value >= {whale_notional_threshold * 5}, 'high', 'medium') as severity,
              token_id,
              market_id,
              user_address,
              occurred_at,
              'trade_notional' as metric_name,
              metric_value,
              {whale_notional_threshold} as threshold,
              concat('Large trade notional ', toString(round(metric_value, 2))) as message,
              concat('{{"trade_id":"', trade_id, '"}}') as raw_json,
              now64(3) as updated_at
            from
            (
              select
                token_id,
                market_id,
                user_address,
                timestamp as occurred_at,
                notional as metric_value,
                trade_id
              from fact_trade
              where timestamp >= now() - interval {since_hours} hour
                and notional >= {whale_notional_threshold}
            )
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_alert").strip() or "0")
        return MartBuildResult(mart="alert", rows=rows)

    def build_trade_reconciliation(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_trade_reconciliation
            select
              toString(cityHash64(transaction_hash, token_id, agg_data_trade_id, toString(agg_chain_log_index))) as reconciliation_id,
              transaction_hash,
              token_id,
              agg_data_trade_id as data_trade_id,
              agg_chain_log_index as chain_log_index,
              multiIf(
                agg_data_trade_id = '', 'chain_only',
                agg_chain_log_index = 0 and agg_chain_missing = 1, 'data_only',
                price_delta <= 0.000001 and size_delta <= 0.000001, 'matched',
                'mismatch'
              ) as status,
              agg_side_data as side_data,
              agg_side_chain as side_chain,
              agg_price_data as price_data,
              agg_price_chain as price_chain,
              agg_size_data as size_data,
              agg_size_chain as size_chain,
              agg_notional_data as notional_data,
              agg_notional_chain as notional_chain,
              price_delta,
              size_delta,
              notional_delta,
              now64(3) as checked_at
            from
            (
              select
                transaction_hash,
                token_id,
                agg_data_trade_id,
                agg_chain_log_index,
                agg_chain_missing,
                agg_side_data,
                agg_side_chain,
                agg_price_data,
                agg_price_chain,
                agg_size_data,
                agg_size_chain,
                agg_notional_data,
                agg_notional_chain,
                abs(ifNull(agg_price_data, 0) - ifNull(agg_price_chain, 0)) as price_delta,
                abs(ifNull(agg_size_data, 0) - ifNull(agg_size_chain, 0)) as size_delta,
                abs(ifNull(agg_notional_data, 0) - ifNull(agg_notional_chain, 0)) as notional_delta
              from
              (
                select
                  transaction_hash,
                  token_id,
                  anyIf(data_trade_id, data_trade_id != '') as agg_data_trade_id,
                  max(chain_log_index) as agg_chain_log_index,
                  min(chain_missing) as agg_chain_missing,
                  anyIf(side_data, data_trade_id != '') as agg_side_data,
                  anyIf(side_chain, chain_missing = 0) as agg_side_chain,
                  anyIf(price_data, data_trade_id != '') as agg_price_data,
                  anyIf(price_chain, chain_missing = 0) as agg_price_chain,
                  anyIf(size_data, data_trade_id != '') as agg_size_data,
                  anyIf(size_chain, chain_missing = 0) as agg_size_chain,
                  anyIf(notional_data, data_trade_id != '') as agg_notional_data,
                  anyIf(notional_chain, chain_missing = 0) as agg_notional_chain
                from
                (
                  select
                    transaction_hash,
                    token_id,
                    trade_id as data_trade_id,
                    0 as chain_log_index,
                    1 as chain_missing,
                    side as side_data,
                    '' as side_chain,
                    price as price_data,
                    cast(null, 'Nullable(Float64)') as price_chain,
                    size as size_data,
                    cast(null, 'Nullable(Float64)') as size_chain,
                    notional as notional_data,
                    cast(null, 'Nullable(Float64)') as notional_chain
                  from fact_trade
                  where transaction_hash != '' and token_id != ''
                  union all
                  select
                    transaction_hash,
                    token_id,
                    '' as data_trade_id,
                    log_index as chain_log_index,
                    0 as chain_missing,
                    '' as side_data,
                    side as side_chain,
                    cast(null, 'Nullable(Float64)') as price_data,
                    price as price_chain,
                    cast(null, 'Nullable(Float64)') as size_data,
                    size as size_chain,
                    cast(null, 'Nullable(Float64)') as notional_data,
                    notional as notional_chain
                  from fact_exchange_fill final
                  where transaction_hash != '' and token_id != ''
                )
                group by transaction_hash, token_id
              )
            )
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_trade_reconciliation").strip() or "0")
        return MartBuildResult(mart="trade_reconciliation", rows=rows)

    def build_settlement_audit(self) -> MartBuildResult:
        self.clickhouse.execute(
            """
            insert into mart_settlement_audit
            select
              condition_id,
              agg_market_id as market_id,
              agg_question as question,
              agg_market_closed as market_closed,
              agg_redeem_count as redeem_count,
              agg_redeemed_amount as redeemed_amount,
              agg_first_redeem_block as first_redeem_block,
              agg_last_redeem_block as last_redeem_block,
              multiIf(
                agg_market_closed = 1 and agg_redeem_count > 0, 'closed_with_redeems',
                agg_market_closed = 1 and agg_redeem_count = 0, 'closed_no_redeems',
                agg_market_closed = 0 and agg_redeem_count > 0, 'open_with_redeems',
                'open_no_redeems'
              ) as status,
              now64(3) as checked_at
            from
            (
              select
                condition_id,
                anyIf(market_id, market_id != '') as agg_market_id,
                anyIf(question, question != '') as agg_question,
                max(market_closed) as agg_market_closed,
                sum(redeem_count) as agg_redeem_count,
                sum(redeemed_amount) as agg_redeemed_amount,
                minIf(first_redeem_block, first_redeem_block > 0) as agg_first_redeem_block,
                max(last_redeem_block) as agg_last_redeem_block
              from
              (
                select
                  condition_id,
                  market_id,
                  question,
                  closed as market_closed,
                  0 as redeem_count,
                  0.0 as redeemed_amount,
                  0 as first_redeem_block,
                  0 as last_redeem_block
                from dim_market
                where condition_id != ''
                union all
                select
                  condition_id,
                  '' as market_id,
                  '' as question,
                  false as market_closed,
                  count() as redeem_count,
                  sum(amount) as redeemed_amount,
                  min(block_number) as first_redeem_block,
                  max(block_number) as last_redeem_block
                from fact_ctf_lifecycle_event final
                where event_type = 'redeem' and condition_id != ''
                group by condition_id
              )
              group by condition_id
            )
            """
        )
        rows = int(self.clickhouse.query_text("select count() from mart_settlement_audit").strip() or "0")
        return MartBuildResult(mart="settlement_audit", rows=rows)

    def build_collector_health(self, *, postgres_dsn: str) -> MartBuildResult:
        psycopg = import_psycopg()

        rows: list[dict[str, object]] = []
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    select
                      date_trunc('minute', coalesce(finished_at, started_at)) as bucket,
                      node_id,
                      status,
                      count(*) as runs,
                      coalesce(sum(pages), 0) as pages,
                      coalesce(sum(items), 0) as items,
                      count(*) filter (where error is not null and error != '') as errors
                    from collector_runs
                    group by 1, 2, 3
                    order by 1, 2, 3
                    """
                )
                for bucket, node_id, status, runs, pages, items, errors in cursor.fetchall():
                    rows.append(
                        {
                            "bucket": ch_datetime(bucket),
                            "node_id": str(node_id),
                            "status": str(status),
                            "runs": int(runs),
                            "pages": int(pages),
                            "items": int(items),
                            "errors": int(errors),
                        }
                    )
        updated_at = datetime.now(UTC)
        for row in rows:
            row["updated_at"] = ch_datetime64(updated_at)
        if rows:
            self.clickhouse.insert("mart_collector_health", rows)
        return MartBuildResult(mart="collector_health", rows=len(rows))


def import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "Collector health build requires psycopg. Install project dependencies with `pip install -e .`."
        ) from exc
    return psycopg


def ch_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def ch_datetime64(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
