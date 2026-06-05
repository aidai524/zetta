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
                    trade_id,
                    anyLast(user_address) as trader,
                    anyLast(side) as side,
                    anyLast(size) as size,
                    anyLast(notional) as notional,
                    anyLast(timestamp) as timestamp
                  from fact_trade
                  where user_address != ''
                  group by trade_id
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
                            "bucket": bucket,
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
            row["updated_at"] = updated_at
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
