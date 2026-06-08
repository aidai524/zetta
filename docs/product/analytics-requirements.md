# Analytics Product Requirements

## Scope

Build a full-market analytics product for Polymarket events, markets, wallets, and
public trading signals using data that Zetta can collect from public APIs, CLOB market
data, WebSocket streams, and Polygon chain logs.

The product is category-agnostic. It must work across Polymarket's market universe,
including politics, crypto, sports, macro, culture, weather, science, and any new
category that appears in Gamma metadata. No feature should be hard-coded to one event,
one category, or one market theme.

This document intentionally excludes features that require private order ownership data.
For example, current order book price levels can be shown, but current bid/ask order
owners cannot be shown unless a public L3/order-owner feed becomes available.

## Core Users

- Traders researching active events and smart-money behavior.
- Analysts reviewing completed events and wallet PnL.
- Operators monitoring abnormal market activity.
- Power users asking custom questions through natural language and SQL.

## Product Modules

### 1. Global Market Overview

Purpose: make the first product surface a Polymarket-wide intelligence workspace, not
an event-specific dashboard. Users should start from the whole market universe and then
drill down into a selected event, market, token, category, or wallet.

Frontend requirements:

- Full-market KPI strip with active markets, completed markets, 24h volume, total
  liquidity, tracked wallets, anomaly count, and latest data timestamp.
- Market explorer with category, tag, status, time range, volume, liquidity, spread,
  price movement, and data freshness filters.
- Trending markets table showing title, category, status, volume, liquidity, price move,
  smart-money direction, anomaly severity, and quick links to event/market detail.
- Category flow panel comparing volume, liquidity, active wallet count, PnL dispersion,
  and anomaly density across categories and tags.
- Smart-money activity feed across all active markets.
- Anomaly feed across all markets with evidence links and severity labels.
- Saved global views for common workflows such as active markets, completed markets,
  high-liquidity markets, high-volatility markets, and smart-money watchlists.

Backend requirements:

- `GET /markets/overview`
- `GET /markets/trending`
- `GET /markets/movers`
- `GET /categories/summary`
- `GET /signals/anomalies`
- `GET /wallets/smart-money/activity`

Required marts:

- `mart_market_overview`
- `mart_market_trending`
- `mart_market_mover`
- `mart_category_flow`
- `mart_global_smart_money_activity`

### 2. Market And Event Analytics

Purpose: let users inspect any Polymarket event or market group from metadata to market
structure, price, wallet flow, and settlement outcome.

Frontend requirements:

- Global search by title, slug, event id, market id, condition id, token id, category,
  tag, and status.
- Event or market summary header with title, status, start/end time, volume, liquidity,
  count, token count, and latest data timestamp.
- Market table grouped by outcome, group item, topic, or category with active/closed
  state, price, volume, liquidity, spread, and token ids.
- Volume and liquidity charts over time when historical snapshots are available.
- Price chart per outcome token.
- Wallet flow panel with net buy, net sell, trade count, notional, realized PnL for
  completed events, and mark-to-market PnL for active events.
- Order book panel showing top bid/ask price levels and depth, without wallet owners.
- Settlement panel for completed events with winning outcome, redeem activity, chain
  reconciliation status, and unresolved anomalies.

Backend requirements:

- `GET /markets/search`
- `GET /events/search`
- `GET /events/{event_id}/summary`
- `GET /events/{event_id}/markets`
- `GET /events/{event_id}/volume-liquidity`
- `GET /events/{event_id}/wallet-flow`
- `GET /events/{event_id}/orderbook`
- `GET /events/{event_id}/settlement`

Required marts:

- `mart_market_search`
- `mart_event_summary`
- `mart_event_market_snapshot`
- `mart_event_volume_liquidity_daily`
- `mart_event_wallet_flow`
- `mart_event_settlement_status`

### 3. Completed Event PnL

Purpose: rank wallets by realized performance after event completion.

Frontend requirements:

- Completed event leaderboard with top profit, top loss, highest notional, highest trade
  count, and highest ROI.
- Wallet detail drawer showing trades, fills, deposits/withdrawals where available,
  redeemed amount, final position, and PnL attribution.
- Filters by category, tag, time range, min notional, min trades, and market status.
- CSV export for leaderboard and wallet trade history.

Backend requirements:

- `GET /events/{event_id}/pnl-leaderboard`
- `GET /wallets/{wallet}/event-pnl?event_id=...`
- `GET /wallets/{wallet}/trades?event_id=...`

Required marts:

- `mart_event_wallet_pnl`
- `mart_wallet_event_history`
- `mart_wallet_trade_timeline`

PnL rules:

- Prefer chain-confirmed fills when available.
- Use Data API trades as public trade history and cross-check against chain fills.
- Use final settlement/redeem data to compute realized PnL for completed events.
- Show reconciliation status when Data API and chain results differ.

### 4. Active Market Smart Money

Purpose: identify high-quality wallets participating in active markets/events and expose
their current positioning.

Frontend requirements:

- Active market smart-money table with wallet, historical reputation, current net
  position, average entry, current mark price, unrealized PnL, trade count, and latest
  action.
- Highlight wallets with strong historical PnL or high category-specific win rate.
- Show wallet timeline: first entry, adds, reductions, flips, and recent activity.
- Compare smart-money direction against public market price movement.
- Watchlist wallets, events, markets, and tokens.

Backend requirements:

- `GET /markets/{condition_id}/smart-money`
- `GET /events/{event_id}/smart-money`
- `GET /wallets/{wallet}/live-positions`
- `GET /wallets/{wallet}/strategy-profile`

Required marts:

- `mart_live_wallet_position`
- `mart_wallet_reputation`
- `mart_wallet_category_performance`
- `mart_smart_money_signal`

Marking rules:

- Mark active positions using latest price history or order book mid price.
- Show unrealized PnL as an estimate, not final PnL.
- Include data freshness timestamp and price source.

### 5. Wallet Intelligence

Purpose: make every public wallet inspectable as a trading profile.

Frontend requirements:

- Wallet overview with total trades, notional, realized PnL, unrealized PnL, win rate,
  average holding period, favorite categories, and recent active positions.
- Historical event table with PnL, ROI, side, notional, timing, and result.
- Category performance chart.
- Counterparty/fill network when chain maker/taker data is available.
- Similar wallets by strategy or market overlap.

Backend requirements:

- `GET /wallets/{wallet}/profile`
- `GET /wallets/{wallet}/events`
- `GET /wallets/{wallet}/positions`
- `GET /wallets/{wallet}/counterparties`
- `GET /wallets/{wallet}/similar`

Required marts:

- `mart_wallet_reputation`
- `mart_wallet_event_history`
- `mart_wallet_position_history`
- `mart_wallet_counterparty_network`

### 6. Market Anomaly Signals

Purpose: detect suspicious or unusual public trading patterns without making unsupported
claims about insider activity.

Frontend requirements:

- Market anomaly feed with signal type, severity, timestamp, affected market/outcome,
  involved wallets, and evidence.
- Wallet risk signal panel showing repeated abnormal behavior patterns.
- Signal detail page with before/after price chart, trade timeline, wallet flow, and
  related wallets.
- Filters by signal type, category, tag, severity, event/market status, and time range.

Supported signal types:

- Early large position before major price movement.
- Repeated high-profit timing in completed events.
- Abnormal volume spike in low-liquidity markets.
- Coordinated wallet cluster buying the same side in a short window.
- Large maker/taker concentration from chain fills.
- Sudden liquidity wall or liquidity withdrawal at price levels.
- Settlement or redeem mismatch.

Backend requirements:

- `GET /signals/anomalies`
- `GET /events/{event_id}/signals`
- `GET /wallets/{wallet}/signals`
- `GET /signals/{signal_id}`

Required marts:

- `mart_event_anomaly_signal`
- `mart_wallet_risk_signal`
- `mart_wallet_cluster`
- `mart_orderbook_anomaly`

Language rules:

- Use labels such as `abnormal`, `suspicious`, `high-risk`, `coordinated-like`, or
  `insider-like behavior signal`.
- Do not state that a wallet is an internal wallet, insider wallet, or manipulator
  without external evidence.
- Every signal must include supporting data and uncertainty.

### 7. SQL Agent / Data Assistant

Purpose: let users ask natural-language questions and get a safe SQL-backed answer.

Frontend requirements:

- Natural language query input.
- Suggested starter questions.
- Generated SQL preview.
- Result table and chart suggestions.
- Explanation of fields, filters, and assumptions.
- Save/share query.
- Query history.

Example questions:

- Find the most profitable wallets in completed markets over the last 90 days.
- Show wallets accumulating a selected outcome token during the last 7 days.
- Which wallets entered before a 20% price move in active markets?
- Show markets with the largest liquidity withdrawal in the last 24 hours.

Backend requirements:

- `POST /agent/sql/plan`
- `POST /agent/sql/execute`
- `GET /agent/schema`
- `GET /agent/examples`

Safety requirements:

- Read-only SQL only.
- Block `insert`, `update`, `delete`, `drop`, `alter`, `truncate`, and external table
  functions.
- Default `LIMIT`.
- Query timeout.
- Cost guard by scanned rows or bytes when available.
- Prefer curated marts over raw tables.
- Return SQL, results, explanation, and caveats.

Required metadata:

- Table catalog with descriptions.
- Column catalog with business meanings.
- Join graph for event, market, token, wallet, and chain tables.
- Approved query templates.
- Example prompt templates that use placeholders such as `<event_id>`, `<condition_id>`,
  `<token_id>`, `<category>`, and `<wallet>`, not hard-coded market names.

## Data Dependencies

Required ingestion:

- Gamma event and market backfill.
- All-event Data API trades backfill and incremental updates.
- CLOB price history backfill per outcome token.
- CLOB REST book snapshots and WebSocket book/price changes.
- Polygon exchange logs, ERC1155 transfers, split/merge/redeem lifecycle events.
- Holder, position, and open interest snapshots for public Data API surfaces.

Required normalization:

- Event -> market -> token mapping.
- Wallet trade/fill timeline.
- Wallet position ledger.
- Event final outcome and settlement state.
- L2 order book levels and price-change history.
- Chain fill maker/taker attribution.
- Category-agnostic labels for outcomes and market groups.

## Explicit Non-Goals

- Do not show current order book wallet owners from L2 book data.
- Do not claim a wallet is an internal wallet or insider wallet without external proof.
- Do not rely on private authenticated user-channel data for public market-wide analytics.
- Do not expose write-capable SQL in the data assistant.
- Do not hard-code product flows or examples around one market, event, sport, election,
  crypto asset, or category.

## Initial Milestones

1. Build global market overview, market search, trending markets, and category flow UI.
2. Build event summary, market table, and current order book price-level UI.
3. Build completed event wallet PnL leaderboard.
4. Build active market smart-money wallet table.
5. Build wallet profile page.
6. Build anomaly signal MVP.
7. Build SQL Agent MVP against curated marts.
