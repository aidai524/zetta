# Polycop Wallet Signals API

本文档描述“聪明钱包/有用钱包”信号缓存接口，供 Discovery 页面、Chrome 插件或其他客户端查询。

## Base URL

Discovery 域名：

```text
https://discovery.prophet.zone/api
```

API 域名：

```text
https://api-zetta.prophet.zone
```

本机服务：

```text
http://127.0.0.1:8088
```

## 数据刷新与缓存

- 定时任务：`zetta-polycop-wallet-signals.timer`
- 刷新频率：每 10 分钟
- 数据源：`https://polycop.ai/v1/web/trade`
- 排序源：Polycop `score desc`
- 分页请求：POST body 使用 `page_size=15`，按 Polycop 前端一页 15 个钱包分页抓取，默认最多 100 页。
- 缓存表：Postgres `polycop_wallet_signal_cache`
- 缓存键：`latest`
- API 默认只读缓存，不在请求时现场抓 Polycop。
- 如果定时刷新 Polycop 失败，会保留上一版可用结果，并把 `status` 标记为 `stale_error`，避免页面或插件结果变空。

当前 Polycop 返回里的 `totalVolume` 字段内容异常，会返回地址字符串，所以评分和接口均不使用该字段。

## 分段说明

每个钱包会有一个或多个 `segments`：

| Segment | 含义 | 主要规则 |
|---|---|---|
| `stable` | 稳定型聪明钱包 | 样本不少、总盈利和近 20 场盈利为正、胜率和盈亏比较好、滑点不过高、近期盈利不过度集中 |
| `flow` | 高频/大样本资金流钱包 | 交易市场数很高，总盈利和近 20 场盈利为正 |
| `burst` | 近期爆发型钱包 | 近 20 场盈利很高，且盈利集中在近期或样本较小 |
| `watch` | 观察钱包 | 不满足以上分段，但仍在 Polycop 榜单内 |
| `ai_top` | 综合排序 | 按本系统 `ai_score` 从高到低排序 |
| `all` | 全部缓存钱包 | 返回缓存内全部钱包，仍按 `ai_score` 排序 |

`ai_score` 是 0-100 分，综合 Polycop 原始分、总盈利、近 20 场盈利、胜率、近 20 场胜率、市场数、盈亏比、低滑点等指标，并对样本太小、近期盈利过度集中、高滑点、对冲比例过高、回测差异过大做扣分。

## Endpoint: Summary

用于插件 badge、首页摘要、定时任务状态检查。

```http
GET /wallets/polycop-signals/summary
```

完整链接：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals/summary
```

### Query Parameters

| 参数 | 类型 | 默认值 | 说明 |
|---|---:|---:|---|
| `max_age_seconds` | integer | `0` | 可接受的最大缓存年龄。`0` 表示接受任意年龄的缓存。大于 0 时，如果缓存超过该秒数，会返回 `missing_cache`。最大值 604800。 |

### Response

```json
{
  "status": "ok",
  "source": "polycop",
  "cache": {
    "source": "postgres",
    "hit": true,
    "cache_key": "latest",
    "refreshed_at": "2026-06-22T10:30:14.816372+00:00",
    "generated_at": "2026-06-22T10:30:01+00:00",
    "age_seconds": 180.1,
    "trigger_reason": "scheduled",
    "error": null
  },
  "summary": {
    "wallet_count": 787,
    "raw_wallet_count": 823,
    "stable_count": 23,
    "flow_count": 35,
    "burst_count": 122,
    "top_wallets": [
      {
        "rank": 1,
        "address": "0x71abe97b83eaba3f06cb04fd4d9a03ee37d2f015",
        "user_name": "groth",
        "x_name": "",
        "ai_score": 80.38,
        "primary_segment": "stable",
        "segments": ["stable"],
        "actual_total_pnl": 115885.7956,
        "recent20_pnl": 18292.0825,
        "win_rate": 72.5424,
        "total_markets": 295
      }
    ],
    "notes": [
      "totalVolume from the current Polycop response is not used because it contains an address value.",
      "Scores are percentile-ranked across the fetched Polycop leaderboard rows and penalize tiny sample size, high recent concentration, high slippage, and heavy hedging."
    ]
  },
  "parameters": {
    "source_url": "https://polycop.ai/v1/web/trade",
    "sort_options": [{"field": "score", "descending": true}],
    "page_size": 15,
    "max_pages": 100,
    "pages_fetched": 17,
    "result_limit": 2000
  }
}
```

## Endpoint: Wallet List

用于列表页、插件弹层、筛选和搜索。

```http
GET /wallets/polycop-signals
```

完整链接：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=stable&limit=20
```

### Query Parameters

| 参数 | 类型 | 默认值 | 取值范围 | 说明 |
|---|---:|---:|---|---|
| `segment` | string | `ai_top` | `ai_top`, `stable`, `flow`, `burst`, `watch`, `all` | 查询哪个分段。非法值会回退到 `ai_top`。 |
| `limit` | integer | `50` | 1-500 | 返回条数。 |
| `offset` | integer | `0` | 0-100000 | 分页偏移。 |
| `q` | string | 空 | 任意字符串 | 搜索 `address`、`user_name`、`x_name`、`primary_segment`。 |
| `min_ai_score` | number | `0` | 0-100 | 只返回 `ai_score >= min_ai_score` 的钱包。 |
| `max_age_seconds` | integer | `0` | 0-604800 | 可接受的最大缓存年龄。`0` 表示接受任意年龄的缓存。 |

### Request Examples

查询稳定型钱包：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=stable&limit=20
```

查询资金流钱包：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=flow&limit=20
```

查询近期爆发钱包：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=burst&limit=20
```

搜索用户名或地址：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=all&q=groth&limit=10
```

只看高分钱包：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=ai_top&min_ai_score=70&limit=50
```

分页：

```text
https://discovery.prophet.zone/api/wallets/polycop-signals?segment=stable&limit=20&offset=20
```

### Response

```json
{
  "status": "ok",
  "source": "polycop",
  "cache": {
    "source": "postgres",
    "hit": true,
    "cache_key": "latest",
    "refreshed_at": "2026-06-22T10:30:14.816372+00:00",
    "generated_at": "2026-06-22T10:30:01+00:00",
    "age_seconds": 180.1,
    "trigger_reason": "scheduled",
    "error": null
  },
  "summary": {
    "wallet_count": 787,
    "raw_wallet_count": 823,
    "stable_count": 23,
    "flow_count": 35,
    "burst_count": 122,
    "top_wallets": []
  },
  "parameters": {
    "source_url": "https://polycop.ai/v1/web/trade",
    "sort_options": [{"field": "score", "descending": true}],
    "page_size": 15,
    "max_pages": 100,
    "pages_fetched": 17,
    "result_limit": 2000
  },
  "segment": "stable",
  "total": 23,
  "limit": 1,
  "offset": 0,
  "wallets": [
    {
      "rank": 1,
      "address": "0x71abe97b83eaba3f06cb04fd4d9a03ee37d2f015",
      "user_name": "groth",
      "x_name": "",
      "profile_image": "",
      "ai_score": 80.38,
      "source_score": 96.2941,
      "primary_segment": "stable",
      "segments": ["stable"],
      "reasons": ["stable_profit_sample", "low_slippage", "high_win_rate"],
      "metrics": {
        "balance": 124425.7924,
        "available": 29305.5093,
        "actual_total_pnl": 115885.7956,
        "backtest_total_pnl": 102308.2412,
        "recent20_pnl": 18292.0825,
        "recent20_backtest_pnl": 16239.7723,
        "win_rate": 72.5424,
        "recent20_win_rate": 80.0,
        "avg_profit_loss_ratio": 7.6773,
        "avg_market_roi": 35.3046,
        "avg_market_profit_rate": 0.0,
        "slippage_cost_rate": 11.7163,
        "recent20_slippage_cost_rate": 0.0,
        "total_markets": 295,
        "hedged_markets": 32
      },
      "derived": {
        "hedge_ratio": 0.1085,
        "recent_pnl_share": 0.1578,
        "backtest_gap_ratio": 0.1172
      }
    }
  ]
}
```

## Response Fields

## Endpoint: Polycop FIFA Wallets

用于把 Polycop 的聪明钱包榜单和我们自己的 FIFA 钱包数据做交集。这个接口不会覆盖
`/wallets/screener?scope=fifa`，只是提供一个独立的“第三方聪明钱包里交易过 FIFA 的钱包”列表。

```http
GET /wallets/polycop-fifa-signals
```

完整链接：

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100
```

### Query Parameters

| 参数 | 类型 | 默认值 | 说明 |
|---|---:|---:|---|
| `segment` | string | `ai_top` | Polycop 分段：`ai_top`, `stable`, `flow`, `burst`, `watch`, `all`。 |
| `limit` | integer | `100` | 返回条数，最大 500。 |
| `offset` | integer | `0` | 分页偏移。 |
| `candidate_limit` | integer | `max(limit + offset, 2000)` | 从 Polycop 缓存里参与 FIFA 交集的候选钱包数量，最大 2000。 |
| `min_ai_score` | number | `0` | 只看 Polycop `ai_score >= min_ai_score` 的钱包。 |
| `min_fifa_notional` | number | `0` | 只返回 FIFA 交易额达到该值的钱包。 |
| `min_fifa_events` | integer | `0` | 只返回至少参与 N 个 FIFA event 的钱包。 |
| `positive_fifa` | boolean | `false` | 为 `1/true` 时只返回 FIFA equity 为正的钱包。 |
| `active_24h` | boolean | `false` | 为 `1/true` 时只返回最近 24 小时有 FIFA 交易的钱包。 |
| `data_quality` | string | 空 | 可选 `estimate`, `missing_mark_price`, `needs_chain_balance`。 |
| `q` | string | 空 | 搜索 Polycop 地址、用户名、X 名称、分段。 |
| `max_age_seconds` | integer | `0` | 可接受的最大缓存年龄。 |

### Examples

Top 100 Polycop smart wallets that traded FIFA:

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100
```

Search one wallet inside the Polycop-FIFA list:

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?q=0xb56db5215443706244b0af76b3daaad3066ad621&limit=1
```

The returned wallet row uses flat FIFA fields only:

- `fifa_pnl_24h`: FIFA 24 hour PnL.
- `fifa_pnl_roi_24h`: FIFA 24 hour ROI.
- `fifa_traded_notional_24h`: FIFA 24 hour traded notional.
- `fifa_total_pnl`: FIFA total/current PnL.
- `fifa_win_rate`: FIFA total token-position win rate.
- `fifa_pnl_7d`: FIFA 7 day PnL.
- `fifa_win_rate_7d`: FIFA 7 day token-position win rate.

只看 Polycop 稳定型钱包，并要求 FIFA 交易额至少 1000：

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?segment=stable&limit=100&min_fifa_notional=1000
```

只看 FIFA 数据质量最高且 FIFA 盈利为正：

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100&data_quality=estimate&positive_fifa=1
```

只看 Polycop 钱包集合里最近 24 小时有 FIFA 交易的钱包：

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100&active_24h=1
```

If this response is empty while `/api/wallets/fifa-24h-pnl?active_24h=1`
has rows, it means the current Polycop smart-wallet candidate set has no
overlap with the active FIFA wallets in the last 24 hours.

### Response Shape

```json
{
  "status": "ok",
  "source": "polycop_fifa",
  "summary": {
    "polycop_wallet_count": 500,
    "candidate_wallet_count": 500,
    "fifa_wallet_count": 42,
    "active_wallets_24h": 8,
    "nonzero_pnl_wallets_24h": 8,
    "returned_wallet_count": 42
  },
  "parameters": {
    "segment": "ai_top",
    "limit": 100,
    "candidate_limit": 2000
  },
  "wallets": [
    {
      "rank": 1,
      "address": "0x...",
      "polycop_rank": 12,
      "user_name": "example",
      "ai_score": 83.2,
      "primary_segment": "stable",
      "polycop_metrics": {
        "actual_total_pnl": 50000,
        "recent20_pnl": 5000,
        "win_rate": 62
      },
      "fifa_traded_notional": 12000,
      "fifa_trade_count_24h": 8,
      "fifa_traded_notional_24h": 2500,
      "fifa_total_pnl": 2400,
      "fifa_total_pnl_roi": 0.4,
      "fifa_pnl_24h": 700,
      "fifa_pnl_roi_24h": 0.368421,
      "fifa_pnl_7d": 1600,
      "fifa_pnl_roi_7d": 0.64,
      "fifa_win_rate": 0.666667,
      "fifa_win_rate_24h": 0.5,
      "fifa_win_rate_7d": 0.714286,
      "fifa_equity_now": 2400,
      "fifa_pnl_roi": 0.4,
      "fifa_event_count": 3,
      "fifa_market_count": 5,
      "fifa_data_quality": "estimate"
    }
  ]
}
```

排序逻辑：先按 Polycop `ai_score`，再按 FIFA 交易额和 FIFA equity 排序。

### Top Level

| 字段 | 类型 | 说明 |
|---|---|---|
| `status` | string | `ok`, `stale_error`, `error`, `missing_cache`, `store_unavailable`。 |
| `source` | string | 当前固定为 `polycop`。 |
| `cache` | object | 缓存元信息。 |
| `summary` | object | 缓存摘要。 |
| `parameters` | object | 定时任务抓取和分析参数。 |
| `segment` | string | 列表接口返回，表示本次查询的分段。 |
| `total` | integer | 列表接口返回，表示筛选后总数，不是当前页条数。 |
| `limit` | integer | 列表接口返回，当前页请求条数。 |
| `offset` | integer | 列表接口返回，当前页偏移。 |
| `wallets` | array | 列表接口返回的钱包数组。 |

### Cache

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | string | 缓存来源，当前为 `postgres`。 |
| `hit` | boolean | 是否命中缓存。 |
| `cache_key` | string | 当前固定为 `latest`。 |
| `refreshed_at` | string | 缓存写入时间，UTC ISO 格式。 |
| `generated_at` | string | 本次分析生成时间，UTC ISO 格式。 |
| `age_seconds` | number | 缓存年龄，单位秒。 |
| `trigger_reason` | string | 刷新原因，例如 `scheduled`, `manual-initial`, `smoke-test`。 |
| `error` | string/null | 最近一次刷新错误。`status=stale_error` 时这里会有错误信息。 |

### Summary

| 字段 | 类型 | 说明 |
|---|---|---|
| `wallet_count` | integer | 去重后的钱包数量。 |
| `raw_wallet_count` | integer | Polycop 原始返回行数。 |
| `stable_count` | integer | `stable` 分段钱包数。 |
| `flow_count` | integer | `flow` 分段钱包数。 |
| `burst_count` | integer | `burst` 分段钱包数。 |
| `top_wallets` | array | 综合排序前 10 的精简钱包对象。 |
| `notes` | array | 数据和评分说明。 |

### Wallet

| 字段 | 类型 | 说明 |
|---|---|---|
| `rank` | integer | 综合排名，按 `ai_score` 降序。 |
| `address` | string | 钱包地址，小写。 |
| `user_name` | string | Polycop 用户名。 |
| `x_name` | string | Polycop 返回的 X/Twitter 名称。 |
| `profile_image` | string | 头像 URL。 |
| `ai_score` | number | 本系统综合评分，0-100。 |
| `source_score` | number | Polycop 原始 `score`。 |
| `segments` | array | 钱包所属分段，可能有多个。 |
| `primary_segment` | string | 主分段，优先级为 `stable`, `flow`, `burst`, `watch`。 |
| `reasons` | array | 进入分段或扣分相关原因。 |
| `metrics` | object | 原始/标准化指标。 |
| `derived` | object | 派生指标。 |

### Wallet.metrics

| 字段 | 类型 | 说明 |
|---|---|---|
| `balance` | number | Polycop 返回余额。 |
| `available` | number | Polycop 返回可用余额。 |
| `actual_total_pnl` | number | 实际总 PnL。 |
| `backtest_total_pnl` | number | 回测总 PnL。 |
| `recent20_pnl` | number | 近 20 场实际 PnL。 |
| `recent20_backtest_pnl` | number | 近 20 场回测 PnL。 |
| `win_rate` | number | 胜率，百分数口径，例如 `72.5` 表示 72.5%。 |
| `recent20_win_rate` | number | 近 20 场胜率，百分数口径。 |
| `avg_profit_loss_ratio` | number | 平均盈亏比。 |
| `avg_market_roi` | number | 平均市场 ROI。 |
| `avg_market_profit_rate` | number | 平均市场盈利率。 |
| `slippage_cost_rate` | number | 滑点成本率。 |
| `recent20_slippage_cost_rate` | number | 近 20 场滑点成本率。 |
| `total_markets` | integer | 参与市场数。 |
| `hedged_markets` | integer | 对冲市场数。 |

### Wallet.derived

| 字段 | 类型 | 说明 |
|---|---|---|
| `hedge_ratio` | number | `hedged_markets / total_markets`。 |
| `recent_pnl_share` | number | `recent20_pnl / actual_total_pnl`，用于判断盈利是否过度集中在近期。 |
| `backtest_gap_ratio` | number | `abs(actual_total_pnl - backtest_total_pnl) / max(abs(actual_total_pnl), 1)`。 |

## Error And Empty States

### Missing Cache

当缓存不存在，或设置了 `max_age_seconds` 且缓存太旧时，返回：

```json
{
  "status": "missing_cache",
  "cache": {"source": "postgres", "hit": false},
  "summary": {},
  "parameters": {},
  "wallets": [],
  "total": 0,
  "limit": 0,
  "offset": 0
}
```

### Store Unavailable

如果 API 没有配置 Postgres store，会返回 HTTP 503：

```json
{
  "status": "store_unavailable",
  "wallets": []
}
```

### Stale Error

如果最近一次定时刷新失败，但存在旧缓存，会返回旧钱包数据，并带上：

```json
{
  "status": "stale_error",
  "cache": {
    "error": "polycop_timeout"
  }
}
```

客户端可以继续展示旧数据，同时在 UI 上提示缓存刷新失败。

## Chrome 插件建议

- 插件默认调用 `summary` 获取数量、更新时间和 top wallets。
- 详情列表再调用 `/wallets/polycop-signals?segment=...`。
- 插件不要直接调用 Polycop，避免浏览器跨域、认证和限流问题。
- `age_seconds` 大于 900 秒时可以显示“数据较旧”提示。
- 钱包地址展示建议使用前 6 后 6，例如 `0x71ab...2f015`。
