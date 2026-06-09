import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  AlertTriangle,
  BarChart3,
  Bell,
  Bot,
  Database,
  Gauge,
  LineChart,
  Loader2,
  RefreshCw,
  Search,
  ShieldAlert,
  Sparkles,
  Table2,
  UserRound,
  Wallet,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import "./styles.css";

const API_BASE = import.meta.env.VITE_ZETTA_API_BASE || "/api";
const REFRESH_MS = 60_000;

type Overview = {
  events?: number;
  markets?: number;
  active_markets?: number;
  completed_markets?: number;
  outcome_tokens?: number;
  trades?: number;
  tracked_wallets?: number;
  active_wallets_24h?: number;
  volume_24h?: number;
  active_liquidity?: number;
  anomaly_signals?: number;
  high_anomaly_signals?: number;
  last_ingested_at?: string;
};

type Market = {
  market_id: string;
  event_id: string;
  condition_id: string;
  question: string;
  slug?: string;
  category?: string;
  active?: boolean;
  closed?: boolean;
  volume?: number;
  liquidity?: number;
  signal_count?: number;
  high_signal_count?: number;
  latest_trade_at?: string;
  volume_24h?: number;
  wallets_24h?: number;
  tokens?: Array<{ token_id: string; outcome: string; outcome_index: number }>;
};

type CategorySummary = {
  category: string;
  market_count: number;
  active_market_count: number;
  closed_market_count: number;
  volume: number;
  liquidity: number;
  volume_24h: number;
  active_wallets_24h: number;
  signal_count: number;
};

type Signal = {
  signal_id: string;
  signal_type: string;
  severity: string;
  event_id: string;
  market_id: string;
  condition_id: string;
  token_id: string;
  outcome: string;
  user_address: string;
  occurred_at: string;
  metric_name: string;
  metric_value: number;
  baseline_value: number;
  threshold: number;
  message: string;
  uncertainty: string;
};

type SmartActivity = {
  event_id: string;
  market_id: string;
  condition_id: string;
  token_id: string;
  outcome: string;
  user_address: string;
  position_size: number;
  traded_notional: number;
  unrealized_pnl_estimate: number;
  net_notional_24h: number;
  latest_action: string;
  last_trade_at: string;
  win_rate: number;
  realized_pnl: number;
  completed_event_count: number;
  favorite_category: string;
};

type WalletFlow = {
  user_address: string;
  trade_count: number;
  buy_count: number;
  sell_count: number;
  buy_notional: number;
  sell_notional: number;
  traded_notional: number;
  net_size: number;
  net_buy_notional: number;
  first_trade_at: string;
  last_trade_at: string;
};

type WalletProfile = {
  user_address: string;
  completed_event_count?: number;
  profitable_event_count?: number;
  win_rate?: number;
  realized_pnl?: number;
  traded_notional?: number;
  trade_count?: number;
  active_position_count?: number;
  active_unrealized_pnl_estimate?: number;
  favorite_category?: string;
  first_trade_at?: string;
  last_trade_at?: string;
};

type WalletPosition = {
  event_id: string;
  market_id: string;
  condition_id: string;
  token_id: string;
  outcome: string;
  user_address: string;
  position_size: number;
  avg_entry_price: number;
  mark_price: number;
  current_value: number;
  unrealized_pnl_estimate: number;
  traded_notional: number;
  net_notional_24h: number;
  latest_action: string;
  last_trade_at: string;
};

type View = "markets" | "signals" | "wallets" | "assistant";

function App() {
  const [view, setView] = useState<View>("markets");
  const [overview, setOverview] = useState<Overview>({});
  const [markets, setMarkets] = useState<Market[]>([]);
  const [categories, setCategories] = useState<CategorySummary[]>([]);
  const [signals, setSignals] = useState<Signal[]>([]);
  const [activity, setActivity] = useState<SmartActivity[]>([]);
  const [selectedMarket, setSelectedMarket] = useState<Market | null>(null);
  const [walletFlow, setWalletFlow] = useState<WalletFlow[]>([]);
  const [wallet, setWallet] = useState("");
  const [walletProfile, setWalletProfile] = useState<WalletProfile | null>(null);
  const [walletPositions, setWalletPositions] = useState<WalletPosition[]>([]);
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("Loading market intelligence");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    void refreshAll();
    const timer = window.setInterval(() => {
      void refreshAll({ quiet: true });
    }, REFRESH_MS);
    return () => window.clearInterval(timer);
  }, []);

  async function getJson<T>(path: string): Promise<T> {
    const response = await fetch(`${API_BASE}${path}`);
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  }

  async function refreshAll(options: { quiet?: boolean } = {}) {
    if (!options.quiet) setLoading(true);
    setError("");
    try {
      const [overviewData, marketsData, categoryData, signalData, activityData] =
        await Promise.all([
          getJson<{ overview: Overview }>("/markets/overview"),
          getJson<{ markets: Market[] }>("/markets/trending?status=active&limit=15"),
          getJson<{ categories: CategorySummary[] }>("/categories/summary?limit=8"),
          getJson<{ signals: Signal[] }>("/signals/anomalies?limit=30"),
          getJson<{ activity: SmartActivity[] }>("/wallets/smart-money/activity?limit=12"),
        ]);
      setOverview(overviewData.overview || {});
      setMarkets(marketsData.markets || []);
      setCategories(categoryData.categories || []);
      setSignals(signalData.signals || []);
      setActivity(activityData.activity || []);
      setStatus(`Synced ${formatDateTime(overviewData.overview?.last_ingested_at) || "recently"}`);
      const firstMarket = marketsData.markets?.[0];
      if (!selectedMarket && firstMarket) {
        await openMarket(firstMarket);
      }
    } catch (exc) {
      setError(errorMessage(exc));
      setStatus("Data API unavailable");
    } finally {
      setLoading(false);
    }
  }

  async function searchMarkets() {
    setLoading(true);
    setError("");
    try {
      const path = query.trim()
        ? `/markets/search?q=${encodeURIComponent(query.trim())}&limit=20`
        : "/markets/trending?status=active&limit=20";
      const data = await getJson<{ markets: Market[] }>(path);
      setMarkets(data.markets || []);
      if (data.markets?.[0]) {
        await openMarket(data.markets[0]);
      }
    } catch (exc) {
      setError(errorMessage(exc));
    } finally {
      setLoading(false);
    }
  }

  async function openMarket(market: Market) {
    setSelectedMarket(market);
    setWalletFlow([]);
    try {
      const [detail, flow] = await Promise.all([
        getJson<{ market: Market }>(`/markets/detail?market_id=${encodeURIComponent(market.market_id)}`),
        getJson<{ wallets: WalletFlow[] }>(
          `/events/wallet-flow?market_id=${encodeURIComponent(market.market_id)}&limit=12`,
        ),
      ]);
      setSelectedMarket(detail.market);
      setWalletFlow(flow.wallets || []);
    } catch (exc) {
      setError(errorMessage(exc));
    }
  }

  async function inspectWallet(address = wallet) {
    const normalized = address.trim().toLowerCase();
    if (!normalized) return;
    setWallet(normalized);
    setView("wallets");
    setLoading(true);
    setError("");
    try {
      const [profileData, positionData] = await Promise.all([
        getJson<{ profile: WalletProfile }>(`/wallets/reputation?user=${encodeURIComponent(normalized)}`),
        getJson<{ positions: WalletPosition[] }>(`/wallets/live-positions?user=${encodeURIComponent(normalized)}&limit=20`),
      ]);
      setWalletProfile(profileData.profile);
      setWalletPositions(positionData.positions || []);
    } catch {
      setWalletProfile({ user_address: normalized });
      setWalletPositions([]);
    } finally {
      setLoading(false);
    }
  }

  const maxCategoryVolume = useMemo(
    () => Math.max(...categories.map((item) => Number(item.volume_24h || item.volume || 0)), 1),
    [categories],
  );
  const signalRows = useMemo(() => summarizeSignals(signals), [signals]);

  return (
    <div className="productShell">
      <aside className="rail">
        <div className="brandMark">Z</div>
        <nav className="railNav" aria-label="Primary">
          <IconNav active={view === "markets"} label="Markets" icon={<Table2 size={20} />} onClick={() => setView("markets")} />
          <IconNav active={view === "signals"} label="Signals" icon={<ShieldAlert size={20} />} onClick={() => setView("signals")} />
          <IconNav active={view === "wallets"} label="Wallets" icon={<Wallet size={20} />} onClick={() => setView("wallets")} />
          <IconNav active={view === "assistant"} label="Assistant" icon={<Bot size={20} />} onClick={() => setView("assistant")} />
        </nav>
      </aside>

      <main className="workspace">
        <header className="productTopbar">
          <div className="titleBlock">
            <h1>Polymarket Intelligence</h1>
            <p>Market-wide event, wallet, flow, and anomaly analytics from public Zetta data.</p>
          </div>
          <div className="searchBox">
            <Search size={18} />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={(event) => event.key === "Enter" && searchMarkets()}
              placeholder="Search title, slug, condition id, or category"
            />
          </div>
          <div className="syncStatus">
            <div>
              <strong>{status}</strong>
              <span>{API_BASE}</span>
            </div>
            <button className="iconButton" onClick={() => refreshAll()} title="Refresh">
              {loading ? <Loader2 className="spin" size={18} /> : <RefreshCw size={18} />}
            </button>
          </div>
        </header>

        {error ? (
          <div className="notice">
            <AlertTriangle size={16} />
            {error}
          </div>
        ) : null}

        <section className="tabs" aria-label="Views">
          <button className={view === "markets" ? "tab active" : "tab"} onClick={() => setView("markets")}>All Markets</button>
          <button className={view === "signals" ? "tab active" : "tab"} onClick={() => setView("signals")}>Anomalies</button>
          <button className={view === "wallets" ? "tab active" : "tab"} onClick={() => setView("wallets")}>Wallets</button>
          <button className={view === "assistant" ? "tab active" : "tab"} onClick={() => setView("assistant")}>Data Assistant</button>
        </section>

        <Kpis overview={overview} />

        {view === "markets" ? (
          <MarketWorkspace
            categories={categories}
            maxCategoryVolume={maxCategoryVolume}
            markets={markets}
            selectedMarket={selectedMarket}
            walletFlow={walletFlow}
            signals={signals}
            activity={activity}
            signalRows={signalRows}
            onSearch={searchMarkets}
            onOpenMarket={openMarket}
            onInspectWallet={inspectWallet}
          />
        ) : null}

        {view === "signals" ? <SignalsView signals={signals} signalRows={signalRows} /> : null}

        {view === "wallets" ? (
          <WalletsView
            wallet={wallet}
            setWallet={setWallet}
            profile={walletProfile}
            positions={walletPositions}
            activity={activity}
            onInspectWallet={inspectWallet}
          />
        ) : null}

        {view === "assistant" ? <AssistantView /> : null}
      </main>
    </div>
  );
}

function Kpis({ overview }: { overview: Overview }) {
  const cards = [
    ["Active Markets", overview.active_markets, `${formatNumber(overview.markets)} total`],
    ["Completed Markets", overview.completed_markets, `${formatNumber(overview.outcome_tokens)} tokens`],
    ["24h Volume", overview.volume_24h, `${formatNumber(overview.trades)} trades`, "currency"],
    ["Active Liquidity", overview.active_liquidity, "open markets", "currency"],
    ["Tracked Wallets", overview.tracked_wallets, `${formatNumber(overview.active_wallets_24h)} active 24h`],
    ["Open Signals", overview.anomaly_signals, `${formatNumber(overview.high_anomaly_signals)} high severity`],
  ];
  return (
    <section className="kpis" aria-label="Market KPIs">
      {cards.map(([label, value, detail, format]) => (
        <div className="kpi" key={String(label)}>
          <label>{label}</label>
          <strong>{format === "currency" ? formatCompactCurrency(value) : formatCompact(value)}</strong>
          <small>{detail}</small>
        </div>
      ))}
    </section>
  );
}

function MarketWorkspace({
  categories,
  maxCategoryVolume,
  markets,
  selectedMarket,
  walletFlow,
  signals,
  activity,
  signalRows,
  onSearch,
  onOpenMarket,
  onInspectWallet,
}: {
  categories: CategorySummary[];
  maxCategoryVolume: number;
  markets: Market[];
  selectedMarket: Market | null;
  walletFlow: WalletFlow[];
  signals: Signal[];
  activity: SmartActivity[];
  signalRows: Array<{ type: string; count: number; high: number }>;
  onSearch: () => void;
  onOpenMarket: (market: Market) => void;
  onInspectWallet: (wallet: string) => void;
}) {
  return (
    <section className="intelligenceGrid">
      <article className="panel marketMap">
        <PanelHeader title="Market Explorer" meta="category flow" icon={<BarChart3 size={17} />} />
        <div className="filterStrip">
          {["All", "Active", "Completed", "High liquidity", "Risk signals"].map((label, index) => (
            <button className={index === 0 ? "chip active" : "chip"} key={label} onClick={onSearch}>{label}</button>
          ))}
        </div>
        <div className="flowList">
          {categories.map((item, index) => (
            <div className="flowRow" key={item.category}>
              <b>{item.category || "Uncategorized"}</b>
              <div className="barTrack">
                <i style={{ width: `${Math.max(5, ((item.volume_24h || item.volume) / maxCategoryVolume) * 100)}%`, background: palette[index % palette.length] }} />
              </div>
              <span>{formatCompactCurrency(item.volume_24h || item.volume)}</span>
            </div>
          ))}
        </div>
        <div className="legendGrid">
          <MiniStat label="active wallets 24h" value={formatCompact(sum(categories, "active_wallets_24h"))} />
          <MiniStat label="category signals" value={formatCompact(sum(categories, "signal_count"))} />
          <MiniStat label="active markets" value={formatCompact(sum(categories, "active_market_count"))} />
          <MiniStat label="liquidity" value={formatCompactCurrency(sum(categories, "liquidity"))} />
        </div>
      </article>

      <article className="panel trendPanel">
        <PanelHeader title="Trending Markets" meta="ranked by flow and signals" icon={<LineChart size={17} />} />
        <table>
          <thead>
            <tr>
              <th>Market</th>
              <th>Status</th>
              <th>24h Vol</th>
              <th>Wallets</th>
              <th>Signals</th>
            </tr>
          </thead>
          <tbody>
            {markets.map((market) => (
              <tr key={market.market_id} onClick={() => onOpenMarket(market)} className="clickRow">
                <td>
                  <div className="marketTitle">{market.question}</div>
                  <div className="sub">{market.category || "Uncategorized"} / {shortId(market.condition_id)}</div>
                </td>
                <td><span className={market.closed ? "tag" : "tag green"}>{market.closed ? "closed" : "active"}</span></td>
                <td className="num">{formatCompactCurrency(market.volume_24h || market.volume)}</td>
                <td className="num">{formatCompact(market.wallets_24h)}</td>
                <td><span className={Number(market.high_signal_count) > 0 ? "tag red" : "tag blue"}>{formatNumber(market.signal_count)}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>

      <MarketDetail market={selectedMarket} walletFlow={walletFlow} onInspectWallet={onInspectWallet} />
      <SmartMoneyFeed activity={activity} onInspectWallet={onInspectWallet} />
      <SignalPanel signals={signals} signalRows={signalRows} />
    </section>
  );
}

function MarketDetail({
  market,
  walletFlow,
  onInspectWallet,
}: {
  market: Market | null;
  walletFlow: WalletFlow[];
  onInspectWallet: (wallet: string) => void;
}) {
  return (
    <article className="panel detailPanel">
      <PanelHeader title="Selected Market" meta="wallet flow" icon={<Gauge size={17} />} />
      {market ? (
        <>
          <div className="detailHead">
            <h2>{market.question}</h2>
            <div className="pillRow">
              <span>{market.closed ? "Closed" : "Open"}</span>
              <span>{market.category || "Uncategorized"}</span>
              <span>{formatCompactCurrency(market.volume)} volume</span>
              <span>{formatCompactCurrency(market.liquidity)} liquidity</span>
            </div>
          </div>
          <div className="tokenGrid">
            {(market.tokens || []).slice(0, 6).map((token) => (
              <div className="token" key={token.token_id}>
                <span>{token.outcome || `Outcome ${token.outcome_index}`}</span>
                <code>{shortId(token.token_id)}</code>
              </div>
            ))}
          </div>
          <h3>Top Wallet Flow</h3>
          <table>
            <thead>
              <tr><th>Wallet</th><th>Trades</th><th>Buy</th><th>Sell</th><th>Net</th></tr>
            </thead>
            <tbody>
              {walletFlow.map((row) => (
                <tr key={row.user_address} className="clickRow" onClick={() => onInspectWallet(row.user_address)}>
                  <td><code>{shortId(row.user_address)}</code></td>
                  <td className="num">{formatNumber(row.trade_count)}</td>
                  <td className="num">{formatCompactCurrency(row.buy_notional)}</td>
                  <td className="num">{formatCompactCurrency(row.sell_notional)}</td>
                  <td className={row.net_buy_notional >= 0 ? "num up" : "num down"}>{formatCompactCurrency(row.net_buy_notional)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : (
        <div className="empty">Select a market to inspect event wallet flow.</div>
      )}
    </article>
  );
}

function SmartMoneyFeed({
  activity,
  onInspectWallet,
}: {
  activity: SmartActivity[];
  onInspectWallet: (wallet: string) => void;
}) {
  return (
    <article className="panel">
      <PanelHeader title="Smart Money Activity" meta="active positions" icon={<UserRound size={17} />} />
      <div className="feed">
        {activity.length ? activity.map((item) => (
          <button className="feedItem" key={`${item.user_address}-${item.token_id}`} onClick={() => onInspectWallet(item.user_address)}>
            <div className="feedTop">
              <span className="walletText">{shortId(item.user_address)}</span>
              <span className="tag green">{formatPercent(Number(item.win_rate) * 100)} win</span>
            </div>
            <p>
              {item.latest_action || "TRADE"} {formatCompactCurrency(item.net_notional_24h)} 24h net on {item.outcome || shortId(item.token_id)}.
            </p>
          </button>
        )) : (
          <div className="empty">Live position and reputation marts are ready to populate this stream.</div>
        )}
      </div>
    </article>
  );
}

function SignalPanel({ signals, signalRows }: { signals: Signal[]; signalRows: Array<{ type: string; count: number; high: number }> }) {
  return (
    <article className="panel">
      <PanelHeader title="Anomaly Signals" meta="evidence-first" icon={<ShieldAlert size={17} />} />
      <div className="riskList">
        {signalRows.map((row, index) => (
          <div className="riskLine" key={row.type}>
            <div>
              <b>{labelize(row.type)}</b>
              <div className="meter"><i style={{ width: `${Math.min(100, row.count / Math.max(signalRows[0]?.count || 1, 1) * 100)}%`, background: palette[(index + 2) % palette.length] }} /></div>
            </div>
            <div className={row.high > 0 ? "score down" : "score"}>{formatNumber(row.count)}</div>
          </div>
        ))}
      </div>
      <div className="miniFeed">
        {signals.slice(0, 4).map((signal) => (
          <div key={signal.signal_id}>
            <strong>{labelize(signal.signal_type)}</strong>
            <span>{signal.severity} / {formatDateTime(signal.occurred_at)}</span>
          </div>
        ))}
      </div>
    </article>
  );
}

function SignalsView({ signals, signalRows }: { signals: Signal[]; signalRows: Array<{ type: string; count: number; high: number }> }) {
  return (
    <section className="pageGrid">
      <article className="panel">
        <PanelHeader title="Signal Mix" meta="latest mart output" icon={<Bell size={17} />} />
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={signalRows}>
            <CartesianGrid strokeDasharray="3 3" stroke="#dde2e3" />
            <XAxis dataKey="type" tickFormatter={labelize} tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar dataKey="count" fill="#26637a" />
            <Bar dataKey="high" fill="#b34a3e" />
          </BarChart>
        </ResponsiveContainer>
      </article>
      <article className="panel wide">
        <PanelHeader title="Signal Feed" meta="risk signal, not allegation" icon={<ShieldAlert size={17} />} />
        <table>
          <thead>
            <tr><th>Signal</th><th>Severity</th><th>Wallet</th><th>Market</th><th>Metric</th><th>Time</th></tr>
          </thead>
          <tbody>
            {signals.map((signal) => (
              <tr key={signal.signal_id}>
                <td>
                  <div className="marketTitle">{labelize(signal.signal_type)}</div>
                  <div className="sub">{signal.message}</div>
                </td>
                <td><span className={signal.severity === "high" ? "tag red" : "tag amber"}>{signal.severity}</span></td>
                <td><code>{shortId(signal.user_address)}</code></td>
                <td><code>{shortId(signal.market_id || signal.event_id)}</code></td>
                <td className="num">{formatNumber(signal.metric_value)}</td>
                <td>{formatDateTime(signal.occurred_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>
    </section>
  );
}

function WalletsView({
  wallet,
  setWallet,
  profile,
  positions,
  activity,
  onInspectWallet,
}: {
  wallet: string;
  setWallet: (value: string) => void;
  profile: WalletProfile | null;
  positions: WalletPosition[];
  activity: SmartActivity[];
  onInspectWallet: (wallet: string) => void;
}) {
  const cards = [
    ["Completed Events", profile?.completed_event_count],
    ["Win Rate", Number(profile?.win_rate || 0) * 100, "percent"],
    ["Realized PnL", profile?.realized_pnl, "currency"],
    ["Traded Notional", profile?.traded_notional, "currency"],
    ["Active Positions", profile?.active_position_count],
    ["Active Est. PnL", profile?.active_unrealized_pnl_estimate, "currency"],
  ];
  return (
    <section className="pageGrid">
      <article className="panel wide">
        <PanelHeader title="Wallet Intelligence" meta="public wallet profile" icon={<Wallet size={17} />} />
        <div className="walletSearch">
          <input
            value={wallet}
            onChange={(event) => setWallet(event.target.value)}
            onKeyDown={(event) => event.key === "Enter" && onInspectWallet(wallet)}
            placeholder="0x wallet address"
          />
          <button onClick={() => onInspectWallet(wallet)}>Inspect</button>
        </div>
        {profile ? (
          <>
            <div className="metrics">
              {cards.map(([label, value, kind]) => (
                <div className="metric" key={String(label)}>
                  <span>{label}</span>
                  <strong>{kind === "currency" ? formatCompactCurrency(value) : kind === "percent" ? formatPercent(value) : formatCompact(value)}</strong>
                </div>
              ))}
            </div>
            <div className="profileLine">
              <span>Favorite category: <b>{profile.favorite_category || "Not available"}</b></span>
              <span>First seen: <b>{formatDateTime(profile.first_trade_at)}</b></span>
              <span>Last trade: <b>{formatDateTime(profile.last_trade_at)}</b></span>
            </div>
          </>
        ) : (
          <div className="empty">Enter a wallet or select one from smart-money activity.</div>
        )}
      </article>

      <article className="panel wide">
        <PanelHeader title="Live Positions" meta="mark-to-market estimate" icon={<Gauge size={17} />} />
        <table>
          <thead>
            <tr><th>Outcome</th><th>Position</th><th>Entry</th><th>Mark</th><th>Value</th><th>Est. PnL</th><th>Action</th></tr>
          </thead>
          <tbody>
            {positions.map((item) => (
              <tr key={`${item.user_address}-${item.token_id}`}>
                <td>{item.outcome || shortId(item.token_id)}</td>
                <td className="num">{formatNumber(item.position_size)}</td>
                <td className="num">{formatNumber(item.avg_entry_price)}</td>
                <td className="num">{formatNumber(item.mark_price)}</td>
                <td className="num">{formatCompactCurrency(item.current_value)}</td>
                <td className={Number(item.unrealized_pnl_estimate) >= 0 ? "num up" : "num down"}>{formatCompactCurrency(item.unrealized_pnl_estimate)}</td>
                <td>{item.latest_action}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>

      <article className="panel wide">
        <PanelHeader title="Candidate Wallets" meta="from live position mart" icon={<UserRound size={17} />} />
        <table>
          <thead>
            <tr><th>Wallet</th><th>Win Rate</th><th>Realized PnL</th><th>24h Net</th><th>Action</th></tr>
          </thead>
          <tbody>
            {activity.map((item) => (
              <tr key={`${item.user_address}-${item.token_id}`} className="clickRow" onClick={() => onInspectWallet(item.user_address)}>
                <td><code>{shortId(item.user_address)}</code></td>
                <td className="num">{formatPercent(Number(item.win_rate) * 100)}</td>
                <td className="num">{formatCompactCurrency(item.realized_pnl)}</td>
                <td className="num">{formatCompactCurrency(item.net_notional_24h)}</td>
                <td>{item.latest_action}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>
    </section>
  );
}

function AssistantView() {
  return (
    <section className="assistantPanel panel">
      <PanelHeader title="SQL Agent / Data Assistant" meta="read-only marts" icon={<Sparkles size={17} />} />
      <div className="assistantBody">
        <div className="askBox">
          <div className="queryBox">
            Find wallets with positive realized PnL in completed markets and new active positions in the same category during the last 24 hours.
          </div>
          <div className="suggestions">
            <button>Top profitable wallets by category over 90 days</button>
            <button>Markets with largest liquidity withdrawal today</button>
            <button>Wallets entering before a 20% price move</button>
            <button>Completed markets with settlement metadata gaps</button>
          </div>
        </div>
        <pre className="sqlPreview">{`SELECT
  user_address,
  favorite_category,
  realized_pnl,
  active_unrealized_pnl_estimate,
  last_trade_at
FROM mart_wallet_reputation
WHERE realized_pnl > 0
ORDER BY realized_pnl DESC
LIMIT 100;`}</pre>
      </div>
    </section>
  );
}

function PanelHeader({ title, meta, icon }: { title: string; meta: string; icon: React.ReactNode }) {
  return (
    <div className="panelHeader">
      <h2>{icon}{title}</h2>
      <span>{meta}</span>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <b>{value}</b>
      <span>{label}</span>
    </div>
  );
}

function IconNav({ active, icon, label, onClick }: { active: boolean; icon: React.ReactNode; label: string; onClick: () => void }) {
  return (
    <button className={active ? "railButton active" : "railButton"} onClick={onClick} title={label}>
      {icon}
    </button>
  );
}

const palette = ["#2a7d6f", "#26637a", "#9d742c", "#5b6f48", "#6b5f86", "#b34a3e"];

function summarizeSignals(signals: Signal[]) {
  const grouped = new Map<string, { type: string; count: number; high: number }>();
  for (const signal of signals) {
    const row = grouped.get(signal.signal_type) || { type: signal.signal_type, count: 0, high: 0 };
    row.count += 1;
    if (signal.severity === "high") row.high += 1;
    grouped.set(signal.signal_type, row);
  }
  return [...grouped.values()].sort((a, b) => b.count - a.count);
}

function sum<T extends Record<string, unknown>>(rows: T[], key: keyof T) {
  return rows.reduce((total, row) => total + Number(row[key] || 0), 0);
}

function formatCompact(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(number);
}

function formatNumber(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(number);
}

function formatCompactCurrency(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(number);
}

function formatPercent(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "0%";
  return `${formatNumber(number)}%`;
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
}

function shortId(value: string | null | undefined) {
  if (!value) return "";
  if (value.length <= 14) return value;
  return `${value.slice(0, 6)}...${value.slice(-6)}`;
}

function labelize(value: string) {
  return (value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function errorMessage(exc: unknown) {
  return exc instanceof Error ? exc.message : String(exc);
}

createRoot(document.getElementById("root")!).render(<App />);
