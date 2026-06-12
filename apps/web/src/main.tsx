import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Cpu,
  Database,
  HardDrive,
  LineChart,
  Loader2,
  MemoryStick,
  Copy,
  ExternalLink,
  Search,
  Server,
  Table2,
  UserRound,
  WalletCards,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import "./styles.css";

const API_BASE = import.meta.env.VITE_ZETTA_API_BASE || "/api";
const DASHBOARD_REFRESH_MS = 30_000;

type Overview = {
  events?: number;
  markets?: number;
  outcome_tokens?: number;
  trades?: number;
  price_points?: number;
  orderbook_snapshots?: number;
  chain_logs?: number;
  last_ingested_at?: string;
  latest_data_at?: string;
  latest_trade_at?: string;
};

type IngestionRow = {
  source: string;
  entity: string;
  raw_batches: number;
  items: number;
  last_collected_at: string;
};

type SystemStats = {
  collected_at?: string;
  uptime_seconds?: number | null;
  cpu?: {
    percent?: number | null;
    count?: number | null;
    load_avg_1m?: number | null;
    load_avg_5m?: number | null;
    load_avg_15m?: number | null;
    load_per_cpu_percent?: number | null;
  };
  memory?: {
    total_bytes?: number;
    used_bytes?: number;
    available_bytes?: number;
    percent?: number;
  };
  disk?: {
    path?: string;
    total_bytes?: number;
    used_bytes?: number;
    free_bytes?: number;
    percent?: number;
  };
};

type Market = {
  market_id: string;
  event_id: string;
  condition_id: string;
  question: string;
  slug: string;
  active: boolean;
  closed: boolean;
  volume: number;
  liquidity: number;
  tokens?: Array<{ token_id: string; outcome: string; outcome_index: number }>;
};

type Trade = {
  trade_id: string;
  timestamp: string;
  token_id: string;
  user_address: string;
  side: string;
  price: number;
  size: number;
  notional: number;
};

type TraderProfile = Record<string, string | number | null>;

type WalletSummary = {
  total_wallets?: number;
  wallets_over_10k?: number;
  smart_wallets?: number;
  whale_wallets?: number;
  pnl_covered_wallets?: number;
  over_10k_with_pnl?: number;
  over_10k_without_pnl?: number;
  updated_at?: string;
};

type WalletRow = {
  user_address: string;
  trade_count: number;
  traded_notional: number;
  max_single_trade_notional?: number;
  position_count?: number;
  positions_value?: number;
  portfolio_value?: number;
  available_balance?: number;
  total_pnl?: number;
  portfolio_captured_at?: string | null;
  pnl_captured_at?: string | null;
  pnl_roi?: number;
  is_whale?: boolean | number;
  is_smart?: boolean | number;
  whale_reason?: string;
  traded_notional_24h: number;
  trade_count_24h: number;
  net_notional_24h: number;
  latest_action: string;
  whale_tier: string;
  data_lag_seconds: number;
  last_trade_at: string;
  realized_pnl: number;
  win_rate: number;
  updated_at?: string;
};

type Progress = {
  summary: Record<string, number>;
  total_tasks: number;
  done_percent: number;
  closed_percent: number;
  by_kind: Record<
    string,
    {
      pending: number;
      running: number;
      done: number;
      failed: number;
      dead_lettered: number;
      total: number;
      done_percent: number;
    }
  >;
  recent_runs: Array<{
    task_id: string;
    kind: string;
    node_id: string;
    status: string;
    pages: number;
    items: number;
    duration_seconds: number;
    finished_at: string | null;
    error: string | null;
  }>;
};

type View = "dashboard" | "wallets" | "hardware" | "markets" | "traders" | "operations";

function App() {
  const [view, setView] = useState<View>("dashboard");
  const [overview, setOverview] = useState<Overview>({});
  const [ingestion, setIngestion] = useState<IngestionRow[]>([]);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [system, setSystem] = useState<SystemStats | null>(null);
  const [markets, setMarkets] = useState<Market[]>([]);
  const [selectedMarket, setSelectedMarket] = useState<Market | null>(null);
  const [marketTrades, setMarketTrades] = useState<Trade[]>([]);
  const [query, setQuery] = useState("election");
  const [wallet, setWallet] = useState("");
  const [profile, setProfile] = useState<TraderProfile | null>(null);
  const [whaleWallets, setWhaleWallets] = useState<WalletRow[]>([]);
  const [walletSummary, setWalletSummary] = useState<WalletSummary>({});
  const [smartWallets, setSmartWallets] = useState<WalletRow[]>([]);
  const [listedWhaleWallets, setListedWhaleWallets] = useState<WalletRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    void refreshDashboard();
    void refreshWallets();
    void searchMarkets("election");
    const timer = window.setInterval(() => {
      void refreshDashboard();
      void refreshWallets();
    }, DASHBOARD_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, []);

  async function getJson<T>(path: string): Promise<T> {
    const response = await fetch(`${API_BASE}${path}`);
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  }

  async function refreshDashboard() {
    setError("");
    try {
      const [overviewData, ingestionData, progressData, systemData] = await Promise.all([
        getJson<{ overview: Overview }>("/stats/overview"),
        getJson<{ ingestion: IngestionRow[] }>("/stats/ingestion"),
        getJson<Progress>("/tasks/progress?recent_limit=8"),
        getJson<{ system: SystemStats }>("/stats/system"),
      ]);
      const whaleData = await getJson<{ wallets: WalletRow[] }>("/wallets/screener?mode=whale&limit=8");
      setOverview(overviewData.overview || {});
      setIngestion(ingestionData.ingestion || []);
      setProgress(progressData);
      setSystem(systemData.system || null);
      setWhaleWallets(whaleData.wallets || []);
    } catch (exc) {
      setError(errorMessage(exc));
    }
  }

  async function refreshWallets() {
    setError("");
    try {
      const [summaryData, smartData, whaleData] = await Promise.all([
        getJson<{ summary: WalletSummary }>("/wallets/summary"),
        getJson<{ wallets: WalletRow[] }>("/wallets/screener?mode=smart&limit=100"),
        getJson<{ wallets: WalletRow[] }>("/wallets/screener?mode=whale&limit=100"),
      ]);
      setWalletSummary(summaryData.summary || {});
      setSmartWallets(smartData.wallets || []);
      setListedWhaleWallets(whaleData.wallets || []);
    } catch (exc) {
      setError(errorMessage(exc));
    }
  }

  function refreshCurrentView() {
    if (view === "wallets") {
      void refreshWallets();
      return;
    }
    void refreshDashboard();
  }

  async function searchMarkets(nextQuery = query) {
    setLoading(true);
    setError("");
    try {
      const data = await getJson<{ markets: Market[] }>(
        `/markets/search?q=${encodeURIComponent(nextQuery)}&limit=25`,
      );
      setMarkets(data.markets || []);
      if (data.markets?.[0]) {
        await openMarket(data.markets[0]);
      }
    } catch (exc) {
      setError(String(exc));
    } finally {
      setLoading(false);
    }
  }

  async function openMarket(market: Market) {
    setSelectedMarket(market);
    setMarketTrades([]);
    setError("");
    try {
      const [detail, trades] = await Promise.all([
        getJson<{ market: Market }>(`/markets/detail?market_id=${encodeURIComponent(market.market_id)}`),
        getJson<{ trades: Trade[] }>(
          `/markets/trades?condition_id=${encodeURIComponent(market.condition_id)}&limit=20`,
        ),
      ]);
      setSelectedMarket(detail.market);
      setMarketTrades(trades.trades || []);
    } catch (exc) {
      setError(errorMessage(exc));
    }
  }

  async function loadTrader() {
    if (!wallet.trim()) return;
    setLoading(true);
    setError("");
    setProfile(null);
    try {
      const data = await getJson<{ profile: TraderProfile }>(
        `/traders/profile?user=${encodeURIComponent(wallet.trim())}`,
      );
      setProfile(data.profile);
    } catch (exc) {
      setError("Trader profile not found yet.");
    } finally {
      setLoading(false);
    }
  }

  const progressRows = useMemo(() => {
    if (!progress) return [];
    return Object.entries(progress.by_kind).map(([kind, counts]) => ({ kind, ...counts }));
  }, [progress]);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <Database size={22} />
          <span>Zetta</span>
        </div>
        <nav>
          <NavButton active={view === "dashboard"} icon={<BarChart3 size={18} />} label="Dashboard" onClick={() => setView("dashboard")} />
          <NavButton active={view === "wallets"} icon={<WalletCards size={18} />} label="Wallets" onClick={() => setView("wallets")} />
          <NavButton active={view === "hardware"} icon={<Cpu size={18} />} label="Hardware" onClick={() => setView("hardware")} />
          <NavButton active={view === "markets"} icon={<Table2 size={18} />} label="Markets" onClick={() => setView("markets")} />
          <NavButton active={view === "traders"} icon={<UserRound size={18} />} label="Traders" onClick={() => setView("traders")} />
          <NavButton active={view === "operations"} icon={<Server size={18} />} label="Operations" onClick={() => setView("operations")} />
        </nav>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <h1>{viewTitle(view)}</h1>
            <p>{API_BASE}</p>
          </div>
          <button className="iconButton" onClick={refreshCurrentView} title="Refresh">
            <Activity size={18} />
          </button>
        </header>

        {error ? <div className="notice"><AlertTriangle size={16} />{error}</div> : null}

        {view === "dashboard" ? (
          <Dashboard overview={overview} ingestion={ingestion} progress={progress} progressRows={progressRows} system={system} whaleWallets={whaleWallets} />
        ) : null}

        {view === "wallets" ? (
          <Wallets summary={walletSummary} smartWallets={smartWallets} whaleWallets={listedWhaleWallets} />
        ) : null}

        {view === "hardware" ? <Hardware system={system} /> : null}

        {view === "markets" ? (
          <Markets
            query={query}
            setQuery={setQuery}
            loading={loading}
            markets={markets}
            selectedMarket={selectedMarket}
            trades={marketTrades}
            onSearch={() => searchMarkets()}
            onOpenMarket={openMarket}
          />
        ) : null}

        {view === "traders" ? (
          <Traders wallet={wallet} setWallet={setWallet} profile={profile} loading={loading} onLoad={loadTrader} />
        ) : null}

        {view === "operations" ? <Operations progress={progress} progressRows={progressRows} ingestion={ingestion} system={system} /> : null}
      </main>
    </div>
  );
}

function Hardware({ system }: { system: SystemStats | null }) {
  return (
    <section className="gridPage">
      <SystemPressure system={system} wide />

      <div className="panel">
        <PanelTitle icon={<Cpu size={18} />} title="CPU" />
        <table>
          <tbody>
            <tr><td>Usage</td><td className="num">{formatPercent(system?.cpu?.percent)}</td></tr>
            <tr><td>Cores</td><td className="num">{formatMetric(system?.cpu?.count)}</td></tr>
            <tr><td>Load 1m</td><td className="num">{formatMetric(system?.cpu?.load_avg_1m)}</td></tr>
            <tr><td>Load 5m</td><td className="num">{formatMetric(system?.cpu?.load_avg_5m)}</td></tr>
            <tr><td>Load 15m</td><td className="num">{formatMetric(system?.cpu?.load_avg_15m)}</td></tr>
          </tbody>
        </table>
      </div>

      <div className="panel">
        <PanelTitle icon={<MemoryStick size={18} />} title="Memory" />
        <table>
          <tbody>
            <tr><td>Usage</td><td className="num">{formatPercent(system?.memory?.percent)}</td></tr>
            <tr><td>Used</td><td className="num">{formatBytes(system?.memory?.used_bytes)}</td></tr>
            <tr><td>Available</td><td className="num">{formatBytes(system?.memory?.available_bytes)}</td></tr>
            <tr><td>Total</td><td className="num">{formatBytes(system?.memory?.total_bytes)}</td></tr>
          </tbody>
        </table>
      </div>

      <div className="panel wide">
        <PanelTitle icon={<HardDrive size={18} />} title="Disk" />
        <table>
          <tbody>
            <tr><td>Path</td><td className="num">{system?.disk?.path || "/"}</td></tr>
            <tr><td>Usage</td><td className="num">{formatPercent(system?.disk?.percent)}</td></tr>
            <tr><td>Used</td><td className="num">{formatBytes(system?.disk?.used_bytes)}</td></tr>
            <tr><td>Free</td><td className="num">{formatBytes(system?.disk?.free_bytes)}</td></tr>
            <tr><td>Total</td><td className="num">{formatBytes(system?.disk?.total_bytes)}</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Dashboard({
  overview,
  ingestion,
  progress,
  progressRows,
  system,
  whaleWallets,
}: {
  overview: Overview;
  ingestion: IngestionRow[];
  progress: Progress | null;
  progressRows: Array<{ kind: string; total: number; done: number; running: number; dead_lettered: number }>;
  system: SystemStats | null;
  whaleWallets: WalletRow[];
}) {
  const closedPercent = boundedPercent(progress?.closed_percent);
  const donePercent = boundedPercent(progress?.done_percent);
  const deadLettered = progress?.summary?.dead_lettered ?? 0;
  const latestDataAt = overview.latest_data_at || overview.last_ingested_at;
  const statCards = [
    ["Events", overview.events],
    ["Markets", overview.markets],
    ["Outcome Tokens", overview.outcome_tokens],
    ["Trades", overview.trades],
    ["Price Points", overview.price_points],
    ["Chain Logs", overview.chain_logs],
  ];
  return (
    <section className="gridPage">
      <div className="metrics">
        {statCards.map(([label, value]) => (
          <div className="metric" key={label}>
            <span>{label}</span>
            <strong>{formatNumber(value)}</strong>
          </div>
        ))}
      </div>

      <SystemPressure system={system} />

      <div className="panel wide">
        <PanelTitle icon={<WalletCards size={18} />} title="Whale Wallets" />
        <table>
          <thead>
            <tr><th>Wallet</th><th>Tier</th><th>Total Volume</th><th>24h Volume</th><th>24h Net</th><th>Trades</th><th>Last Trade</th></tr>
          </thead>
          <tbody>
            {whaleWallets.map((wallet) => (
              <tr key={wallet.user_address}>
                <td><WalletIdentity wallet={wallet.user_address} /></td>
                <td>{wallet.whale_tier}</td>
                <td className="num">{formatCurrency(wallet.traded_notional)}</td>
                <td className="num">{formatCurrency(wallet.traded_notional_24h)}</td>
                <td className="num">{formatCurrency(wallet.net_notional_24h)}</td>
                <td className="num">{formatNumber(wallet.trade_count)}</td>
                <td>{formatDate(wallet.last_trade_at) || "--"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel wide">
        <PanelTitle icon={<LineChart size={18} />} title="Backfill Progress" />
        <div className="progressLine">
          <div>
            <strong>{formatPercent(closedPercent)}</strong>
            <span>complete</span>
          </div>
          <div className="progressTrack">
            <div style={{ width: `${closedPercent}%` }} />
          </div>
          <div>
            <strong>{formatNumber(progress?.total_tasks)}</strong>
            <span>tasks</span>
          </div>
        </div>
        <div className="progressMeta">
          <span>Done {formatPercent(donePercent)}</span>
          <span>Dead-lettered {formatNumber(deadLettered)}</span>
          <span>Latest data {formatDate(latestDataAt) || "--"}</span>
          <span>Latest trade {formatDate(overview.latest_trade_at) || "--"}</span>
        </div>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={progressRows}>
            <CartesianGrid strokeDasharray="3 3" stroke="#d7dde4" />
            <XAxis dataKey="kind" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip />
            <Bar dataKey="done" stackId="a" fill="#227c9d" />
            <Bar dataKey="running" stackId="a" fill="#f6ae2d" />
            <Bar dataKey="dead_lettered" stackId="a" fill="#d1495b" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="panel">
        <PanelTitle icon={<Database size={18} />} title="Ingestion Batches" />
        <table>
          <tbody>
            {ingestion.slice(0, 8).map((row) => (
              <tr key={`${row.source}-${row.entity}`}>
                <td>{row.source}.{row.entity}</td>
                <td className="num">{formatNumber(row.raw_batches)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel">
        <PanelTitle icon={<Activity size={18} />} title="Task State" />
        <ResponsiveContainer width="100%" height={240}>
          <PieChart>
            <Pie
              data={Object.entries(progress?.summary || {}).map(([name, value]) => ({ name, value }))}
              dataKey="value"
              nameKey="name"
              innerRadius={58}
              outerRadius={86}
              paddingAngle={2}
            >
              {["#227c9d", "#f6ae2d", "#2a9d8f", "#e76f51", "#d1495b"].map((color) => (
                <Cell key={color} fill={color} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function Wallets({
  summary,
  smartWallets,
  whaleWallets,
}: {
  summary: WalletSummary;
  smartWallets: WalletRow[];
  whaleWallets: WalletRow[];
}) {
  const coveragePercent = ratioPercent(summary.over_10k_with_pnl, summary.wallets_over_10k);
  const cards = [
    ["All Wallets", summary.total_wallets],
    ["> $10K Volume", summary.wallets_over_10k],
    ["Smart Wallets", summary.smart_wallets],
    ["Whale Wallets", summary.whale_wallets],
  ];
  return (
    <section className="gridPage walletPage">
      <div className="metrics walletMetrics wide">
        {cards.map(([label, value]) => (
          <div className="metric" key={label}>
            <span>{label}</span>
            <strong>{formatNumber(value)}</strong>
          </div>
        ))}
      </div>

      <div className="walletStatus wide">
        <span>Updated {formatDate(summary.updated_at) || "--"}</span>
        <span>
          PnL snapshots over $10K {formatNumber(summary.over_10k_with_pnl)} / {formatNumber(summary.wallets_over_10k)}
          {" "}({formatPercent(coveragePercent)})
        </span>
      </div>

      <div className="walletListGrid wide">
        <WalletListPanel title="Smart Wallets" kind="smart" wallets={smartWallets} />
        <WalletListPanel title="Whale Wallets" kind="whale" wallets={whaleWallets} />
      </div>
    </section>
  );
}

function WalletListPanel({
  title,
  kind,
  wallets,
}: {
  title: string;
  kind: "smart" | "whale";
  wallets: WalletRow[];
}) {
  return (
    <div className="panel walletListPanel">
      <PanelTitle icon={kind === "smart" ? <LineChart size={18} /> : <WalletCards size={18} />} title={`${title} (${formatNumber(wallets.length)})`} />
      <div className="tableScroller">
        <table className="walletTable">
          <thead>
            {kind === "smart" ? (
              <tr>
                <th>Wallet</th>
                <th>ROI</th>
                <th>PnL</th>
                <th>Total Volume</th>
                <th>Portfolio</th>
                <th>Trades</th>
                <th>Last Trade</th>
              </tr>
            ) : (
              <tr>
                <th>Wallet</th>
                <th>Reason</th>
                <th>Total Volume</th>
                <th>Max Trade</th>
                <th>24h Volume</th>
                <th>Trades</th>
                <th>Last Trade</th>
              </tr>
            )}
          </thead>
          <tbody>
            {wallets.length === 0 ? (
              <tr>
                <td colSpan={7}><div className="empty compact">No wallets loaded</div></td>
              </tr>
            ) : wallets.map((wallet) => (
              kind === "smart" ? (
                <tr key={wallet.user_address}>
                  <td><WalletIdentity wallet={wallet.user_address} /></td>
                  <td className="num valuePositive">{formatRatioPercent(wallet.pnl_roi)}</td>
                  <td className={`num ${signedValueClass(wallet.total_pnl)}`}>{formatSignedCurrency(wallet.total_pnl)}</td>
                  <td className="num">{formatCurrency(wallet.traded_notional)}</td>
                  <td className="num">{formatCurrencyPrecise(wallet.portfolio_value)}</td>
                  <td className="num">{formatNumber(wallet.trade_count)}</td>
                  <td>{formatDate(wallet.last_trade_at) || "--"}</td>
                </tr>
              ) : (
                <tr key={wallet.user_address}>
                  <td><WalletIdentity wallet={wallet.user_address} /></td>
                  <td><span className="statusTag">{formatWalletReason(wallet)}</span></td>
                  <td className="num">{formatCurrency(wallet.traded_notional)}</td>
                  <td className="num">{formatCurrency(wallet.max_single_trade_notional)}</td>
                  <td className="num">{formatCurrency(wallet.traded_notional_24h)}</td>
                  <td className="num">{formatNumber(wallet.trade_count)}</td>
                  <td>{formatDate(wallet.last_trade_at) || "--"}</td>
                </tr>
              )
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Markets(props: {
  query: string;
  setQuery: (value: string) => void;
  loading: boolean;
  markets: Market[];
  selectedMarket: Market | null;
  trades: Trade[];
  onSearch: () => void;
  onOpenMarket: (market: Market) => void;
}) {
  return (
    <section className="twoColumn">
      <div className="panel">
        <div className="searchRow">
          <Search size={18} />
          <input value={props.query} onChange={(event) => props.setQuery(event.target.value)} onKeyDown={(event) => event.key === "Enter" && props.onSearch()} />
          <button onClick={props.onSearch}>{props.loading ? <Loader2 className="spin" size={16} /> : "Search"}</button>
        </div>
        <div className="list">
          {props.markets.map((market) => (
            <button className="listItem" key={market.market_id} onClick={() => props.onOpenMarket(market)}>
              <strong>{market.question}</strong>
              <span>{formatCurrency(market.volume)} volume / {market.closed ? "Closed" : "Open"}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="panel detailPanel">
        {props.selectedMarket ? (
          <>
            <h2>{props.selectedMarket.question}</h2>
            <div className="pillRow">
              <span>{props.selectedMarket.active ? "Active" : "Inactive"}</span>
              <span>{props.selectedMarket.closed ? "Closed" : "Open"}</span>
              <span>{formatCurrency(props.selectedMarket.volume)} volume</span>
              <span>{formatCurrency(props.selectedMarket.liquidity)} liquidity</span>
            </div>
            <div className="tokenGrid">
              {(props.selectedMarket.tokens || []).map((token) => (
                <div className="token" key={token.token_id}>
                  <span>{token.outcome || `Outcome ${token.outcome_index}`}</span>
                  <code>{shortId(token.token_id)}</code>
                </div>
              ))}
            </div>
            <h3>Recent Trades</h3>
            <table>
              <thead>
                <tr><th>Time</th><th>Side</th><th>Price</th><th>Size</th><th>Notional</th></tr>
              </thead>
              <tbody>
                {props.trades.map((trade) => (
                  <tr key={trade.trade_id || `${trade.timestamp}-${trade.token_id}`}>
                    <td>{formatDate(trade.timestamp)}</td>
                    <td>{trade.side}</td>
                    <td className="num">{formatNumber(trade.price)}</td>
                    <td className="num">{formatNumber(trade.size)}</td>
                    <td className="num">{formatCurrency(trade.notional)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        ) : (
          <div className="empty">No market selected</div>
        )}
      </div>
    </section>
  );
}

function Traders({
  wallet,
  setWallet,
  profile,
  loading,
  onLoad,
}: {
  wallet: string;
  setWallet: (value: string) => void;
  profile: TraderProfile | null;
  loading: boolean;
  onLoad: () => void;
}) {
  const fields = [
    "trade_count",
    "traded_notional",
    "position_count",
    "current_value",
    "total_pnl",
    "chain_fill_count",
    "chain_traded_notional",
    "chain_mark_to_market_pnl",
  ];
  return (
    <section className="panel">
      <div className="searchRow">
        <UserRound size={18} />
        <input value={wallet} onChange={(event) => setWallet(event.target.value)} placeholder="0x wallet address" onKeyDown={(event) => event.key === "Enter" && onLoad()} />
        <button onClick={onLoad}>{loading ? <Loader2 className="spin" size={16} /> : "Load"}</button>
      </div>
      {profile ? (
        <div className="metrics">
          {fields.map((field) => (
            <div className="metric" key={field}>
              <span>{field.replaceAll("_", " ")}</span>
              <strong>{formatNumber(profile[field] as number)}</strong>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty">Enter a wallet address to inspect trader profile data.</div>
      )}
    </section>
  );
}

function Operations({
  progress,
  progressRows,
  ingestion,
  system,
}: {
  progress: Progress | null;
  progressRows: Array<{ kind: string; total: number; pending: number; running: number; done: number; dead_lettered: number; done_percent: number }>;
  ingestion: IngestionRow[];
  system: SystemStats | null;
}) {
  return (
    <section className="gridPage">
      <SystemPressure system={system} wide />

      <div className="panel wide">
        <PanelTitle icon={<Server size={18} />} title="Task Queue" />
        <table>
          <thead>
            <tr><th>Kind</th><th>Total</th><th>Pending</th><th>Running</th><th>Done</th><th>Dead</th><th>Done %</th></tr>
          </thead>
          <tbody>
            {progressRows.map((row) => (
              <tr key={row.kind}>
                <td>{row.kind}</td>
                <td className="num">{formatNumber(row.total)}</td>
                <td className="num">{formatNumber(row.pending)}</td>
                <td className="num">{formatNumber(row.running)}</td>
                <td className="num">{formatNumber(row.done)}</td>
                <td className="num">{formatNumber(row.dead_lettered)}</td>
                <td className="num">{row.done_percent}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="panel wide">
        <PanelTitle icon={<Activity size={18} />} title="Recent Runs" />
        <table>
          <thead>
            <tr><th>Task</th><th>Node</th><th>Status</th><th>Pages</th><th>Items</th><th>Duration</th></tr>
          </thead>
          <tbody>
            {(progress?.recent_runs || []).map((run) => (
              <tr key={`${run.task_id}-${run.finished_at}`}>
                <td>{run.kind}</td>
                <td>{run.node_id}</td>
                <td>{run.status}</td>
                <td className="num">{run.pages}</td>
                <td className="num">{formatNumber(run.items)}</td>
                <td className="num">{run.duration_seconds}s</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="panel wide">
        <PanelTitle icon={<Database size={18} />} title="Raw Ingestion" />
        <table>
          <thead>
            <tr><th>Source</th><th>Entity</th><th>Batches</th><th>Items</th><th>Last Collected</th></tr>
          </thead>
          <tbody>
            {ingestion.map((row) => (
              <tr key={`${row.source}-${row.entity}`}>
                <td>{row.source}</td>
                <td>{row.entity}</td>
                <td className="num">{formatNumber(row.raw_batches)}</td>
                <td className="num">{formatNumber(row.items)}</td>
                <td>{formatDate(row.last_collected_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SystemPressure({ system, wide = false }: { system: SystemStats | null; wide?: boolean }) {
  return (
    <div className={wide ? "panel wide" : "panel"}>
      <PanelTitle icon={<Server size={18} />} title="Hardware Status" />
      <div className="resourceGrid">
        <ResourceMeter
          icon={<Cpu size={18} />}
          label="CPU"
          percent={system?.cpu?.percent}
          value={formatPercent(system?.cpu?.percent)}
          detail={`${formatCores(system?.cpu?.count)} / load ${formatLoad(system)}`}
        />
        <ResourceMeter
          icon={<MemoryStick size={18} />}
          label="Memory"
          percent={system?.memory?.percent}
          value={formatPercent(system?.memory?.percent)}
          detail={`${formatBytes(system?.memory?.used_bytes)} / ${formatBytes(system?.memory?.total_bytes)}`}
        />
        <ResourceMeter
          icon={<HardDrive size={18} />}
          label="Disk /"
          percent={system?.disk?.percent}
          value={formatPercent(system?.disk?.percent)}
          detail={`${formatBytes(system?.disk?.used_bytes)} / ${formatBytes(system?.disk?.total_bytes)}`}
        />
      </div>
      <div className="resourceMeta">
        <span>Uptime {formatDuration(system?.uptime_seconds)}</span>
        <span>{formatDate(system?.collected_at)}</span>
      </div>
    </div>
  );
}

function ResourceMeter({
  icon,
  label,
  percent,
  value,
  detail,
}: {
  icon: React.ReactNode;
  label: string;
  percent: unknown;
  value: string;
  detail: string;
}) {
  const bounded = boundedPercent(percent);
  return (
    <div className="resourceItem" data-pressure={pressureTone(bounded)}>
      <div className="resourceHead">
        <span className="resourceLabel">{icon}{label}</span>
        <strong>{value}</strong>
      </div>
      <div className="resourceTrack">
        <div style={{ width: `${bounded}%` }} />
      </div>
      <div className="resourceDetail">{detail}</div>
    </div>
  );
}

function NavButton({ active, icon, label, onClick }: { active: boolean; icon: React.ReactNode; label: string; onClick: () => void }) {
  return (
    <button className={active ? "nav active" : "nav"} onClick={onClick}>
      {icon}
      {label}
    </button>
  );
}

function PanelTitle({ icon, title }: { icon: React.ReactNode; title: string }) {
  return <div className="panelTitle">{icon}<h2>{title}</h2></div>;
}

function viewTitle(view: View) {
  return {
    dashboard: "Internal Overview",
    wallets: "Wallets",
    hardware: "Hardware Status",
    markets: "Market Explorer",
    traders: "Trader Profiles",
    operations: "Collection Operations",
  }[view];
}

function formatNumber(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(number);
}

function formatMetric(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return formatNumber(number);
}

function formatCurrency(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(number);
}

function formatCurrencyPrecise(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(number);
}

function formatSignedCurrency(value: unknown) {
  const number = Number(value || 0);
  const formatted = formatCurrencyPrecise(Math.abs(number));
  if (number > 0) return `+${formatted}`;
  if (number < 0) return `-${formatted}`;
  return formatted;
}

function formatPercent(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return `${formatNumber(number)}%`;
}

function formatRatioPercent(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return formatPercent(number * 100);
}

function ratioPercent(value: unknown, total: unknown) {
  const numerator = Number(value || 0);
  const denominator = Number(total || 0);
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) {
    return 0;
  }
  return (numerator / denominator) * 100;
}

function boundedPercent(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Math.max(0, Math.min(number, 100));
}

function formatBytes(value: unknown) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) return "--";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: size >= 100 ? 0 : 1 }).format(size)} ${units[unitIndex]}`;
}

function formatLoad(system: SystemStats | null) {
  const loads = [system?.cpu?.load_avg_1m, system?.cpu?.load_avg_5m, system?.cpu?.load_avg_15m]
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  return loads.length ? loads.map((value) => formatNumber(value)).join(" / ") : "--";
}

function formatCores(value: unknown) {
  const cores = Number(value);
  if (!Number.isFinite(cores) || cores <= 0) return "-- cores";
  return `${formatNumber(cores)} cores`;
}

function formatDate(value: string | null | undefined) {
  if (!value) return "";
  const normalized = /[zZ]|[+-]\d\d:?\d\d$/.test(value) ? value : `${value.replace(" ", "T")}Z`;
  return new Date(normalized).toLocaleString();
}

function formatDuration(value: unknown) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return "--";
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function pressureTone(value: number) {
  if (value >= 85) return "hot";
  if (value >= 70) return "warm";
  return "normal";
}

function errorMessage(exc: unknown) {
  return exc instanceof Error ? exc.message : String(exc);
}

function shortId(value: string) {
  if (!value || value.length < 14) return value;
  return `${value.slice(0, 6)}...${value.slice(-6)}`;
}

function shortAddress(value: string) {
  if (!value || value.length < 12) return value;
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
}

function signedValueClass(value: unknown) {
  const number = Number(value || 0);
  if (number > 0) return "valuePositive";
  if (number < 0) return "valueNegative";
  return "";
}

function formatWalletReason(wallet: WalletRow) {
  if (wallet.whale_reason === "total_volume_and_single_trade") return "Volume + single trade";
  if (wallet.whale_reason === "single_trade") return "Single trade";
  if (wallet.whale_reason === "total_volume") return "Total volume";
  return wallet.whale_tier || "Whale";
}

function WalletIdentity({ wallet }: { wallet: string }) {
  return (
    <div className="walletCell">
      <a className="walletLink" href={polymarketProfileUrl(wallet)} target="_blank" rel="noreferrer">
        <code>{shortAddress(wallet)}</code>
      </a>
      <a
        className="iconMini"
        href={polymarketProfileUrl(wallet)}
        target="_blank"
        rel="noreferrer"
        title="Open Polymarket profile"
      >
        <ExternalLink size={14} />
      </a>
      <button
        className="iconMini"
        type="button"
        title="Copy wallet address"
        onClick={() => copyWallet(wallet)}
      >
        <Copy size={14} />
      </button>
    </div>
  );
}

function polymarketProfileUrl(wallet: string) {
  return `https://polymarket.com/profile/${encodeURIComponent(wallet)}`;
}

async function copyWallet(wallet: string) {
  try {
    await navigator.clipboard.writeText(wallet);
  } catch {
    const textarea = document.createElement("textarea");
    textarea.value = wallet;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    document.body.removeChild(textarea);
  }
}

createRoot(document.getElementById("root")!).render(<App />);
