import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  ChevronRight,
  Cpu,
  Database,
  HardDrive,
  Loader2,
  MemoryStick,
  RefreshCw,
  Search,
  Server,
  Table2,
  Trophy,
  UserRound,
  UsersRound,
  Wallet,
} from "lucide-react";
import {
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import "./styles.css";

const API_BASE = import.meta.env.VITE_ZETTA_API_BASE || "/api";
const DASHBOARD_REFRESH_MS = 30_000;
const MARKET_REFRESH_MS = 30_000;
const LEADERBOARD_REFRESH_MS = 30_000;
const WORLD_CUP_DEFAULT_QUERY = "World Cup";

type Overview = {
  events?: number;
  markets?: number;
  outcome_tokens?: number;
  trades?: number;
  price_points?: number;
  orderbook_snapshots?: number;
  chain_logs?: number;
  last_ingested_at?: string;
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
  category?: string;
  event_title?: string;
  primary_token_id?: string;
  primary_outcome?: string;
  last_price?: number | null;
  price_change_24h?: number | null;
  price_change_pct_24h?: number | null;
  volume_24h?: number;
  trade_count_24h?: number;
  latest_trade_at?: string | null;
  best_bid?: number | null;
  best_ask?: number | null;
  spread?: number | null;
  start_time?: string | null;
  end_time?: string | null;
  updated_at?: string | null;
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
  question?: string;
  slug?: string;
  outcome?: string;
};

type TraderProfile = Record<string, string | number | null>;

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

type LeaderboardUser = {
  rank: string;
  proxyWallet: string;
  userName: string;
  xUsername?: string;
  verifiedBadge?: boolean;
  vol: number;
  pnl: number;
  profileImage?: string;
};

type BiggestWinner = {
  winRank: string;
  proxyWallet: string;
  userName: string;
  eventSlug: string;
  eventTitle: string;
  initialValue: number;
  finalValue: number;
  pnl: number;
  profileImage?: string;
};

type LeaderboardRange = "DAY" | "WEEK" | "MONTH" | "ALL";
type LeaderboardMetric = "profit" | "volume" | "wins";
type MarketFilter = "all" | "active" | "movement" | "liquidity" | "closing";
type View = "markets" | "leaderboard" | "dashboard" | "traders" | "operations";

const rangeOptions: Array<{ label: string; value: LeaderboardRange }> = [
  { label: "Today", value: "DAY" },
  { label: "Weekly", value: "WEEK" },
  { label: "Monthly", value: "MONTH" },
  { label: "All", value: "ALL" },
];

const marketFilters: Array<{ label: string; value: MarketFilter }> = [
  { label: "All", value: "all" },
  { label: "Active", value: "active" },
  { label: "Movers", value: "movement" },
  { label: "Liquid", value: "liquidity" },
  { label: "Closing", value: "closing" },
];

function App() {
  const [view, setView] = useState<View>("markets");
  const [overview, setOverview] = useState<Overview>({});
  const [ingestion, setIngestion] = useState<IngestionRow[]>([]);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [system, setSystem] = useState<SystemStats | null>(null);
  const [markets, setMarkets] = useState<Market[]>([]);
  const [selectedMarket, setSelectedMarket] = useState<Market | null>(null);
  const [marketTrades, setMarketTrades] = useState<Trade[]>([]);
  const [query, setQuery] = useState(WORLD_CUP_DEFAULT_QUERY);
  const [marketFilter, setMarketFilter] = useState<MarketFilter>("all");
  const [leaderboardRange, setLeaderboardRange] = useState<LeaderboardRange>("DAY");
  const [leaderboardMetric, setLeaderboardMetric] = useState<LeaderboardMetric>("profit");
  const [leaderboardRows, setLeaderboardRows] = useState<LeaderboardUser[]>([]);
  const [winnerRows, setWinnerRows] = useState<BiggestWinner[]>([]);
  const [leaderboardLoading, setLeaderboardLoading] = useState(false);
  const [wallet, setWallet] = useState("");
  const [profile, setProfile] = useState<TraderProfile | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const queryRef = useRef(query);

  useEffect(() => {
    void refreshDashboard();
    const timer = window.setInterval(() => {
      void refreshDashboard();
    }, DASHBOARD_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    void searchMarkets(WORLD_CUP_DEFAULT_QUERY);
    const timer = window.setInterval(() => {
      void searchMarkets(queryRef.current, { silent: true });
    }, MARKET_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    void loadLeaderboard();
    const timer = window.setInterval(() => {
      void loadLeaderboard({ silent: true });
    }, LEADERBOARD_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [leaderboardRange, leaderboardMetric]);

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
      setOverview(overviewData.overview || {});
      setIngestion(ingestionData.ingestion || []);
      setProgress(progressData);
      setSystem(systemData.system || null);
    } catch (exc) {
      setError(errorMessage(exc));
    }
  }

  function updateQuery(nextQuery: string) {
    queryRef.current = nextQuery;
    setQuery(nextQuery);
  }

  async function searchMarkets(nextQuery = query, options: { silent?: boolean } = {}) {
    queryRef.current = nextQuery;
    if (!options.silent) setLoading(true);
    setError("");
    try {
      const data = await getJson<{ markets: Market[] }>(
        `/markets/search?scope=world_cup&q=${encodeURIComponent(nextQuery)}&limit=40`,
      );
      const nextMarkets = data.markets || [];
      setMarkets(nextMarkets);
      const stillSelected = nextMarkets.find((market) => market.market_id === selectedMarket?.market_id);
      if (stillSelected) {
        setSelectedMarket(stillSelected);
      } else if (nextMarkets[0]) {
        setSelectedMarket(nextMarkets[0]);
      }
      void loadWorldCupActivity();
    } catch (exc) {
      setError(errorMessage(exc));
    } finally {
      if (!options.silent) setLoading(false);
    }
  }

  async function loadWorldCupActivity() {
    try {
      const data = await getJson<{ trades: Trade[] }>("/markets/trades?scope=world_cup&limit=30");
      setMarketTrades(data.trades || []);
    } catch {
      setMarketTrades([]);
    }
  }

  async function openMarket(market: Market) {
    setSelectedMarket(market);
    setError("");
    try {
      const detail = await getJson<{ market: Market }>(
        `/markets/detail?market_id=${encodeURIComponent(market.market_id)}`,
      );
      setSelectedMarket({ ...market, ...detail.market });
    } catch (exc) {
      setError(errorMessage(exc));
    }
  }

  async function loadLeaderboard(options: { silent?: boolean } = {}) {
    if (!options.silent) setLeaderboardLoading(true);
    setError("");
    try {
      if (leaderboardMetric === "wins") {
        const rows = await getJson<BiggestWinner[]>(
          `/polymarket-data/v1/biggest-winners?category=sports&timePeriod=${leaderboardRange}&limit=50`,
        );
        setWinnerRows(rows || []);
        setLeaderboardRows([]);
      } else {
        const orderBy = leaderboardMetric === "volume" ? "VOL" : "PNL";
        const rows = await getJson<LeaderboardUser[]>(
          `/polymarket-data/v1/leaderboard?category=sports&timePeriod=${leaderboardRange}&orderBy=${orderBy}&limit=50`,
        );
        setLeaderboardRows(rows || []);
        setWinnerRows([]);
      }
    } catch (exc) {
      setError(errorMessage(exc));
    } finally {
      if (!options.silent) setLeaderboardLoading(false);
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
    } catch {
      setError("Trader profile not found yet.");
    } finally {
      setLoading(false);
    }
  }

  const progressRows = useMemo(() => {
    if (!progress) return [];
    return Object.entries(progress.by_kind).map(([kind, counts]) => ({ kind, ...counts }));
  }, [progress]);

  const filteredMarkets = useMemo(() => filterMarkets(markets, marketFilter), [markets, marketFilter]);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <Database size={22} />
          <span>Zetta</span>
        </div>
        <nav>
          <NavButton active={view === "markets"} icon={<Table2 size={18} />} label="World Cup" onClick={() => setView("markets")} />
          <NavButton active={view === "leaderboard"} icon={<Trophy size={18} />} label="Leaderboard" onClick={() => setView("leaderboard")} />
          <NavButton active={view === "dashboard"} icon={<BarChart3 size={18} />} label="Overview" onClick={() => setView("dashboard")} />
          <NavButton active={view === "traders"} icon={<UserRound size={18} />} label="Trader Lookup" onClick={() => setView("traders")} />
          <NavButton active={view === "operations"} icon={<Server size={18} />} label="Operations" onClick={() => setView("operations")} />
        </nav>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <h1>{viewTitle(view)}</h1>
            <p>{viewDescription(view)}</p>
          </div>
          <button className="iconButton" onClick={() => view === "leaderboard" ? loadLeaderboard() : view === "markets" ? searchMarkets() : refreshDashboard()} title="Refresh">
            <RefreshCw size={18} />
          </button>
        </header>

        {error ? <div className="notice"><AlertTriangle size={16} />{error}</div> : null}

        {view === "markets" ? (
          <WorldCupMarkets
            query={query}
            setQuery={updateQuery}
            loading={loading}
            markets={filteredMarkets}
            allMarkets={markets}
            selectedMarket={selectedMarket}
            trades={marketTrades}
            filter={marketFilter}
            setFilter={setMarketFilter}
            onSearch={() => searchMarkets()}
            onOpenMarket={openMarket}
          />
        ) : null}

        {view === "leaderboard" ? (
          <Leaderboard
            range={leaderboardRange}
            setRange={setLeaderboardRange}
            metric={leaderboardMetric}
            setMetric={setLeaderboardMetric}
            rows={leaderboardRows}
            winners={winnerRows}
            loading={leaderboardLoading}
          />
        ) : null}

        {view === "dashboard" ? (
          <Dashboard overview={overview} ingestion={ingestion} progress={progress} system={system} />
        ) : null}

        {view === "traders" ? (
          <Traders wallet={wallet} setWallet={setWallet} profile={profile} loading={loading} onLoad={loadTrader} />
        ) : null}

        {view === "operations" ? <Operations progress={progress} progressRows={progressRows} ingestion={ingestion} system={system} /> : null}
      </main>
    </div>
  );
}

function WorldCupMarkets(props: {
  query: string;
  setQuery: (value: string) => void;
  loading: boolean;
  markets: Market[];
  allMarkets: Market[];
  selectedMarket: Market | null;
  trades: Trade[];
  filter: MarketFilter;
  setFilter: (value: MarketFilter) => void;
  onSearch: () => void;
  onOpenMarket: (market: Market) => void;
}) {
  const totalVolume = sumBy(props.allMarkets, (market) => market.volume);
  const dayVolume = sumBy(props.allMarkets, (market) => market.volume_24h || 0);
  const liquidity = sumBy(props.allMarkets, (market) => market.liquidity);
  const activeCount = props.allMarkets.filter((market) => market.active && !market.closed).length;
  const hottest = [...props.allMarkets]
    .filter((market) => Number.isFinite(Number(market.price_change_pct_24h)))
    .sort((a, b) => Math.abs(Number(b.price_change_pct_24h || 0)) - Math.abs(Number(a.price_change_pct_24h || 0)))[0];

  return (
    <section className="marketWorkspace">
      <div className="workspaceMain">
        <div className="marketHero">
          <div>
            <span className="scopeLabel">FIFA World Cup markets</span>
            <h2>World Cup market board</h2>
            <p>Focused market data for football World Cup contracts only.</p>
          </div>
          <div className="marketSearch">
            <Search size={18} />
            <input
              value={props.query}
              onChange={(event) => props.setQuery(event.target.value)}
              onKeyDown={(event) => event.key === "Enter" && props.onSearch()}
              aria-label="Search World Cup markets"
            />
            <button onClick={props.onSearch}>{props.loading ? <Loader2 className="spin" size={16} /> : "Search"}</button>
          </div>
        </div>

        <div className="marketStats">
          <StatTile label="Active" value={formatNumber(activeCount)} />
          <StatTile label="24h Volume" value={formatCurrency(dayVolume)} />
          <StatTile label="Liquidity" value={formatCurrency(liquidity)} />
          <StatTile label="Total Volume" value={formatCurrency(totalVolume)} />
        </div>

        <div className="marketBoard panelFlush">
          <div className="boardToolbar">
            <SegmentedControl options={marketFilters} value={props.filter} onChange={props.setFilter} />
            <div className="boardCount">{formatNumber(props.markets.length)} markets</div>
          </div>
          <div className="marketRows" role="table" aria-label="World Cup markets">
            <div className="marketRow marketHeader" role="row">
              <span>Market</span>
              <span>Price</span>
              <span>24h</span>
              <span>Volume</span>
              <span>Liquidity</span>
              <span>Ends</span>
            </div>
            {props.markets.map((market) => (
              <button
                className={market.market_id === props.selectedMarket?.market_id ? "marketRow active" : "marketRow"}
                key={market.market_id}
                onClick={() => props.onOpenMarket(market)}
                role="row"
              >
                <span className="marketQuestion">
                  <strong>{market.question}</strong>
                  <small>{market.event_title || "World Cup"} / {market.active && !market.closed ? "Open" : "Closed"}</small>
                </span>
                <span className="marketPrice">{formatPrice(market.last_price)}</span>
                <ChangeCell value={market.price_change_pct_24h} />
                <span className="num">{formatCurrency(market.volume_24h || market.volume)}</span>
                <span className="num">{formatCurrency(market.liquidity)}</span>
                <span className="marketEnds">{formatShortDate(market.end_time || market.start_time)}</span>
              </button>
            ))}
            {!props.markets.length ? <div className="empty">No World Cup markets matched this view.</div> : null}
          </div>
        </div>
      </div>

      <aside className="workspaceSide">
        <MarketFocus market={props.selectedMarket} hottest={hottest} />
        <LiveActivity trades={props.trades} />
      </aside>
    </section>
  );
}

function MarketFocus({ market, hottest }: { market: Market | null; hottest?: Market }) {
  if (!market) {
    return <div className="sidePanel emptyPanel">Select a market to inspect pricing and liquidity.</div>;
  }
  const tokens = market.tokens || [];
  return (
    <div className="sidePanel">
      <div className="sideTitle">
        <h2>Market detail</h2>
        <ChevronRight size={18} />
      </div>
      <h3 className="focusQuestion">{market.question}</h3>
      <div className="focusPrice">
        <strong>{formatPrice(market.last_price)}</strong>
        <ChangeCell value={market.price_change_pct_24h} />
      </div>
      <div className="miniGrid">
        <MiniStat label="Best bid" value={formatPrice(market.best_bid)} />
        <MiniStat label="Best ask" value={formatPrice(market.best_ask)} />
        <MiniStat label="Spread" value={formatPrice(market.spread)} />
        <MiniStat label="Trades 24h" value={formatNumber(market.trade_count_24h)} />
      </div>
      <div className="tokenList">
        {tokens.slice(0, 4).map((token) => (
          <div className="tokenLine" key={token.token_id}>
            <span>{token.outcome || `Outcome ${token.outcome_index}`}</span>
            <code>{shortId(token.token_id)}</code>
          </div>
        ))}
      </div>
      {hottest ? (
        <div className="hottestLine">
          <span>Largest 24h move</span>
          <strong>{hottest.question}</strong>
        </div>
      ) : null}
    </div>
  );
}

function LiveActivity({ trades }: { trades: Trade[] }) {
  return (
    <div className="sidePanel livePanel">
      <div className="sideTitle">
        <h2>Live activity</h2>
        <Activity size={18} />
      </div>
      <div className="activityList">
        {trades.slice(0, 14).map((trade) => (
          <div className="activityItem" key={trade.trade_id || `${trade.timestamp}-${trade.token_id}`}>
            <div>
              <strong>{shortId(trade.user_address)}</strong>
              <span>{trade.side} {trade.outcome || "position"}</span>
            </div>
            <p>{trade.question || shortId(trade.token_id)}</p>
            <small>{formatCurrency(trade.notional)} @ {formatPrice(trade.price)} / {formatRelativeTime(trade.timestamp)}</small>
          </div>
        ))}
        {!trades.length ? <div className="empty">No recent activity loaded yet.</div> : null}
      </div>
    </div>
  );
}

function Leaderboard(props: {
  range: LeaderboardRange;
  setRange: (value: LeaderboardRange) => void;
  metric: LeaderboardMetric;
  setMetric: (value: LeaderboardMetric) => void;
  rows: LeaderboardUser[];
  winners: BiggestWinner[];
  loading: boolean;
}) {
  return (
    <section className="leaderboardPage">
      <div className="leaderboardHero">
        <div>
          <span className="scopeLabel">Polymarket Sports leaderboard</span>
          <h2>Use the official leaderboard data directly</h2>
          <p>Profit, volume, and biggest-winner tables are pulled from Polymarket data-api.</p>
        </div>
        <div className="heroActions">
          {props.loading ? <Loader2 className="spin" size={18} /> : <Trophy size={18} />}
          <span>{rangeLabel(props.range)} refreshes every 30s</span>
        </div>
      </div>

      <div className="boardToolbar leaderboardControls">
        <SegmentedControl options={rangeOptions} value={props.range} onChange={props.setRange} />
        <SegmentedControl
          options={[
            { label: "Profit", value: "profit" as const },
            { label: "Volume", value: "volume" as const },
            { label: "Biggest wins", value: "wins" as const },
          ]}
          value={props.metric}
          onChange={props.setMetric}
        />
      </div>

      <div className="leaderboardGrid">
        <div className="panelFlush leaderboardTable">
          {props.metric === "wins" ? <BiggestWinnerRows winners={props.winners} /> : <LeaderboardRows rows={props.rows} metric={props.metric} />}
        </div>
        <div className="sidePanel leaderboardNote">
          <PanelTitle icon={<Wallet size={18} />} title="Data policy" />
          <p>Leaderboard rows are not recomputed by Zetta. We proxy Polymarket's current sports leaderboard and keep the same time windows.</p>
          <div className="miniGrid single">
            <MiniStat label="Category" value="sports" />
            <MiniStat label="Range" value={rangeLabel(props.range)} />
            <MiniStat label="Source" value="data-api" />
            <MiniStat label="Refresh" value="30s" />
          </div>
        </div>
      </div>
    </section>
  );
}

function LeaderboardRows({ rows, metric }: { rows: LeaderboardUser[]; metric: LeaderboardMetric }) {
  return (
    <div className="leaderRows">
      <div className="leaderRow leaderHeader">
        <span>Rank</span>
        <span>Trader</span>
        <span>Profit</span>
        <span>Volume</span>
      </div>
      {rows.map((row) => (
        <div className="leaderRow" key={`${row.rank}-${row.proxyWallet}`}>
          <span className="rank">#{row.rank}</span>
          <TraderIdentity wallet={row.proxyWallet} name={row.userName} image={row.profileImage} />
          <span className={Number(row.pnl) >= 0 ? "num positive" : "num negative"}>{formatCurrency(row.pnl)}</span>
          <span className={metric === "volume" ? "num emphasis" : "num"}>{formatCurrency(row.vol)}</span>
        </div>
      ))}
      {!rows.length ? <div className="empty">No leaderboard rows loaded yet.</div> : null}
    </div>
  );
}

function BiggestWinnerRows({ winners }: { winners: BiggestWinner[] }) {
  return (
    <div className="leaderRows winsRows">
      <div className="leaderRow leaderHeader">
        <span>Rank</span>
        <span>Trader</span>
        <span>Market</span>
        <span>Win</span>
      </div>
      {winners.map((row) => (
        <div className="leaderRow" key={`${row.winRank}-${row.proxyWallet}-${row.eventSlug}`}>
          <span className="rank">#{row.winRank}</span>
          <TraderIdentity wallet={row.proxyWallet} name={row.userName} image={row.profileImage} />
          <span className="eventTitle">{row.eventTitle}</span>
          <span className="num positive">{formatCurrency(row.pnl)}</span>
        </div>
      ))}
      {!winners.length ? <div className="empty">No biggest-winner rows loaded yet.</div> : null}
    </div>
  );
}

function TraderIdentity({ wallet, name, image }: { wallet: string; name?: string; image?: string }) {
  return (
    <span className="traderIdentity">
      {image ? <img src={image} alt="" /> : <span className="avatarFallback"><UsersRound size={14} /></span>}
      <span>
        <strong>{name || shortId(wallet)}</strong>
        <small>{shortId(wallet)}</small>
      </span>
    </span>
  );
}

function Dashboard({
  overview,
  ingestion,
  progress,
  system,
}: {
  overview: Overview;
  ingestion: IngestionRow[];
  progress: Progress | null;
  system: SystemStats | null;
}) {
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

function StatTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="statTile">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="miniStat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function SegmentedControl<T extends string>({
  options,
  value,
  onChange,
}: {
  options: Array<{ label: string; value: T }>;
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div className="segmentedControl">
      {options.map((option) => (
        <button
          className={option.value === value ? "active" : ""}
          key={option.value}
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

function ChangeCell({ value }: { value: unknown }) {
  const number = Number(value);
  if (!Number.isFinite(number)) return <span className="changeCell muted">--</span>;
  const percent = number * 100;
  const className = percent >= 0 ? "changeCell positive" : "changeCell negative";
  return <span className={className}>{percent >= 0 ? "+" : ""}{formatNumber(percent)}%</span>;
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

function filterMarkets(markets: Market[], filter: MarketFilter) {
  const sorted = [...markets];
  if (filter === "active") return sorted.filter((market) => market.active && !market.closed);
  if (filter === "movement") {
    return sorted
      .filter((market) => Number.isFinite(Number(market.price_change_pct_24h)))
      .sort((a, b) => Math.abs(Number(b.price_change_pct_24h || 0)) - Math.abs(Number(a.price_change_pct_24h || 0)));
  }
  if (filter === "liquidity") return sorted.sort((a, b) => Number(b.liquidity || 0) - Number(a.liquidity || 0));
  if (filter === "closing") {
    return sorted
      .filter((market) => market.end_time || market.start_time)
      .sort((a, b) => new Date(a.end_time || a.start_time || 0).getTime() - new Date(b.end_time || b.start_time || 0).getTime());
  }
  return sorted;
}

function viewTitle(view: View) {
  return {
    markets: "World Cup Markets",
    leaderboard: "Leaderboard",
    dashboard: "Internal Overview",
    traders: "Trader Profiles",
    operations: "Collection Operations",
  }[view];
}

function viewDescription(view: View) {
  return {
    markets: "World Cup football markets, prices, liquidity, and live trade flow.",
    leaderboard: "Polymarket sports leaderboard data, used directly from data-api.",
    dashboard: "Collector health and warehouse progress. Auto-refreshes every 30 seconds.",
    traders: "Lookup a wallet in Zetta's internal mart data.",
    operations: "Backfill queue and ingestion state.",
  }[view];
}

function rangeLabel(value: LeaderboardRange) {
  return rangeOptions.find((option) => option.value === value)?.label || value;
}

function sumBy<T>(items: T[], getter: (item: T) => number) {
  return items.reduce((total, item) => total + Number(getter(item) || 0), 0);
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
  const abs = Math.abs(number);
  if (abs >= 1_000_000) return `${number < 0 ? "-" : ""}$${formatNumber(abs / 1_000_000)}M`;
  if (abs >= 1_000) return `${number < 0 ? "-" : ""}$${formatNumber(abs / 1_000)}K`;
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(number);
}

function formatPrice(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return `${formatNumber(number * 100)}c`;
}

function formatPercent(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return `${formatNumber(number)}%`;
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
  return new Date(value).toLocaleString();
}

function formatShortDate(value: string | null | undefined) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function formatRelativeTime(value: string | null | undefined) {
  if (!value) return "just now";
  const time = new Date(value).getTime();
  if (Number.isNaN(time)) return "just now";
  const seconds = Math.max(0, Math.floor((Date.now() - time) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
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

function boundedPercent(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Math.max(0, Math.min(number, 100));
}

function pressureTone(value: number) {
  if (value >= 85) return "hot";
  if (value >= 70) return "warm";
  return "normal";
}

function errorMessage(exc: unknown) {
  return exc instanceof Error ? exc.message : String(exc);
}

function shortId(value: string | undefined) {
  if (!value) return "--";
  if (value.length < 14) return value;
  return `${value.slice(0, 6)}...${value.slice(-6)}`;
}

createRoot(document.getElementById("root")!).render(<App />);
