import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ArrowLeft,
  Bell,
  CalendarDays,
  ChevronDown,
  CircleDollarSign,
  Copy,
  ExternalLink,
  Filter,
  Flame,
  Gift,
  LineChart,
  Link2,
  Loader2,
  Plus,
  RefreshCw,
  Search,
  Send,
  Star,
  Trash2,
  Trophy,
  UserCheck,
  Wallet,
  X,
} from "lucide-react";
import "./styles.css";

const API_BASE = import.meta.env.VITE_ZETTA_API_BASE || "/api";
const TRADE_STREAM_PATH = import.meta.env.VITE_ZETTA_TRADE_STREAM_PATH || "/stream/trades";
const OFFICIAL_TRADE_FEED_PATH = import.meta.env.VITE_ZETTA_OFFICIAL_TRADE_FEED_PATH || "/stream/official-trades";
const SUMMARY_CACHE_KEY = "zetta.discovery.walletSummaries.v1";
const LIVE_TRADES_CACHE_KEY = "zetta.discovery.liveTrades.v1";
const LIVE_TRADES_CACHE_LIMIT = 1000;
const OFFICIAL_TRADES_LIMIT = 500;
const OFFICIAL_TRADE_SETTINGS_KEY = "zetta.discovery.officialTradeSettings.v1";
const DETAIL_QUERY = "live=1&position_limit=100&activity_limit=80&pnl_points_limit=240";
const SHANGHAI_TIME_ZONE = "Asia/Shanghai";
const SMART_CATEGORIES = ["全部", "政治", "体育", "加密货币", "电竞", "伊朗", "金融", "地缘政治", "科技", "文化", "经济", "天气", "选举", "提及"];
const SMART_SEGMENTS = ["candidate_smart", "strict_smart", "whale", "watch", "active"] as const;
type SmartSegment = (typeof SMART_SEGMENTS)[number];

type Route =
  | { name: "track"; modal?: "add" }
  | { name: "address"; wallet: string }
  | { name: "leaderboard"; tab: "trades" | "smart"; open?: "wallet" | "type" | "category" }
  | { name: "unusual-betting"; slug: string }
  | { name: "official-trades"; open?: "wallet" | "type" | "category" };

type ApiTrackedWallet = {
  address?: string;
  user_address?: string;
  name?: string;
  created_at?: string | null;
  updated_at?: string | null;
};

type WalletPosition = {
  asset?: string;
  condition_id?: string;
  title?: string;
  slug?: string;
  event_slug?: string;
  outcome?: string;
  size?: number;
  avg_price?: number;
  cur_price?: number;
  initial_value?: number;
  current_value?: number;
  cash_pnl?: number;
  percent_pnl?: number;
  realized_pnl?: number;
  total_bought?: number;
  sold_value?: number;
  redeemed_value?: number;
  buy_notional?: number;
  sell_notional?: number;
  buy_count?: number;
  sell_count?: number;
  trade_count?: number;
  activity_count?: number;
  first_activity_at?: string | null;
  last_activity_at?: string | null;
  redeemable?: boolean;
  is_open?: boolean;
  is_settled_or_redeemable?: boolean;
  is_worldcup?: boolean;
  cost_basis_estimate?: number;
  end_date?: string | null;
  icon?: string;
};

type WalletActivity = {
  timestamp?: string;
  activity_type?: string;
  side?: string;
  price?: number;
  size?: number;
  notional?: number;
  title?: string;
  slug?: string;
  event_slug?: string;
  outcome?: string;
  condition_id?: string;
  token_id?: string;
  transaction_hash?: string;
};

type WalletReputation = {
  user_address?: string;
  completed_event_count?: number;
  profitable_event_count?: number;
  losing_event_count?: number;
  win_rate?: number;
  realized_pnl?: number;
  avg_event_roi?: number;
  best_event_pnl?: number;
  worst_event_pnl?: number;
  active_position_count?: number;
  active_event_count?: number;
  active_unrealized_pnl_estimate?: number;
  favorite_category?: string;
  favorite_category_notional?: number;
  first_trade_at?: string;
  last_trade_at?: string;
};

type WalletRiskMetrics = {
  completed_event_count?: number | null;
  profitable_event_count?: number | null;
  losing_event_count?: number | null;
  realized_pnl?: number | null;
  win_rate?: number | null;
  profit_factor?: number | null;
  max_drawdown?: number | null;
  sharpe_ratio?: number | null;
  short_term_ratio?: number | null;
  short_term_win_rate?: number | null;
  short_term_value?: number | null;
  settlement_ratio?: number | null;
  settlement_win_rate?: number | null;
  avg_event_roi?: number | null;
  prediction_score?: number | null;
};

type WalletPerformanceMetrics = {
  avg_holding_seconds?: number | null;
  holding_sample_count?: number | null;
  holding_position_count?: number | null;
  avg_add_count?: number | null;
  add_sample_count?: number | null;
};

type WalletSummary = {
  user_address: string;
  data_source?: string | null;
  data_freshness_status?: string | null;
  latest_total_pnl?: number | null;
  portfolio_total_pnl?: number | null;
  current_pnl?: number | null;
  position_cash_pnl?: number | null;
  pnl_7d?: number | null;
  win_rate?: number | null;
  completed_event_count?: number | null;
  avg_event_roi?: number | null;
  avg_bet?: number | null;
  cash?: number | null;
  portfolio_value?: number | null;
  positions_value?: number | null;
  available_balance?: number | null;
  position_count?: number | null;
  trade_volume_7d?: number | null;
  trade_count_7d?: number | null;
  activity_count?: number | null;
  trade_activity_count?: number | null;
  first_activity_at?: string | null;
  last_activity_at?: string | null;
  pnl_captured_at?: string | null;
  portfolio_captured_at?: string | null;
  pnl_lag_minutes?: number | null;
  favorite_category?: string | null;
  realized_pnl?: number | null;
  profitable_event_count?: number | null;
  losing_event_count?: number | null;
  first_trade_at?: string | null;
  last_trade_at?: string | null;
  cached_at?: string;
};

type WalletDetail = {
  wallet: WalletSummary;
  position_summary?: {
    position_count?: number;
    open_position_count?: number;
    historical_or_redeemable_position_count?: number;
    positive_pnl_position_count?: number;
    negative_pnl_position_count?: number;
    worldcup_position_count?: number;
    current_value?: number;
    open_current_value?: number;
    cash_pnl?: number;
    open_cash_pnl?: number;
    worldcup_cash_pnl?: number;
    worldcup_current_value?: number;
  };
  positions?: WalletPosition[];
  positions_returned?: number;
  positions_available?: number;
  pnl_points?: Array<{ timestamp?: number; datetime?: string | null; pnl?: number }>;
  pnl_point_count?: number;
  activity_summary?: Record<string, string | number | null>;
  activity_by_type?: Array<Record<string, string | number | null>>;
  reputation?: WalletReputation;
  risk_metrics?: WalletRiskMetrics;
  performance_metrics?: WalletPerformanceMetrics;
  recent_activity?: WalletActivity[];
  closed_positions?: WalletPosition[];
  closed_positions_returned?: number;
  closed_positions_available?: number;
};

type SmartWallet = {
  user_address: string;
  trade_count?: number;
  buy_count?: number;
  sell_count?: number;
  traded_notional?: number;
  max_single_trade_notional?: number;
  first_trade_at?: string;
  last_trade_at?: string;
  position_count?: number;
  positions_value?: number;
  portfolio_value?: number;
  available_balance?: number;
  total_pnl?: number;
  pnl_roi?: number;
  is_whale?: boolean;
  is_smart?: boolean;
  is_candidate_smart?: boolean;
  wallet_segment?: string;
  candidate_reason?: string;
  whale_reason?: string;
  traded_notional_24h?: number;
  trade_count_24h?: number;
  buy_notional_24h?: number;
  sell_notional_24h?: number;
  net_notional_24h?: number;
  latest_action?: string;
  data_lag_seconds?: number;
  win_rate?: number | null;
  win_rate_24h?: number | null;
  win_rate_7d?: number | null;
  avg_bet?: number | null;
  realized_pnl?: number;
  completed_event_count?: number;
  profitable_event_count?: number;
  losing_event_count?: number;
  active_unrealized_pnl_estimate?: number;
  favorite_category?: string;
  scope?: string;
  all_site_total_pnl?: number | null;
  all_site_pnl_roi?: number | null;
  total_pnl_scope?: string | null;
  fifa_total_pnl?: number | null;
  fifa_total_pnl_roi?: number | null;
  fifa_pnl_24h?: number | null;
  fifa_pnl_roi_24h?: number | null;
  fifa_pnl_7d?: number | null;
  fifa_pnl_roi_7d?: number | null;
  fifa_win_rate?: number | null;
  fifa_win_rate_24h?: number | null;
  fifa_win_rate_7d?: number | null;
  fifa_traded_notional_24h?: number | null;
  updated_at?: string;
};

type RecentTrade = {
  trade_id?: string;
  transaction_hash?: string;
  timestamp?: string;
  market_id?: string;
  condition_id?: string;
  token_id?: string;
  user_address?: string;
  side?: string;
  price?: number;
  size?: number;
  notional?: number;
  question?: string;
  market_slug?: string;
  event_id?: string;
  event_title?: string;
  event_slug?: string;
  category?: string;
  outcome?: string;
  trader_name?: string;
  trader_pseudonym?: string;
  is_smart?: boolean;
  is_whale?: boolean;
  wallet_total_pnl?: number;
  wallet_pnl_roi?: number;
  wallet_traded_notional?: number;
};

type LiveTradesMeta = {
  source?: string;
  status?: string;
  captured_at?: string;
  latency_seconds?: number | null;
  metadata_missing_count?: number;
  stream_connected?: boolean;
  stream_status?: string;
  stream_last_at?: string;
};

type TradeStreamMessage = {
  type?: string;
  status?: string;
  source?: string;
  captured_at?: string;
  latency_seconds?: number | null;
  server_time?: string;
  upstream_connected?: boolean;
  last_trade_at?: string | null;
  replay?: boolean;
  trade?: RecentTrade;
  trade_id?: string;
  hash?: string;
  transaction_hash?: string;
  timestamp?: string;
  market_id?: string;
  condition_id?: string;
  asset_id?: string;
  token_id?: string;
  maker_address?: string;
  user_address?: string;
  side?: string;
  price?: number | string;
  size?: number | string;
  notional?: number | string;
  question?: string;
  market?: string;
  slug?: string;
  event_slug?: string;
  event_title?: string;
  category?: string;
  outcome?: string;
};

type OfficialTradeFeedMeta = {
  connected?: boolean;
  upstream_connected?: boolean;
  status?: string;
  last_trade_at?: string | null;
  latency_seconds?: number | null;
};

type OfficialTradeFeedSettings = {
  soundEnabled?: boolean;
  volume?: number;
  soundMinNotional?: string;
  gifMinNotional?: string;
};

const DEFAULT_OFFICIAL_TRADE_SETTINGS: OfficialTradeFeedSettings = {
  soundEnabled: false,
  volume: 50,
  soundMinNotional: "1000",
  gifMinNotional: "5000",
};

type UnusualBettingEvent = {
  event_id?: string;
  slug?: string;
  title?: string;
  category?: string;
  active?: boolean;
  closed?: boolean;
  start_time?: string;
  end_time?: string;
  updated_at?: string;
};

type UnusualOutcome = {
  market_slug?: string;
  question?: string;
  outcome?: string;
  user_side?: string;
  signal_type?: string;
  user_fill_rows?: number;
  wallet_count?: number;
  total_notional?: number;
  max_notional?: number;
  avg_price?: number;
  min_price?: number;
  max_price?: number;
  large_trade_count?: number;
  very_large_trade_count?: number;
  extreme_trade_count?: number;
  max_user_notional?: number;
};

type UnusualWallet = {
  market_slug?: string;
  question?: string;
  outcome?: string;
  user_side?: string;
  user_address?: string;
  fills?: number;
  total_notional?: number;
  max_notional?: number;
  avg_price?: number;
  first_ts?: string;
  last_ts?: string;
};

type UnusualTrade = {
  timestamp?: string;
  market_slug?: string;
  question?: string;
  outcome?: string;
  user_side?: string;
  user_address?: string;
  notional?: number;
  price?: number;
  size?: number;
  transaction_hash?: string;
};

type UnusualBettingAnalysis = {
  severity?: string;
  has_large_signal?: boolean;
  large_signal_trade_count?: number;
  very_large_signal_trade_count?: number;
  extreme_signal_trade_count?: number;
  max_signal_trade_notional?: number;
  max_signal_wallet_notional?: number;
  signal_total_notional?: number;
  signal_outcome_count?: number;
  has_large_cold_buy?: boolean;
  large_cold_trade_count?: number;
  very_large_cold_trade_count?: number;
  extreme_cold_trade_count?: number;
  max_cold_trade_notional?: number;
  max_cold_wallet_notional?: number;
  cold_buy_total_notional?: number;
  cold_buy_outcome_count?: number;
  conclusion?: string;
  thresholds?: Record<string, number>;
};

type UnusualBettingDetail = {
  status?: string;
  event?: UnusualBettingEvent;
  parameters?: Record<string, number | string | string[]>;
  fill_summary?: {
    fill_rows?: number;
    total_fill_notional?: number;
    max_fill_notional?: number;
    first_ts?: string;
    last_ts?: string;
  };
  outcome_summary?: UnusualOutcome[];
  signal_outcomes?: UnusualOutcome[];
  signal_wallet_summary?: {
    signal_wallet_count?: number;
    abnormal_wallet_count?: number;
    max_abnormal_wallet_notional?: number;
    max_watch_wallet_notional?: number;
    max_watch_trade_notional?: number;
  };
  signal_wallets?: UnusualWallet[];
  signal_trades?: UnusualTrade[];
  cold_buy_outcomes?: UnusualOutcome[];
  cold_wallets?: UnusualWallet[];
  cold_trades?: UnusualTrade[];
  analysis?: UnusualBettingAnalysis;
  generated_at?: string;
};

type LeaderFilters = {
  walletType: "all" | "new" | "smart" | "whale";
  side: "all" | "BUY" | "SELL";
  category: string;
  minNotional: string;
  maxNotional: string;
  search: string;
};

const DEFAULT_LEADER_FILTERS: LeaderFilters = {
  walletType: "all",
  side: "all",
  category: "all",
  minNotional: "100",
  maxNotional: "",
  search: "",
};

const DEFAULT_OFFICIAL_FILTERS: LeaderFilters = {
  ...DEFAULT_LEADER_FILTERS,
  minNotional: "0",
};

type DetailTab = "positions" | "history" | "activity";

function App() {
  const initialLiveTrades = useMemo(() => loadLiveTradesCache(), []);
  const [route, setRoute] = useState<Route>(() => parseRoute());
  const [trackedWallets, setTrackedWallets] = useState<ApiTrackedWallet[]>([]);
  const [summaries, setSummaries] = useState<Record<string, WalletSummary>>(() => loadSummaryCache());
  const [detail, setDetail] = useState<WalletDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [trackSearch, setTrackSearch] = useState("");
  const [trackRange, setTrackRange] = useState("7d");
  const [detailRange, setDetailRange] = useState("7D");
  const [detailTab, setDetailTab] = useState<DetailTab>("positions");
  const [addAddress, setAddAddress] = useState("");
  const [addName, setAddName] = useState("");
  const [isAdding, setIsAdding] = useState(false);
  const [trackedLoading, setTrackedLoading] = useState(false);
  const [trackedError, setTrackedError] = useState("");
  const [smartWallets, setSmartWallets] = useState<SmartWallet[]>([]);
  const [smartLoading, setSmartLoading] = useState(false);
  const [smartSegment, setSmartSegment] = useState<SmartSegment>("candidate_smart");
  const [smartRange, setSmartRange] = useState("7d");
  const [smartCategory, setSmartCategory] = useState("全部");
  const [liveTradeCache, setLiveTradeCache] = useState<RecentTrade[]>(initialLiveTrades);
  const liveTradeCacheRef = useRef<RecentTrade[]>(liveTradeCache);
  const [recentTrades, setRecentTrades] = useState<RecentTrade[]>(() => filterTradesClientSide(initialLiveTrades, DEFAULT_LEADER_FILTERS));
  const [liveTradesMeta, setLiveTradesMeta] = useState<LiveTradesMeta>({});
  const [tradesLoading, setTradesLoading] = useState(false);
  const [leaderFilters, setLeaderFilters] = useState<LeaderFilters>(DEFAULT_LEADER_FILTERS);
  const [officialFilters, setOfficialFilters] = useState<LeaderFilters>(DEFAULT_OFFICIAL_FILTERS);
  const [newTradeKeys, setNewTradeKeys] = useState<Set<string>>(() => new Set());
  const [officialTrades, setOfficialTrades] = useState<RecentTrade[]>([]);
  const [officialTradeKeys, setOfficialTradeKeys] = useState<Set<string>>(() => new Set());
  const [officialMeta, setOfficialMeta] = useState<OfficialTradeFeedMeta>({});
  const [officialSettings, setOfficialSettings] = useState<OfficialTradeFeedSettings>(() => loadOfficialTradeSettings());
  const [unusualData, setUnusualData] = useState<UnusualBettingDetail | null>(null);
  const [unusualLoading, setUnusualLoading] = useState(false);
  const [unusualError, setUnusualError] = useState("");
  const seenTradeKeysRef = useRef<Set<string>>(new Set(initialLiveTrades.map(tradeCacheKey)));
  const seenOfficialTradeKeysRef = useRef<Set<string>>(new Set());
  const liveTradesInitializedRef = useRef(initialLiveTrades.length > 0);
  const leaderFiltersRef = useRef<LeaderFilters>(leaderFilters);
  const officialFiltersRef = useRef<LeaderFilters>(officialFilters);
  const officialSettingsRef = useRef<OfficialTradeFeedSettings>(officialSettings);
  const streamReconnectTimerRef = useRef<number | null>(null);
  const officialReconnectTimerRef = useRef<number | null>(null);

  useEffect(() => {
    leaderFiltersRef.current = leaderFilters;
  }, [leaderFilters]);

  useEffect(() => {
    officialFiltersRef.current = officialFilters;
  }, [officialFilters]);

  useEffect(() => {
    officialSettingsRef.current = officialSettings;
    saveOfficialTradeSettings(officialSettings);
  }, [officialSettings]);

  useEffect(() => {
    const onHashChange = () => setRoute(parseRoute());
    window.addEventListener("hashchange", onHashChange);
    if (!window.location.hash) {
      window.location.hash = "#/track";
    }
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  const cacheSummary = useCallback((walletSummary: WalletSummary) => {
    const address = normalizeAddress(walletSummary.user_address);
    if (!address) return;
    setSummaries((current) => {
      const next = {
        ...current,
        [address]: { ...current[address], ...walletSummary, user_address: address, cached_at: new Date().toISOString() },
      };
      saveSummaryCache(next);
      return next;
    });
  }, []);

  const fetchTrackedWallets = useCallback(async () => {
    setTrackedLoading(true);
    setTrackedError("");
    try {
      const data = await requestJson<{ wallets: ApiTrackedWallet[] }>("/wallets/tracked");
      setTrackedWallets(data.wallets || []);
    } catch (error) {
      setTrackedError(errorMessage(error));
    } finally {
      setTrackedLoading(false);
    }
  }, []);

  const fetchWalletDetail = useCallback(
    async (wallet: string) => {
      const address = normalizeAddress(wallet);
      if (!address) return null;
      setDetailLoading(true);
      setDetailError("");
      try {
        const data = await requestJson<WalletDetail>(`/wallets/detail?user=${encodeURIComponent(address)}&${DETAIL_QUERY}`);
        setDetail(data);
        cacheSummary(summaryFromDetail(data));
        return data;
      } catch (error) {
        setDetailError(errorMessage(error));
        setDetail(null);
        return null;
      } finally {
        setDetailLoading(false);
      }
    },
    [cacheSummary],
  );

  const fetchSmartWallets = useCallback(async () => {
    setSmartLoading(true);
    try {
      const params = new URLSearchParams({ mode: smartSegment, limit: "100", range: smartRange });
      if (smartCategory !== "全部") params.set("category", smartCategory);
      const data = await requestJson<{ wallets: SmartWallet[] }>(`/wallets/screener?${params.toString()}`);
      setSmartWallets(data.wallets || []);
    } finally {
      setSmartLoading(false);
    }
  }, [smartCategory, smartRange, smartSegment]);

  const mergeLiveTrades = useCallback((rows: RecentTrade[]) => {
    const previousNewestTime = tradeTimestamp(liveTradeCacheRef.current[0]);
    const incomingByKey = new Map(rows.map((row) => [tradeCacheKey(row), row]));
    const incomingKeys = rows.map(tradeCacheKey);
    const newKeys = [...new Set(incomingKeys.filter((key) => !seenTradeKeysRef.current.has(key)))];
    const insertKeys = newKeys
      .filter((key) => {
        const row = incomingByKey.get(key);
        return row && previousNewestTime > 0 && tradeTimestamp(row) >= previousNewestTime;
      })
      .slice(0, 25);
    const merged = mergeTradeRows(liveTradeCacheRef.current, rows, LIVE_TRADES_CACHE_LIMIT);
    seenTradeKeysRef.current = new Set(merged.map(tradeCacheKey));
    liveTradeCacheRef.current = merged;
    setLiveTradeCache(merged);
    saveLiveTradesCache(merged);
    if (insertKeys.length && liveTradesInitializedRef.current) {
      setNewTradeKeys((current) => new Set([...current, ...insertKeys]));
      window.setTimeout(() => {
        setNewTradeKeys((current) => {
          const next = new Set(current);
          for (const key of insertKeys) next.delete(key);
          return next;
        });
      }, 1200);
    }
    liveTradesInitializedRef.current = true;
    return merged;
  }, []);

  const fetchRecentTrades = useCallback(async () => {
    setTradesLoading(true);
    try {
      const useLive = leaderFilters.walletType === "all";
      if (useLive) {
        const params = new URLSearchParams({ limit: "500", ttl: "2", pages: "8" });
        if (leaderFilters.side !== "all") params.set("side", leaderFilters.side);
        if (leaderFilters.category !== "all") params.set("category", leaderFilters.category);
        if (leaderFilters.minNotional.trim()) params.set("min_notional", leaderFilters.minNotional.trim());
        if (leaderFilters.maxNotional.trim()) params.set("max_notional", leaderFilters.maxNotional.trim());
        if (leaderFilters.search.trim()) params.set("q", leaderFilters.search.trim());
        const liveData = await requestJson<LiveTradesMeta & { trades: RecentTrade[] }>(`/trades/live?${params.toString()}`);
        setLiveTradesMeta({
          source: liveData.source,
          status: liveData.status,
          captured_at: liveData.captured_at,
          latency_seconds: liveData.latency_seconds,
          metadata_missing_count: liveData.metadata_missing_count,
        });
        const merged = mergeLiveTrades(liveData.trades || []);
        setRecentTrades(filterTradesClientSide(merged, leaderFilters));
        return;
      }
      const params = new URLSearchParams({ limit: "100" });
      if (leaderFilters.walletType !== "all") params.set("wallet_type", leaderFilters.walletType);
      if (leaderFilters.side !== "all") params.set("side", leaderFilters.side);
      if (leaderFilters.minNotional.trim()) params.set("min_notional", leaderFilters.minNotional.trim());
      if (leaderFilters.maxNotional.trim()) params.set("max_notional", leaderFilters.maxNotional.trim());
      if (leaderFilters.search.trim()) params.set("q", leaderFilters.search.trim());
      const data = await requestJson<{ trades: RecentTrade[] }>(`/trades/recent?${params.toString()}`);
      setLiveTradesMeta({ source: "clickhouse", status: "ok", latency_seconds: null });
      setRecentTrades(filterTradesClientSide(data.trades || [], leaderFilters));
    } finally {
      setTradesLoading(false);
    }
  }, [leaderFilters, mergeLiveTrades]);

  const fetchUnusualBetting = useCallback(async (slug: string) => {
    const eventSlug = slug.trim();
    if (!eventSlug) {
      setUnusualData(null);
      setUnusualError("");
      return;
    }
    setUnusualLoading(true);
    setUnusualError("");
    try {
      const params = new URLSearchParams({ slug: eventSlug, wallet_limit: "50", trade_limit: "50" });
      const data = await requestJson<UnusualBettingDetail>(`/events/unusual-betting?${params.toString()}`);
      setUnusualData(data);
    } catch (error) {
      setUnusualData(null);
      setUnusualError(errorMessage(error));
    } finally {
      setUnusualLoading(false);
    }
  }, []);

  const trackedWalletSet = useMemo(() => new Set(trackedWallets.map(walletAddress).filter(Boolean)), [trackedWallets]);

  useEffect(() => {
    void fetchTrackedWallets();
  }, [fetchTrackedWallets]);

  useEffect(() => {
    void fetchSmartWallets();
  }, [fetchSmartWallets]);

  useEffect(() => {
    void fetchRecentTrades();
  }, [fetchRecentTrades]);

  useEffect(() => {
    const useLive = leaderFilters.walletType === "all";
    if (useLive) {
      setRecentTrades(filterTradesClientSide(liveTradeCache, leaderFilters));
    }
  }, [leaderFilters, liveTradeCache]);

  useEffect(() => {
    if (route.name === "address") {
      setDetailTab("positions");
      void fetchWalletDetail(route.wallet);
    }
  }, [fetchWalletDetail, route]);

  useEffect(() => {
    if (route.name === "unusual-betting") {
      void fetchUnusualBetting(route.slug);
    }
  }, [fetchUnusualBetting, route]);

  useEffect(() => {
    const useLive = leaderFilters.walletType === "all";
    if (route.name !== "leaderboard" || route.tab !== "trades" || !useLive) return;
    let websocket: WebSocket | null = null;
    let stopped = false;
    let reconnectDelay = 1500;

    const clearReconnectTimer = () => {
      if (streamReconnectTimerRef.current !== null) {
        window.clearTimeout(streamReconnectTimerRef.current);
        streamReconnectTimerRef.current = null;
      }
    };

    const connect = () => {
      clearReconnectTimer();
      websocket = new WebSocket(tradeStreamUrl());
      websocket.onopen = () => {
        reconnectDelay = 1500;
        setLiveTradesMeta((current) => ({
          ...current,
          stream_connected: true,
          stream_status: "connected",
          stream_last_at: new Date().toISOString(),
        }));
      };
      websocket.onmessage = (event) => {
        const message = parseTradeStreamMessage(event.data);
        if (!message) return;
        if (message.type === "trade") {
          const trade = tradeFromStreamMessage(message);
          if (!trade) return;
          const merged = mergeLiveTrades([trade]);
          setRecentTrades(filterTradesClientSide(merged, leaderFiltersRef.current));
          setLiveTradesMeta((current) => ({
            ...current,
            source: message.source || current.source || "stream",
            captured_at: message.captured_at || current.captured_at,
            stream_connected: true,
            stream_status: "connected",
            stream_last_at: new Date().toISOString(),
          }));
          return;
        }
        if (message.type === "status" || message.type === "heartbeat" || message.type === "connected") {
          setLiveTradesMeta((current) => ({
            ...current,
            source: message.source || current.source,
            status: message.status || current.status,
            captured_at: message.captured_at || current.captured_at,
            latency_seconds: message.latency_seconds ?? current.latency_seconds,
            stream_connected: true,
            stream_status: message.type === "connected" ? "connected" : message.status || "ok",
            stream_last_at: message.server_time || new Date().toISOString(),
          }));
        }
      };
      websocket.onerror = () => {
        setLiveTradesMeta((current) => ({
          ...current,
          stream_connected: false,
          stream_status: "error",
          stream_last_at: new Date().toISOString(),
        }));
      };
      websocket.onclose = () => {
        websocket = null;
        if (stopped) return;
        setLiveTradesMeta((current) => ({
          ...current,
          stream_connected: false,
          stream_status: "reconnecting",
          stream_last_at: new Date().toISOString(),
        }));
        streamReconnectTimerRef.current = window.setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 1.6, 12_000);
      };
    };

    connect();
    return () => {
      stopped = true;
      clearReconnectTimer();
      if (websocket) {
        websocket.close();
      }
      setLiveTradesMeta((current) => ({ ...current, stream_connected: false, stream_status: "closed" }));
    };
  }, [leaderFilters.walletType, mergeLiveTrades, route]);

  useEffect(() => {
    if (route.name !== "leaderboard" || route.tab !== "trades") return;
    const intervalMs = leaderFilters.walletType === "all" ? 30_000 : 3_000;
    const timer = window.setInterval(() => void fetchRecentTrades(), intervalMs);
    return () => window.clearInterval(timer);
  }, [fetchRecentTrades, leaderFilters.walletType, route]);

  useEffect(() => {
    if (route.name !== "official-trades") return;
    let websocket: WebSocket | null = null;
    let stopped = false;
    let reconnectDelay = 1000;

    const clearReconnectTimer = () => {
      if (officialReconnectTimerRef.current !== null) {
        window.clearTimeout(officialReconnectTimerRef.current);
        officialReconnectTimerRef.current = null;
      }
    };

    const connect = () => {
      clearReconnectTimer();
      websocket = new WebSocket(officialTradeFeedUrl());
      websocket.onopen = () => {
        reconnectDelay = 1000;
        setOfficialMeta((current) => ({ ...current, connected: true, status: "connected" }));
      };
      websocket.onmessage = (event) => {
        const message = parseTradeStreamMessage(event.data);
        if (!message) return;
        if (message.type === "trade") {
          const trade = tradeFromStreamMessage(message);
          if (!trade) return;
          const key = tradeCacheKey(trade);
          if (seenOfficialTradeKeysRef.current.has(key)) return;
          seenOfficialTradeKeysRef.current.add(key);
          setOfficialTrades((current) => [trade, ...current].slice(0, OFFICIAL_TRADES_LIMIT));
          const settings = officialSettingsRef.current;
          const notional = Number(trade.notional || 0);
          const visible = officialTradeMatchesFilters(trade, officialFiltersRef.current, trackedWalletSet);
          if (visible && notional >= officialGifMinNotional(settings)) {
            setOfficialTradeKeys((current) => new Set([...current, key]));
            window.setTimeout(() => {
              setOfficialTradeKeys((current) => {
                const next = new Set(current);
                next.delete(key);
                return next;
              });
            }, 900);
          }
          if (!message.replay && visible && settings.soundEnabled && notional >= numberSetting(settings.soundMinNotional || "", 0)) {
            playTradeSound(settings.volume || 50, trade.side);
          }
          setOfficialMeta((current) => ({
            ...current,
            connected: true,
            upstream_connected: true,
            status: "live",
            last_trade_at: trade.timestamp || current.last_trade_at,
            latency_seconds: message.latency_seconds ?? current.latency_seconds,
          }));
          return;
        }
        if (message.type === "status" || message.type === "connected" || message.type === "heartbeat") {
          setOfficialMeta((current) => ({
            ...current,
            connected: true,
            upstream_connected: message.upstream_connected ?? current.upstream_connected,
            status: message.status || current.status,
            last_trade_at: message.last_trade_at || current.last_trade_at,
          }));
        }
      };
      websocket.onerror = () => {
        setOfficialMeta((current) => ({ ...current, connected: false, status: "error" }));
      };
      websocket.onclose = () => {
        websocket = null;
        if (stopped) return;
        setOfficialMeta((current) => ({ ...current, connected: false, status: "reconnecting" }));
        officialReconnectTimerRef.current = window.setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 1.6, 10_000);
      };
    };

    connect();
    return () => {
      stopped = true;
      clearReconnectTimer();
      if (websocket) websocket.close();
      setOfficialMeta((current) => ({ ...current, connected: false, status: "closed" }));
    };
  }, [route, trackedWalletSet]);

  async function addTrackedWallet(event: React.FormEvent) {
    event.preventDefault();
    const address = normalizeAddress(addAddress);
    if (!address) {
      setTrackedError("请输入有效的钱包地址");
      return;
    }
    setIsAdding(true);
    setTrackedError("");
    try {
      const data = await requestJson<{ wallet: ApiTrackedWallet }>("/wallets/tracked", {
        method: "POST",
        body: JSON.stringify({ address, name: addName.trim() }),
      });
      setTrackedWallets((current) => upsertTrackedWallet(current, data.wallet || { user_address: address, name: addName }));
      await fetchWalletDetail(address);
      setAddAddress("");
      setAddName("");
      navigate("#/track");
    } catch (error) {
      setTrackedError(errorMessage(error));
    } finally {
      setIsAdding(false);
    }
  }

  async function removeWallet(wallet: string) {
    const address = normalizeAddress(wallet);
    if (!address) return;
    await requestJson<{ deleted: boolean }>("/wallets/tracked", {
      method: "DELETE",
      body: JSON.stringify({ address }),
    });
    setTrackedWallets((current) => current.filter((item) => walletAddress(item) !== address));
  }

  async function clearTrackedWallets() {
    for (const wallet of trackedWallets) {
      const address = walletAddress(wallet);
      if (address) {
        await removeWallet(address);
      }
    }
  }

  const filteredOfficialTrades = useMemo(
    () => filterOfficialTrades(officialTrades, officialFilters, trackedWalletSet),
    [officialTrades, officialFilters, trackedWalletSet],
  );
  const activeWallet = route.name === "address" ? walletByAddress(trackedWallets, route.wallet) : null;
  const activeTab = route.name === "leaderboard" ? route.tab : undefined;
  const activeHeader = route.name === "leaderboard" ? "排行榜" : route.name === "unusual-betting" ? "异常分析" : route.name === "official-trades" ? "官方实时" : "追踪";

  return (
    <div className="page">
      <Header active={activeHeader} />
      <SubToolbar />
      {trackedError ? <Notice message={trackedError} /> : null}

      {route.name === "track" ? (
        <TrackPage
          wallets={trackedWallets}
          summaries={summaries}
          loading={trackedLoading}
          search={trackSearch}
          range={trackRange}
          onSearchChange={setTrackSearch}
          onRangeChange={setTrackRange}
          onRefresh={fetchTrackedWallets}
          onAdd={() => navigate("#/track?modal=add")}
          onClear={clearTrackedWallets}
          onRemove={removeWallet}
        />
      ) : null}

      {route.name === "address" ? (
        <DetailPage
          wallet={activeWallet}
          address={route.wallet}
          detail={detail}
          loading={detailLoading}
          error={detailError}
          range={detailRange}
          tab={detailTab}
          isTracked={Boolean(activeWallet)}
          onRangeChange={setDetailRange}
          onTabChange={setDetailTab}
          onRefresh={() => fetchWalletDetail(route.wallet)}
        />
      ) : null}

      {route.name === "leaderboard" ? (
        <LeaderboardPage
          tab={activeTab || "trades"}
          open={route.open}
          smartWallets={smartWallets}
          smartLoading={smartLoading}
          recentTrades={recentTrades}
          tradesLoading={tradesLoading}
          liveTradesMeta={liveTradesMeta}
          filters={leaderFilters}
          newTradeKeys={newTradeKeys}
          smartRange={smartRange}
          smartCategory={smartCategory}
          smartSegment={smartSegment}
          onFilterChange={setLeaderFilters}
          onSmartRangeChange={setSmartRange}
          onSmartCategoryChange={setSmartCategory}
          onSmartSegmentChange={setSmartSegment}
          onRefreshTrades={fetchRecentTrades}
        />
      ) : null}

      {route.name === "official-trades" ? (
        <OfficialTradesPage
          rows={filteredOfficialTrades}
          totalRows={officialTrades.length}
          meta={officialMeta}
          filters={officialFilters}
          open={route.open}
          trackedWalletCount={trackedWalletSet.size}
          newTradeKeys={officialTradeKeys}
          onFilterChange={setOfficialFilters}
        />
      ) : null}

      {route.name === "unusual-betting" ? (
        <UnusualBettingPage
          slug={route.slug}
          data={unusualData}
          loading={unusualLoading}
          error={unusualError}
          onRefresh={() => fetchUnusualBetting(route.slug)}
        />
      ) : null}

      <BottomTicker />
      {route.name === "track" && route.modal === "add" ? (
        <AddWalletModal
          address={addAddress}
          name={addName}
          loading={isAdding}
          onAddressChange={setAddAddress}
          onNameChange={setAddName}
          onSubmit={addTrackedWallet}
          onClose={() => navigate("#/track")}
        />
      ) : null}
    </div>
  );
}

function Header({ active }: { active: "追踪" | "排行榜" | "异常分析" | "官方实时" }) {
  return (
    <header className="app-header">
      <button className="logo" onClick={() => navigate("#/track")} aria-label="Zetta">
        <span className="zetta-mark">Z</span>
        <span className="logo-word">
          <span className="logo-title">ZETTA</span>
        </span>
      </button>
      <nav className="nav" aria-label="Primary">
        <button className="nav-item trophy" type="button">世界杯</button>
        <button className={active === "追踪" ? "nav-item active" : "nav-item"} type="button" onClick={() => navigate("#/track")}>追踪</button>
        <button className={active === "排行榜" ? "nav-item active" : "nav-item"} type="button" onClick={() => navigate("#/leaderboard")}>排行榜</button>
        <button className={active === "官方实时" ? "nav-item active" : "nav-item"} type="button" onClick={() => navigate("#/official-trades")}>官方实时</button>
        <button className={active === "异常分析" ? "nav-item active" : "nav-item"} type="button" onClick={() => navigate("#/unusual-betting")}>异常分析</button>
      </nav>
      <div className="header-actions">
        <button className="search" type="button" onClick={() => navigate("#/leaderboard")}>
          <Search size={15} />
          <span className="search-placeholder">搜索事件或钱包...</span>
          <span className="slash-key">/</span>
        </button>
        <button className="mini" type="button"><Gift size={15} /><span>$0</span></button>
        <button className="mini" type="button"><Wallet size={15} /><span>$0</span></button>
        <button className="icon-btn" type="button" title="Favorites"><Star size={16} /></button>
        <button className="icon-btn bell-dot" type="button" title="Notifications"><Bell size={15} /></button>
        <button className="recharge" type="button"><CircleDollarSign size={15} />充值</button>
        <button className="profile-btn" type="button" title="Profile"><Flame size={18} /></button>
      </div>
    </header>
  );
}

function SubToolbar() {
  return (
    <div className="sub-toolbar">
      <span className="star">★</span>
      <span>♕</span>
      <span className="divider" />
      <span className="small-muted">UTC+8</span>
    </div>
  );
}

function BottomTicker() {
  return (
    <footer className="bottom-ticker">
      <div className="bottom-nav">
        <button className="bottom-nav-item" type="button" onClick={() => navigate("#/track")}><Wallet size={15} />钱包追踪</button>
        <button className="bottom-nav-item" type="button" onClick={() => navigate("#/leaderboard?tab=SmartMoney")}><Trophy size={15} />排行榜</button>
        <div className="status-pill"><span className="dot" />Stable 146 MS&nbsp;&nbsp;60 FPS</div>
        <span className="small-muted">&nbsp;&nbsp;◉&nbsp;&nbsp;◐&nbsp;&nbsp;×</span>
      </div>
    </footer>
  );
}

function Notice({ message }: { message: string }) {
  return <div className="notice">{message}</div>;
}

function TrackPage({
  wallets,
  summaries,
  loading,
  search,
  range,
  onSearchChange,
  onRangeChange,
  onRefresh,
  onAdd,
  onClear,
  onRemove,
}: {
  wallets: ApiTrackedWallet[];
  summaries: Record<string, WalletSummary>;
  loading: boolean;
  search: string;
  range: string;
  onSearchChange: (value: string) => void;
  onRangeChange: (value: string) => void;
  onRefresh: () => void;
  onAdd: () => void;
  onClear: () => void;
  onRemove: (wallet: string) => void;
}) {
  const filtered = wallets.filter((wallet) => {
    const address = walletAddress(wallet);
    const name = wallet.name || "";
    const term = search.trim().toLowerCase();
    return !term || address.includes(term) || name.toLowerCase().includes(term);
  });

  return (
    <main className="track-main">
      <div className="list-title-row">
        <div className="section-title"><span>追踪钱包 ({wallets.length})</span><span>追踪</span></div>
        <div className="list-controls">
          <Segmented value={range} values={["1d", "7d", "30d", "All"]} onChange={onRangeChange} />
          <label className="list-search">
            <Search size={15} />
            <input value={search} onChange={(event) => onSearchChange(event.target.value)} placeholder="在列表中搜索钱包" />
          </label>
          <button className="action-button" type="button" onClick={onAdd}><Plus size={14} />添加钱包</button>
          <button className="action-button" type="button" onClick={onRefresh}>{loading ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />}刷新列表</button>
          <button className="action-button disabled" type="button" onClick={onClear} disabled={!wallets.length}><Trash2 size={14} />移除全部</button>
        </div>
      </div>
      <div className="table-wrap">
        <table className="track-table">
          <colgroup>
            <col style={{ width: "23%" }} />
            <col style={{ width: "14%" }} />
            <col style={{ width: "15%" }} />
            <col style={{ width: "15%" }} />
            <col style={{ width: "16%" }} />
            <col style={{ width: "10%" }} />
            <col style={{ width: "7%" }} />
          </colgroup>
          <thead>
            <tr>
              <th className="sortable">钱包</th>
              <th className="sortable">7d 盈亏</th>
              <th className="sortable">胜率 / 平均投注</th>
              <th className="sortable">现金</th>
              <th className="sortable">7d 交易量 / 交易次数</th>
              <th>最后活跃</th>
              <th className="right">操作</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((wallet) => {
              const address = walletAddress(wallet);
              const summary = summaries[address];
              return (
                <tr key={address}>
                  <td>
                    <button className="wallet-cell" type="button" onClick={() => navigateAddress(address)}>
                      <WalletAvatar />
                      <span>
                        <span className="wallet-name">{wallet.name || shortAddress(address)} <Link2 size={12} /><span className="heart">♥</span></span>
                        <span className="wallet-meta">{walletAge(summary?.first_activity_at || wallet.created_at)} | {shortAddress(address)} <Copy size={12} /></span>
                      </span>
                    </button>
                  </td>
                  <td><ProfitCell value={summary?.pnl_7d} /></td>
                  <td>
                    <div className="cell-stack">
                      <strong className="value-main">{formatRatioPercent(summary?.win_rate)}</strong>
                      <span className="small-muted">{formatCurrency(summary?.avg_bet)}</span>
                    </div>
                  </td>
                  <td><span className="currency"><span className="money-icon" /><strong className="value-main">{formatCurrency(summary?.cash ?? summary?.available_balance)}</strong></span></td>
                  <td>
                    <div className="cell-stack">
                      <strong className="value-main">{formatCurrency(summary?.trade_volume_7d)}</strong>
                      <span className="small-muted">{formatCount(summary?.trade_count_7d)}</span>
                    </div>
                  </td>
                  <td><span className="small-muted">{formatRelativeTime(summary?.last_activity_at || summary?.last_trade_at)}</span></td>
                  <td className="right">
                    <button className="external" type="button" onClick={() => navigateAddress(address)} title="查看详情"><ExternalLink size={15} /></button>
                    <button className="external danger" type="button" onClick={() => onRemove(address)} title="移除"><Trash2 size={14} /></button>
                  </td>
                </tr>
              );
            })}
            {!filtered.length ? (
              <tr><td colSpan={7}><div className="empty-state">还没有追踪钱包。添加钱包后会立即拉取一次实时数据，并在列表缓存展示。</div></td></tr>
            ) : null}
          </tbody>
        </table>
      </div>
      <div className="empty-space" />
    </main>
  );
}

function DetailPage({
  wallet,
  address,
  detail,
  loading,
  error,
  range,
  tab,
  isTracked,
  onRangeChange,
  onTabChange,
  onRefresh,
}: {
  wallet: ApiTrackedWallet | null;
  address: string;
  detail: WalletDetail | null;
  loading: boolean;
  error: string;
  range: string;
  tab: DetailTab;
  isTracked: boolean;
  onRangeChange: (value: string) => void;
  onTabChange: (value: DetailTab) => void;
  onRefresh: () => void;
}) {
  const summary = detail?.wallet;
  const reputation = detail?.reputation;
  const category = reputation?.favorite_category || summary?.favorite_category || "未分类";

  return (
    <main className="detail-main">
      <div className="detail-hero">
        <div className="profile">
          <button className="back-btn" type="button" onClick={() => navigate("#/track")}><ArrowLeft size={17} />返回</button>
          <WalletAvatar size="large" />
          <div>
            <div className="profile-name">{wallet?.name || shortAddress(address)} <Link2 size={14} /> <Send size={14} /></div>
            <div className="profile-meta">
              <span><CalendarDays size={14} /> 钱包年龄 {walletAge(reputation?.first_trade_at || summary?.first_activity_at || wallet?.created_at)}</span>
              <span>{shortAddress(address)} <Copy size={12} /></span>
              {summary?.data_source ? <span>{summary.data_source === "live" ? "实时接口" : "快照数据"}</span> : null}
            </div>
            <div className="focus-chip">✽ 专注{category}</div>
          </div>
        </div>
        <div className="detail-actions">
          <button className={isTracked ? "followed" : "followed muted"} type="button"><UserCheck size={15} />{isTracked ? "已追踪" : "未追踪"}</button>
          <button className="followed" type="button" onClick={onRefresh}>{loading ? <Loader2 className="spin" size={15} /> : <RefreshCw size={15} />}快速更新</button>
          <div className="detail-filters">
            <span>类别</span>
            <button className="select" type="button">全部 <ChevronDown size={13} /></button>
            <Segmented value={range} values={["1D", "7D", "30D", "All"]} onChange={onRangeChange} />
          </div>
        </div>
      </div>
      {error ? <Notice message={error} /> : null}
      {loading && !detail ? <div className="loading-panel"><Loader2 className="spin" size={18} /> 正在拉取 Polymarket 实时钱包数据...</div> : null}
      {detail ? (
        <>
          <div className="detail-grid">
            <AssetPanel detail={detail} />
            <PerformancePanel detail={detail} />
            <RiskPanel detail={detail} />
          </div>
          <PositionsPanel detail={detail} tab={tab} onTabChange={onTabChange} />
        </>
      ) : null}
    </main>
  );
}

function AssetPanel({ detail }: { detail: WalletDetail }) {
  const wallet = detail.wallet;
  const totalPnl = firstNumber(wallet.latest_total_pnl, wallet.portfolio_total_pnl);
  const portfolioValue = firstNumber(wallet.portfolio_value, wallet.positions_value, wallet.available_balance);
  const pnlPct = pnlPercent(totalPnl, portfolioValue);
  const pnl7dPct = pnlPercent(wallet.pnl_7d, portfolioValue);
  const heatCells = buildHeatCells(detail.pnl_points || []);

  return (
    <section className="panel">
      <div className="kpi-label">总资产</div>
      <div className="asset-value">{formatCurrency(portfolioValue)}</div>
      <div className="asset-kpis">
        <div><div className="kpi-label">总盈亏</div><div className={classForNumber(totalPnl, "kpi-value")}>{formatSignedCurrency(totalPnl)} <span>{pnlPct}</span></div></div>
        <div><div className="kpi-label">7D 已实现盈亏</div><div className={classForNumber(wallet.pnl_7d, "kpi-value")}>{formatSignedCurrency(wallet.pnl_7d)} <span>{pnl7dPct}</span></div></div>
        <div><div className="kpi-label">现金余额 (USDC)</div><div className="kpi-value">{formatCurrency(wallet.cash ?? wallet.available_balance)}</div></div>
        <div><div className="kpi-label">奖励</div><div className="kpi-value">$0 <span className="reward-note">0 待发放积分</span></div></div>
      </div>
      <div className="calendar-head">
        <CalendarDays size={14} />
        <span>{currentMonthLabel()} UTC+8</span>
        <span>今天</span>
        <span className="calendar-actions"><button className="square-tool" type="button"><CalendarDays size={14} /></button><button className="square-tool" type="button"><LineChart size={14} /></button></span>
      </div>
      <div className="heatmap">
        {heatCells.map((cell, index) => <div className={`heat-cell ${cell.cls}`} key={`${index}-${cell.label}`}>{cell.label}</div>)}
      </div>
    </section>
  );
}

function PerformancePanel({ detail }: { detail: WalletDetail }) {
  const wallet = detail.wallet;
  const activity = detail.activity_summary || {};
  const performance = detail.performance_metrics || {};
  const positions = [...(detail.closed_positions || []), ...(detail.positions || [])];
  const best = bestPosition(positions);
  const worst = worstPosition(positions);

  return (
    <section className="panel">
      <div className="panel-title">表现与偏好</div>
      <div className="panel-list">
        <MetricRow label="7d 交易次数" value={formatCount(wallet.trade_count_7d)} />
        <MetricRow label="7d 交易量" value={formatCurrency(wallet.trade_volume_7d)} />
        <MetricRow label="7d 已实现盈亏" value={formatSignedCurrency(wallet.pnl_7d)} tone={tone(wallet.pnl_7d)} />
        <MetricRow label="当前盈亏" value={formatSignedCurrency(wallet.current_pnl)} tone={tone(wallet.current_pnl)} />
      </div>
      <div className="panel-sep" />
      <div className="panel-list">
        <MetricRow label="7d 参与市场数" value={formatCount(uniqueRecentMarkets(detail.recent_activity || []))} />
        <MetricRow label="平均初始成本" value={formatCurrency(wallet.avg_bet ?? activity.avg_bet)} />
        <MetricRow label="平均持仓时间" value={formatDuration(performance.avg_holding_seconds)} />
        <MetricRow label="平均补仓次数" value={formatNumber(performance.avg_add_count, 3)} />
        <MetricRow label="最后活跃时间" value={formatRelativeTime(wallet.last_activity_at || detail.reputation?.last_trade_at)} />
      </div>
      <div className="panel-sep" />
      <div className="best-worst">
        <DealCard label="最佳历史市场" position={best} />
        <DealCard label="最差历史市场" position={worst} />
      </div>
    </section>
  );
}

function RiskPanel({ detail }: { detail: WalletDetail }) {
  const reputation = detail.reputation || {};
  const risk = detail.risk_metrics || {};
  const wallet = detail.wallet;
  const maxDrawdown = firstNumber(risk.max_drawdown, computeMaxDrawdown(detail.pnl_points || []));
  const winRate = firstNumber(risk.win_rate, reputation.win_rate, wallet.win_rate);
  const predictionScore = firstNumber(risk.prediction_score, risk.avg_event_roi, reputation.avg_event_roi, wallet.avg_event_roi);

  return (
    <section className="panel risk-panel">
      <div className="panel-title">收益与风险</div>
      <div className="panel-list">
        <MetricRow label="胜率" value={formatRatioPercent(winRate)} />
        <MetricRow label="盈利因子" value={formatNumber(risk.profit_factor)} />
        <MetricRow label="最大回撤" value={formatCurrency(maxDrawdown)} />
        <MetricRow label="夏普比率" value={formatNumber(risk.sharpe_ratio)} />
      </div>
      <div className="panel-sep" />
      <div className="panel-list">
        <MetricRow label="短线占比" value={formatRatioPercent(risk.short_term_ratio)} />
        <MetricRow label="短线胜率" value={formatRatioPercent(risk.short_term_win_rate)} />
        <MetricRow label="短线价值" value={formatSignedCurrency(risk.short_term_value)} tone={tone(risk.short_term_value)} />
      </div>
      <div className="panel-sep" />
      <div className="panel-list">
        <MetricRow label="结算占比" value={formatRatioPercent(risk.settlement_ratio)} />
        <MetricRow label="结算胜率" value={formatRatioPercent(risk.settlement_win_rate)} />
        <MetricRow label="预测评分" value={formatSignedRatioPercent(predictionScore)} tone={tone(predictionScore)} />
      </div>
      <div className="panel-sep" />
      <div className="category-chip">{reputation.favorite_category || wallet.favorite_category || "全部"} 100%</div>
    </section>
  );
}

function PositionsPanel({ detail, tab, onTabChange }: { detail: WalletDetail; tab: DetailTab; onTabChange: (value: DetailTab) => void }) {
  const allPositions = detail.positions || [];
  const openRows = allPositions.filter((position) => position.is_open === true);
  const closedRows = detail.closed_positions || allPositions.filter((position) => position.is_open === false || position.is_settled_or_redeemable);

  return (
    <section className="positions">
      <div className="tabs">
        <button className={tab === "positions" ? "tab active" : "tab"} type="button" onClick={() => onTabChange("positions")}>仓位</button>
        <button className={tab === "history" ? "tab active" : "tab"} type="button" onClick={() => onTabChange("history")}>结束</button>
        <button className={tab === "activity" ? "tab active" : "tab"} type="button" onClick={() => onTabChange("activity")}>活动</button>
        <button className="tab-search" type="button"><Search size={15} /></button>
      </div>
      <div className="table-wrap">
        {tab === "activity" ? <ActivityTable rows={detail.recent_activity || []} /> : <PositionTable rows={tab === "positions" ? openRows : closedRows} />}
      </div>
    </section>
  );
}

function PositionTable({ rows }: { rows: WalletPosition[] }) {
  return (
    <table className="positions-table">
      <colgroup>
        <col style={{ width: "30%" }} />
        <col style={{ width: "8%" }} />
        <col style={{ width: "7%" }} />
        <col style={{ width: "10%" }} />
        <col style={{ width: "8%" }} />
        <col style={{ width: "9%" }} />
        <col style={{ width: "9%" }} />
        <col style={{ width: "9%" }} />
        <col style={{ width: "7%" }} />
        <col style={{ width: "3%" }} />
      </colgroup>
      <thead>
        <tr>
          <th>市场</th><th>结果</th><th className="sortable">份额</th><th className="sortable">均价 / 现价</th><th className="sortable">投注额</th><th className="sortable">预期获得</th><th>金额</th><th className="sortable">当前盈亏</th><th className="sortable">时间</th><th className="right">操作</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((position) => {
          const stake = firstNumber(position.cost_basis_estimate, position.initial_value, position.total_bought);
          const recovered = Number(position.sold_value || 0) + Number(position.redeemed_value || 0);
          const currentValue = recovered > 0 ? recovered : Number(position.current_value || 0);
          const pnlValue = Number.isFinite(Number(position.realized_pnl)) && recovered > 0 ? Number(position.realized_pnl) : Number(position.cash_pnl || 0);
          const expectedValue = Number(position.size || 0);
          const lastTime = position.last_activity_at || position.end_date;
          return (
            <tr key={`${position.condition_id}-${position.asset}-${position.outcome}`}>
              <td><div className="market"><span className="flag">{position.is_worldcup ? "⚽" : ""}</span><span>{position.title || position.slug || shortAddress(position.condition_id || "")}</span></div></td>
              <td className="positive">{position.outcome || "--"}</td>
              <td>{formatCompactNumber(position.size)}</td>
              <td>{formatPrice(position.avg_price)} &nbsp;→&nbsp; {formatPrice(position.cur_price)}</td>
              <td>{formatCurrency(stake)}</td>
              <td>{formatCurrency(expectedValue)}</td>
              <td className={tone(currentValue)}>{formatCurrency(currentValue)}</td>
              <td className={tone(pnlValue)}>{formatSignedCurrency(pnlValue)} {formatPercentValue(position.percent_pnl)}</td>
              <td>{formatShortDate(lastTime)}</td>
              <td className="right"><Send size={14} /></td>
            </tr>
          );
        })}
        {!rows.length ? <tr><td colSpan={10}><div className="empty-state">没有符合当前筛选的仓位。</div></td></tr> : null}
      </tbody>
    </table>
  );
}

function ActivityTable({ rows }: { rows: WalletActivity[] }) {
  return (
    <table className="positions-table">
      <thead>
        <tr><th>类型</th><th>金额</th><th>手续费</th><th>份额</th><th>结果</th><th>价格</th><th>角色</th><th>市场</th><th>时间</th></tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={`${row.transaction_hash}-${index}`}>
            <td><span className={`trade-type ${row.side === "SELL" ? "sell" : "buy"}`}>{row.side === "SELL" ? "卖出" : row.side === "BUY" ? "买入" : row.activity_type || "--"}</span></td>
            <td className={row.side === "SELL" ? "negative" : "positive"}>{formatCurrency(row.notional)}</td>
            <td>--</td>
            <td>{formatCompactNumber(row.size)}</td>
            <td>{row.outcome || "--"}</td>
            <td>{formatPrice(row.price)}</td>
            <td>--</td>
            <td><div className="market leader-market"><strong>{row.title || row.slug || shortAddress(row.condition_id || "")}</strong></div></td>
            <td>{formatDateTime(row.timestamp)}</td>
          </tr>
        ))}
        {!rows.length ? <tr><td colSpan={9}><div className="empty-state">暂无活动记录。</div></td></tr> : null}
      </tbody>
    </table>
  );
}

function LeaderboardPage({
  tab,
  open,
  smartWallets,
  smartLoading,
  recentTrades,
  tradesLoading,
  liveTradesMeta,
  filters,
  newTradeKeys,
  smartRange,
  smartCategory,
  smartSegment,
  onFilterChange,
  onSmartRangeChange,
  onSmartCategoryChange,
  onSmartSegmentChange,
  onRefreshTrades,
}: {
  tab: "trades" | "smart";
  open?: "wallet" | "type" | "category";
  smartWallets: SmartWallet[];
  smartLoading: boolean;
  recentTrades: RecentTrade[];
  tradesLoading: boolean;
  liveTradesMeta: LiveTradesMeta;
  filters: LeaderFilters;
  newTradeKeys: Set<string>;
  smartRange: string;
  smartCategory: string;
  smartSegment: SmartSegment;
  onFilterChange: React.Dispatch<React.SetStateAction<LeaderFilters>>;
  onSmartRangeChange: (value: string) => void;
  onSmartCategoryChange: (value: string) => void;
  onSmartSegmentChange: (value: SmartSegment) => void;
  onRefreshTrades: () => void;
}) {
  const smartRows = useMemo(() => {
    const term = filters.search.trim().toLowerCase();
    return smartWallets.filter((row) => !term || row.user_address.toLowerCase().includes(term));
  }, [filters.search, smartWallets]);

  return (
    <main className="leader-main">
      <div className="leaderboard-title-row">
        <div className="leader-tabs">
          <button className={tab === "smart" ? "active" : ""} type="button" onClick={() => navigate("#/leaderboard?tab=SmartMoney")}>聪明钱</button>
          <button className={tab === "trades" ? "active" : ""} type="button" onClick={() => navigate("#/leaderboard")}>实时交易</button>
        </div>
        {tab === "smart" ? (
          <div className="list-controls leaderboard-smart-actions">
            <SmartSegmented value={smartSegment} onChange={onSmartSegmentChange} />
            <Segmented value={smartRange} values={["1d", "7d", "30d", "All"]} onChange={onSmartRangeChange} />
            <label className="list-search">
              <Search size={15} />
              <input value={filters.search} onChange={(event) => onFilterChange((current) => ({ ...current, search: event.target.value }))} placeholder="在列表中搜索钱包" />
            </label>
            {smartLoading ? <Loader2 className="spin muted-icon" size={16} /> : null}
          </div>
        ) : (
          <div className="leader-actions">
            <button className="icon-btn" type="button"><Bell size={15} /></button>
            <label className="leader-search">
              <Search size={15} />
              <input value={filters.search} onChange={(event) => onFilterChange((current) => ({ ...current, search: event.target.value }))} placeholder="在列表中搜索钱包/市场" />
            </label>
            <button className="filter-button" type="button" onClick={() => toggleLeaderboardFilter("wallet", open)}><Filter size={15} />过滤</button>
            <button className="filter-button" type="button" onClick={onRefreshTrades}>{tradesLoading ? <Loader2 className="spin" size={15} /> : <RefreshCw size={15} />}刷新</button>
            <span className={liveTradesMeta.stream_connected ? "live-pill" : "live-pill stale"}>{tradeStreamStatusLabel(liveTradesMeta)}</span>
          </div>
        )}
      </div>
      {tab === "smart" ? (
        <>
          <CategoryNav active={smartCategory} onChange={onSmartCategoryChange} />
          <SmartTable rows={smartRows} />
        </>
      ) : (
        <>
          <TradeFilters filters={filters} open={open} onChange={onFilterChange} />
          <TradeTable rows={recentTrades} newTradeKeys={newTradeKeys} />
        </>
      )}
    </main>
  );
}

function SmartSegmented({ value, onChange }: { value: SmartSegment; onChange: (value: SmartSegment) => void }) {
  return (
    <div className="segmented smart-segmented">
      {SMART_SEGMENTS.map((item) => (
        <button className={value === item ? "active" : ""} type="button" key={item} onClick={() => onChange(item)}>
          {smartSegmentLabel(item)}
        </button>
      ))}
    </div>
  );
}

function TradeFilters({
  filters,
  open,
  onChange,
}: {
  filters: LeaderFilters;
  open?: "wallet" | "type" | "category";
  onChange: React.Dispatch<React.SetStateAction<LeaderFilters>>;
}) {
  const selectWalletType = (walletType: LeaderFilters["walletType"]) => {
    onChange((current) => ({ ...current, walletType }));
    closeLeaderboardFilters();
  };
  const selectSide = (side: LeaderFilters["side"]) => {
    onChange((current) => ({ ...current, side }));
    closeLeaderboardFilters();
  };
  const selectCategory = (category: string) => {
    onChange((current) => ({ ...current, category }));
    closeLeaderboardFilters();
  };

  return (
    <div className="trade-filters">
      <FilterSelect label="钱包类型" value={walletTypeLabel(filters.walletType)} target="wallet" open={open}>
        {[
          ["all", "全部"],
          ["new", "新钱包"],
          ["smart", "聪明钱"],
          ["whale", "鲸鱼"],
        ].map(([value, label]) => (
          <button className="check-item" type="button" key={value} onClick={() => selectWalletType(value as LeaderFilters["walletType"])}>
            <span className={filters.walletType === value ? "fake-check checked" : "fake-check"}>{filters.walletType === value ? "✓" : ""}</span><span>{label}</span>
          </button>
        ))}
      </FilterSelect>
      <FilterSelect label="类型" value={sideLabel(filters.side)} target="type" open={open}>
        {[
          ["all", "全部"],
          ["BUY", "买入"],
          ["SELL", "卖出"],
        ].map(([value, label]) => (
          <button className="check-item" type="button" key={value} onClick={() => selectSide(value as LeaderFilters["side"])}>
            <span className={filters.side === value ? "fake-check checked" : "fake-check"}>{filters.side === value ? "✓" : ""}</span><span>{label}</span>
          </button>
        ))}
      </FilterSelect>
      <FilterSelect label="类别" value={categoryLabel(filters.category)} target="category" open={open}>
        {["all", "Sports", "Politics", "Crypto", "Esports", "Finance", "Culture", "Weather"].map((category) => (
          <button className="check-item" type="button" key={category} onClick={() => selectCategory(category)}>
            <span className={filters.category === category ? "fake-check checked" : "fake-check"}>{filters.category === category ? "✓" : ""}</span><span>{category === "all" ? "全部" : category}</span>
          </button>
        ))}
      </FilterSelect>
      <div className="filter-group amount">
        <div className="filter-label">金额</div>
        <div className="range-row">
          <input value={filters.minNotional} onChange={(event) => onChange((current) => ({ ...current, minNotional: event.target.value }))} placeholder="最小" />
          <span>至</span>
          <input value={filters.maxNotional} onChange={(event) => onChange((current) => ({ ...current, maxNotional: event.target.value }))} placeholder="最大" />
        </div>
      </div>
    </div>
  );
}

function FilterSelect({
  label,
  value,
  target,
  open,
  children,
}: {
  label: string;
  value: string;
  target: "wallet" | "type" | "category";
  open?: "wallet" | "type" | "category";
  children: React.ReactNode;
}) {
  const groupRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open !== target) return;
    const onPointerDown = (event: MouseEvent | TouchEvent) => {
      const node = event.target as Node | null;
      if (node && groupRef.current?.contains(node)) return;
      closeLeaderboardFilters();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeLeaderboardFilters();
    };
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("touchstart", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("touchstart", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open, target]);

  return (
    <div className="filter-group" ref={groupRef}>
      <div className="filter-label">{label}</div>
      <button className={open === target ? "filter-select open" : "filter-select"} type="button" onClick={() => toggleLeaderboardFilter(target, open)}>
        <span>{value}</span><ChevronDown size={13} />
      </button>
      {open === target ? <div className={`leader-dropdown ${target}-dropdown`}>{children}</div> : null}
    </div>
  );
}

function TradeTable({ rows, newTradeKeys }: { rows: RecentTrade[]; newTradeKeys: Set<string> }) {
  return (
    <div className="table-wrap">
      <table className="leader-table trade-table">
        <colgroup>
          <col style={{ width: "7%" }} /><col style={{ width: "8%" }} /><col style={{ width: "12%" }} /><col style={{ width: "8.5%" }} /><col style={{ width: "8.5%" }} /><col style={{ width: "30%" }} /><col style={{ width: "8.5%" }} /><col style={{ width: "8.5%" }} /><col style={{ width: "9%" }} />
        </colgroup>
        <thead><tr><th>时间 ⓘ</th><th>类型</th><th>交易员</th><th>金额</th><th>份额</th><th>市场</th><th>结果</th><th>价格</th><th className="right">操作</th></tr></thead>
        <tbody>
          {rows.map((trade, index) => {
            const side = String(trade.side || "").toUpperCase();
            const address = normalizeAddress(trade.user_address || "");
            const rowKey = tradeCacheKey(trade);
            return (
              <tr key={rowKey} data-row-key={rowKey} className={newTradeKeys.has(rowKey) ? "trade-row trade-row-new" : "trade-row"}>
                <td className="muted">{formatRelativeTime(trade.timestamp)}</td>
                <td><span className={`trade-type ${side === "SELL" ? "sell" : "buy"}`}>{side === "SELL" ? "卖出" : "买入"}</span></td>
                <td><button className="trader-cell" type="button" onClick={() => navigateAddress(address)}>{walletBadgeColor(index)}<strong>{displayTraderName(trade)}</strong><span className="tag-icons">{trade.is_whale ? "💰" : ""}{trade.is_smart ? " ✽" : ""}</span></button></td>
                <td className={side === "SELL" ? "negative" : "positive"}>{formatCurrency(trade.notional)}</td>
                <td>{formatCompactNumber(trade.size)}</td>
                <td><div className="market leader-market"><strong>{trade.question || trade.event_title || "--"}</strong><span className="muted">{trade.event_title && trade.question !== trade.event_title ? `/${trade.event_title}` : ""}</span><span className="category-muted">{trade.category || "--"}</span></div></td>
                <td>{resultPill(trade.outcome)}</td>
                <td className="positive">{formatPrice(trade.price)}</td>
                <td className="right"><button className="view-btn" type="button" onClick={() => navigateAddress(address)}>查看</button></td>
              </tr>
            );
          })}
          {!rows.length ? <tr><td colSpan={9}><div className="empty-state">暂无实时交易。</div></td></tr> : null}
        </tbody>
      </table>
    </div>
  );
}

function OfficialTradesPage({
  rows,
  totalRows,
  meta,
  filters,
  open,
  trackedWalletCount,
  newTradeKeys,
  onFilterChange,
}: {
  rows: RecentTrade[];
  totalRows: number;
  meta: OfficialTradeFeedMeta;
  filters: LeaderFilters;
  open?: "wallet" | "type" | "category";
  trackedWalletCount: number;
  newTradeKeys: Set<string>;
  onFilterChange: React.Dispatch<React.SetStateAction<LeaderFilters>>;
}) {
  return (
    <main className="leader-main official-feed-main">
      <div className="leaderboard-title-row">
        <div className="official-feed-title">
          <h1>Polymarket Trade Feed</h1>
          <span>activity / trades</span>
        </div>
        <div className="leader-actions">
          <label className="leader-search">
            <Search size={15} />
            <input value={filters.search} onChange={(event) => onFilterChange((current) => ({ ...current, search: event.target.value }))} placeholder="在列表中搜索钱包/市场" />
          </label>
          <button className="filter-button" type="button" onClick={() => toggleLeaderboardFilter("wallet", open)}><Filter size={15} />过滤</button>
          <span className={meta.connected && meta.upstream_connected ? "live-pill" : "live-pill stale"}>{officialFeedStatusLabel(meta)}</span>
          <span className="small-muted">最新 {formatRelativeTime(meta.last_trade_at)}</span>
          <span className="small-muted">{rows.length}/{totalRows} 条</span>
          {filters.walletType !== "all" ? <span className="small-muted">追踪 {trackedWalletCount}</span> : null}
        </div>
      </div>
      <TradeFilters filters={filters} open={open} onChange={onFilterChange} />
      <OfficialTradeTable rows={rows} newTradeKeys={newTradeKeys} />
    </main>
  );
}

function OfficialTradeTable({ rows, newTradeKeys }: { rows: RecentTrade[]; newTradeKeys: Set<string> }) {
  return (
    <div className="table-wrap">
      <table className="leader-table official-trade-table">
        <colgroup>
          <col style={{ width: "7%" }} /><col style={{ width: "8%" }} /><col style={{ width: "8%" }} /><col style={{ width: "12%" }} /><col style={{ width: "8.5%" }} /><col style={{ width: "8.5%" }} /><col style={{ width: "28%" }} /><col style={{ width: "8%" }} /><col style={{ width: "7%" }} /><col style={{ width: "13%" }} />
        </colgroup>
        <thead><tr><th>到达</th><th>成交</th><th>类型</th><th>交易员</th><th>金额</th><th>份额</th><th>市场</th><th>结果</th><th>价格</th><th>交易</th></tr></thead>
        <tbody>
          {rows.map((trade, index) => {
            const side = String(trade.side || "").toUpperCase();
            const address = normalizeAddress(trade.user_address || "");
            const rowKey = tradeCacheKey(trade);
            return (
              <tr key={rowKey} className={newTradeKeys.has(rowKey) ? "trade-row trade-row-new" : "trade-row"}>
                <td className="muted">now</td>
                <td className="muted">{formatRelativeTime(trade.timestamp)}</td>
                <td><span className={`trade-type ${side === "SELL" ? "sell" : "buy"}`}>{side === "SELL" ? "卖出" : "买入"}</span></td>
                <td><button className="trader-cell" type="button" onClick={() => navigateAddress(address)}>{walletBadgeColor(index)}<strong>{displayTraderName(trade)}</strong></button></td>
                <td className={side === "SELL" ? "negative" : "positive"}>{formatCurrency(trade.notional)}</td>
                <td>{formatCompactNumber(trade.size)}</td>
                <td><div className="market leader-market"><strong>{trade.question || "--"}</strong><span className="muted">{trade.market_slug || trade.event_slug || "--"}</span></div></td>
                <td>{resultPill(trade.outcome)}</td>
                <td className="positive">{formatPrice(trade.price)}</td>
                <td><span className="small-muted">{shortAddress(trade.transaction_hash)}</span></td>
              </tr>
            );
          })}
          {!rows.length ? <tr><td colSpan={10}><div className="empty-state">等待 Polymarket 官方实时成交。</div></td></tr> : null}
        </tbody>
      </table>
    </div>
  );
}

function SmartTable({ rows }: { rows: SmartWallet[] }) {
  return (
    <div className="table-wrap">
      <table className="leader-table smart-table">
        <colgroup>
          <col style={{ width: "3%" }} /><col style={{ width: "22%" }} /><col style={{ width: "14%" }} /><col style={{ width: "15%" }} /><col style={{ width: "15%" }} /><col style={{ width: "14%" }} /><col style={{ width: "9%" }} /><col style={{ width: "8%" }} />
        </colgroup>
        <thead><tr><th>#</th><th>钱包</th><th className="sortable">总盈亏</th><th className="sortable">胜率 / 平均投注</th><th className="sortable">现金</th><th className="sortable">交易量 / 交易次数</th><th>最后活跃</th><th className="right">操作</th></tr></thead>
        <tbody>
          {rows.map((row, index) => {
            const pnlValue = smartWalletPnl(row);
            const roiValue = smartWalletRoi(row);
            return (
              <tr key={row.user_address}>
                <td>{medal(index + 1)}</td>
                <td><button className="smart-wallet" type="button" onClick={() => navigateAddress(row.user_address)}>{walletBadgeColor(index, "large-ish")}<div><div className="smart-name">{shortAddress(row.user_address)} <WalletSegmentPill row={row} /></div><div className="wallet-meta">{walletAge(row.first_trade_at)} | {shortAddress(row.user_address)} <Copy size={12} /></div></div></button></td>
                <td><div className="cell-stack"><strong className={tone(pnlValue)}>{formatSignedCurrency(pnlValue)}</strong><span className={tone(roiValue)}>{formatRatioPercent(roiValue)}</span></div></td>
                <td><div className="cell-stack"><strong>{formatRatioPercent(row.scope === "fifa" ? row.fifa_win_rate : row.win_rate)}</strong><span className="small-muted">{formatCurrency(avgTrade(row))}</span></div></td>
                <td><span className="currency"><span className="money-icon" /><strong className="value-main">{formatCurrency(row.available_balance ?? row.portfolio_value)}</strong></span></td>
                <td><div className="cell-stack"><strong>{formatCurrency(row.traded_notional)}</strong><span className="small-muted">{formatCount(row.trade_count)} ( <span className="positive">{formatCount(row.buy_count)}</span> / <span className="negative">{formatCount(row.sell_count)}</span>)</span></div></td>
                <td><span className="small-muted">{formatRelativeTime(row.last_trade_at || row.updated_at)}</span></td>
                <td className="right"><button className="external" type="button" onClick={() => navigateAddress(row.user_address)}><ExternalLink size={15} /></button></td>
              </tr>
            );
          })}
          {!rows.length ? <tr><td colSpan={8}><div className="empty-state">暂无聪明钱数据。</div></td></tr> : null}
        </tbody>
      </table>
    </div>
  );
}

function WalletSegmentPill({ row }: { row: SmartWallet }) {
  const segment = String(row.wallet_segment || (row.is_smart ? "strict_smart" : row.is_candidate_smart ? "candidate_smart" : row.is_whale ? "whale" : "active"));
  return <span className={`wallet-segment ${segment.replace(/_/g, "-")}`}>{smartSegmentLabel(segment)}</span>;
}

function UnusualBettingPage({
  slug,
  data,
  loading,
  error,
  onRefresh,
}: {
  slug: string;
  data: UnusualBettingDetail | null;
  loading: boolean;
  error: string;
  onRefresh: () => void;
}) {
  const [input, setInput] = useState(slug);
  const analysis = data?.analysis || {};
  const thresholds = analysis.thresholds || {};
  const largeThreshold = Number(thresholds.large_threshold || data?.parameters?.large_threshold || 500_000);
  const signalWallets = data?.signal_wallets || data?.cold_wallets || [];
  const signalTrades = data?.signal_trades || data?.cold_trades || [];
  const abnormalWallets = unusualWalletRows(signalWallets, largeThreshold);
  const abnormalWalletCount = data?.signal_wallet_summary?.abnormal_wallet_count ?? abnormalWallets.length;
  const maxOutcomeNotional = Math.max(
    1,
    ...((data?.outcome_summary || [])
      .filter((row) => String(row.user_side || "").toUpperCase() === "BUY")
      .map((row) => Number(row.total_notional || 0))),
  );

  useEffect(() => {
    setInput(slug);
  }, [slug]);

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    const nextSlug = input.trim();
    if (nextSlug) navigate(`#/unusual-betting?slug=${encodeURIComponent(nextSlug)}`);
  };

  return (
    <main className="analysis-main">
      <div className="analysis-title-row">
        <div>
          <div className="analysis-kicker">异常下注分析</div>
          <h1>{data?.event?.title || slug || "选择比赛"}</h1>
          <div className="profile-meta">
            <span>{data?.event?.slug || slug || "--"}</span>
            <span>{data?.event?.category || "--"}</span>
            <span>更新 {formatDateTime(data?.generated_at)}</span>
          </div>
        </div>
        <form className="analysis-search" onSubmit={submit}>
          <label className="list-search">
            <Search size={15} />
            <input value={input} onChange={(event) => setInput(event.target.value)} placeholder="输入 event slug" />
          </label>
          <button className="action-button" type="submit"><LineChart size={14} />分析</button>
          <button className="action-button" type="button" onClick={onRefresh} disabled={!slug || loading}>{loading ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />}刷新</button>
          {slug ? <a className="filter-button" href={`${API_BASE}/events/unusual-betting?slug=${encodeURIComponent(slug)}&wallet_limit=100&trade_limit=100`} target="_blank" rel="noreferrer"><ExternalLink size={14} />完整数据</a> : null}
        </form>
      </div>

      {error ? <Notice message={error} /> : null}
      {loading && !data ? <div className="loading-panel"><Loader2 className="spin" size={18} /> 正在生成异常下注证据...</div> : null}
      {!slug && !data ? (
        <div className="analysis-empty">
          <button type="button" onClick={() => navigate("#/unusual-betting?slug=fifwc-fra-sen-2026-06-16")}>fifwc-fra-sen-2026-06-16</button>
          <button type="button" onClick={() => navigate("#/unusual-betting?slug=fifwc-irq-nor-2026-06-16")}>fifwc-irq-nor-2026-06-16</button>
          <button type="button" onClick={() => navigate("#/unusual-betting?slug=fifwc-arg-alg-2026-06-16")}>fifwc-arg-alg-2026-06-16</button>
        </div>
      ) : null}

      {data ? (
        <>
          <section className="analysis-summary">
            <div className={`severity-pill ${analysis.severity || "none"}`}>{severityLabel(analysis.severity)}</div>
            <p>{analysis.conclusion || "暂无结论"}</p>
          </section>

          <section className="analysis-metrics">
            <MetricTile label="异常钱包" value={formatCount(abnormalWalletCount)} />
            <MetricTile label="最大钱包累计" value={formatCurrency(analysis.max_signal_wallet_notional ?? analysis.max_cold_wallet_notional)} />
            <MetricTile label="异常方向成交" value={formatCurrency(analysis.signal_total_notional ?? analysis.cold_buy_total_notional)} />
            <MetricTile label="信号方向" value={formatCount(analysis.signal_outcome_count ?? analysis.cold_buy_outcome_count)} />
            <MetricTile label="证据钱包" value={formatCount(data.signal_wallet_summary?.signal_wallet_count)} />
          </section>

          <div className="analysis-grid">
            <section className="analysis-card outcome-card">
              <div className="analysis-card-head">
                <div>
                  <h2>方向成交</h2>
                  <span>按用户侧 BUY/SELL 展开，异常方向高亮</span>
                </div>
                <span className="small-muted">低价 {"<="} {formatPrice(thresholds.cold_price_threshold || data.parameters?.cold_price_threshold)}</span>
              </div>
              <div className="evidence-bars">
                {(data.outcome_summary || [])
                  .sort((a, b) => Number(b.total_notional || 0) - Number(a.total_notional || 0))
                  .slice(0, 12)
                  .map((row) => (
                    <OutcomeBar key={`${row.market_slug}-${row.outcome}-${row.user_side}`} row={row} max={maxOutcomeNotional} threshold={Number(thresholds.cold_price_threshold || 0.25)} />
                  ))}
              </div>
            </section>

            <section className="analysis-card">
              <div className="analysis-card-head">
                <div>
                  <h2>异常钱包</h2>
                  <span>按钱包累计成交额排序</span>
                </div>
              </div>
              <WalletEvidenceTable rows={signalWallets.slice(0, 10)} largeThreshold={largeThreshold} />
            </section>
          </div>

          <section className="analysis-card">
            <div className="analysis-card-head">
              <div>
                <h2>大额成交证据</h2>
                <span>异常方向的最大成交明细</span>
              </div>
            </div>
            <TradeEvidenceTable rows={signalTrades.slice(0, 20)} largeThreshold={largeThreshold} />
          </section>
        </>
      ) : null}
    </main>
  );
}

function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-tile">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function OutcomeBar({ row, max, threshold }: { row: UnusualOutcome; max: number; threshold: number }) {
  const total = Number(row.total_notional || 0);
  const avgPrice = userSidePrice(row.avg_price, row.user_side);
  const isSignal = Boolean(row.signal_type) || (avgPrice > 0 && avgPrice <= threshold);
  return (
    <div className="evidence-row">
      <div className="evidence-label">
        <strong>{row.question || row.market_slug || "--"}</strong>
        <span>{sideLabelText(row.user_side)} {row.outcome || "--"} · {formatPrice(avgPrice)} · {formatCount(row.wallet_count)} 钱包</span>
      </div>
      <div className="bar-track">
        <div className={isSignal ? "bar-fill cold" : "bar-fill"} style={{ width: `${Math.max(3, Math.min(100, (total / max) * 100))}%` }} />
      </div>
      <div className="evidence-value">{formatCurrency(total)}</div>
    </div>
  );
}

function WalletEvidenceTable({ rows, largeThreshold }: { rows: UnusualWallet[]; largeThreshold: number }) {
  return (
    <div className="table-wrap">
      <table className="analysis-table">
        <thead><tr><th>钱包</th><th>方向</th><th>累计</th><th>均价</th><th>最后时间</th></tr></thead>
        <tbody>
          {rows.map((row) => {
            const isLarge = Number(row.total_notional || 0) >= largeThreshold;
            return (
              <tr key={`${row.user_address}-${row.market_slug}-${row.outcome}`}>
                <td><button className="plain-link" type="button" onClick={() => navigateAddress(row.user_address || "")}>{shortAddress(row.user_address)}</button></td>
                <td><div className="cell-stack"><strong>{sideLabelText(row.user_side)} {row.outcome || "--"}</strong><span className="small-muted">{row.question || row.market_slug || "--"}</span></div></td>
                <td className={isLarge ? "orange" : ""}>{formatCurrency(row.total_notional)}</td>
                <td>{formatPrice(userSidePrice(row.avg_price, row.user_side))}</td>
                <td>{formatDateTime(row.last_ts)}</td>
              </tr>
            );
          })}
          {!rows.length ? <tr><td colSpan={5}><div className="empty-state">暂无异常钱包。</div></td></tr> : null}
        </tbody>
      </table>
    </div>
  );
}

function TradeEvidenceTable({ rows, largeThreshold }: { rows: UnusualTrade[]; largeThreshold: number }) {
  return (
    <div className="table-wrap">
      <table className="analysis-table trade-evidence-table">
        <thead><tr><th>时间</th><th>钱包</th><th>金额</th><th>份额</th><th>市场</th><th>结果</th><th>价格</th><th>交易</th></tr></thead>
        <tbody>
          {rows.map((row, index) => {
            const notional = Number(row.notional || 0);
            return (
              <tr key={`${row.transaction_hash}-${row.user_address}-${index}`}>
                <td>{formatDateTime(row.timestamp)}</td>
                <td><button className="plain-link" type="button" onClick={() => navigateAddress(row.user_address || "")}>{shortAddress(row.user_address)}</button></td>
                <td className={notional >= largeThreshold ? "orange" : ""}>{formatCurrency(notional)}</td>
                <td>{formatCompactNumber(row.size)}</td>
                <td><div className="market leader-market"><strong>{row.question || row.market_slug || "--"}</strong><span className="muted">{row.market_slug || "--"}</span></div></td>
                <td>{sideLabelText(row.user_side)} {resultPill(row.outcome)}</td>
                <td>{formatPrice(userSidePrice(row.price, row.user_side))}</td>
                <td>{row.transaction_hash ? shortAddress(row.transaction_hash) : "--"}</td>
              </tr>
            );
          })}
          {!rows.length ? <tr><td colSpan={8}><div className="empty-state">暂无大额异常成交。</div></td></tr> : null}
        </tbody>
      </table>
    </div>
  );
}

function AddWalletModal({
  address,
  name,
  loading,
  onAddressChange,
  onNameChange,
  onSubmit,
  onClose,
}: {
  address: string;
  name: string;
  loading: boolean;
  onAddressChange: (value: string) => void;
  onNameChange: (value: string) => void;
  onSubmit: (event: React.FormEvent) => void;
  onClose: () => void;
}) {
  return (
    <div className="overlay" onMouseDown={onClose}>
      <form className="modal" role="dialog" aria-modal="true" aria-label="添加钱包" onSubmit={onSubmit} onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-head">
          <div className="modal-title">添加钱包</div>
          <button className="close-btn" type="button" onClick={onClose}><X size={17} /></button>
        </div>
        <div className="modal-body">
          <input className="modal-input" value={address} onChange={(event) => onAddressChange(event.target.value)} placeholder="钱包地址" autoFocus />
          <div className="input-row">
            <button className="scan-button" type="button"><Search size={15} /></button>
            <input className="modal-input" value={name} onChange={(event) => onNameChange(event.target.value)} placeholder="钱包名称" />
          </div>
        </div>
        <div className="modal-foot">
          <button className="primary-wide" type="submit" disabled={loading}>{loading ? <Loader2 className="spin" size={15} /> : <Plus size={15} />}添加钱包</button>
        </div>
      </form>
    </div>
  );
}

function Segmented({ value, values, onChange }: { value: string; values: string[]; onChange: (value: string) => void }) {
  return (
    <div className="segmented">
      {values.map((item) => (
        <button className={value === item ? "active" : ""} type="button" key={item} onClick={() => onChange(item)}>{item}</button>
      ))}
    </div>
  );
}

function MetricRow({ label, value, tone: valueTone = "" }: { label: string; value: string; tone?: string }) {
  return <div className="metric-row"><span>{label} <span className="muted">ⓘ</span></span><strong className={valueTone}>{value}</strong></div>;
}

function DealCard({ label, position }: { label: string; position: WalletPosition | null }) {
  const pnl = position?.cash_pnl;
  return (
    <div className="deal-card">
      <div className="deal-label">{label}</div>
      <div className="deal-title">{position?.title || "--"}</div>
      <div className="deal-bottom"><span className={tone(pnl)}>{formatSignedCurrency(pnl)}</span><span className={tone(pnl)}>{formatPercentValue(position?.percent_pnl)}</span></div>
    </div>
  );
}

function WalletAvatar({ size = "" }: { size?: "large" | "" }) {
  return <span className={`avatar ${size}`}><span className="cat-icon" /></span>;
}

function CategoryNav({ active, onChange }: { active: string; onChange: (value: string) => void }) {
  return <div className="category-nav">{SMART_CATEGORIES.map((cat) => <button className={active === cat ? "active" : ""} type="button" key={cat} onClick={() => onChange(cat)}>{cat}</button>)}</div>;
}

function ProfitCell({ value }: { value: unknown }) {
  return <div className="cell-stack"><strong className={tone(value)}>{formatSignedCurrency(value)}</strong><span className={tone(value)}>{formatPnlShortPercent(value)}</span></div>;
}

function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  return fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  }).then(async (response) => {
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(typeof data?.error === "string" ? data.error : `HTTP ${response.status}`);
    }
    return data as T;
  });
}

function parseRoute(): Route {
  const raw = (window.location.hash || "#/track").replace(/^#/, "");
  const [path, query = ""] = raw.split("?");
  const params = new URLSearchParams(query);
  if (path.startsWith("/address/")) {
    return { name: "address", wallet: normalizeAddress(decodeURIComponent(path.replace("/address/", ""))) };
  }
  if (path.startsWith("/leaderboard")) {
    const tab = params.get("tab") === "SmartMoney" || params.get("tab") === "smart" ? "smart" : "trades";
    const open = params.get("open");
    return {
      name: "leaderboard",
      tab,
      open: open === "wallet" || open === "type" || open === "category" ? open : undefined,
    };
  }
  if (path.startsWith("/official-trades")) {
    const open = params.get("open");
    return {
      name: "official-trades",
      open: open === "wallet" || open === "type" || open === "category" ? open : undefined,
    };
  }
  if (path.startsWith("/unusual-betting")) {
    return { name: "unusual-betting", slug: params.get("slug") || "" };
  }
  return { name: "track", modal: params.get("modal") === "add" ? "add" : undefined };
}

function navigate(hash: string) {
  window.location.hash = hash;
}

function toggleLeaderboardFilter(target: "wallet" | "type" | "category", open?: "wallet" | "type" | "category") {
  const route = parseRoute();
  if (route.name === "official-trades") {
    navigate(open === target ? "#/official-trades" : `#/official-trades?open=${target}`);
    return;
  }
  navigate(open === target ? "#/leaderboard" : `#/leaderboard?open=${target}`);
}

function closeLeaderboardFilters() {
  const route = parseRoute();
  if (route.name === "leaderboard") {
    navigate(route.tab === "smart" ? "#/leaderboard?tab=SmartMoney" : "#/leaderboard");
  } else if (route.name === "official-trades") {
    navigate("#/official-trades");
  }
}

function navigateAddress(address: string) {
  const normalized = normalizeAddress(address);
  if (normalized) navigate(`#/address/${normalized}`);
}

function normalizeAddress(value: string) {
  const address = String(value || "").trim().toLowerCase();
  return /^0x[a-f0-9]{40}$/.test(address) ? address : address;
}

function walletAddress(wallet: ApiTrackedWallet) {
  return normalizeAddress(wallet.user_address || wallet.address || "");
}

function walletByAddress(wallets: ApiTrackedWallet[], address: string) {
  const normalized = normalizeAddress(address);
  return wallets.find((wallet) => walletAddress(wallet) === normalized) || null;
}

function upsertTrackedWallet(wallets: ApiTrackedWallet[], wallet: ApiTrackedWallet) {
  const address = walletAddress(wallet);
  const next = wallets.filter((item) => walletAddress(item) !== address);
  return [wallet, ...next];
}

function summaryFromDetail(detail: WalletDetail): WalletSummary {
  const reputation = detail.reputation || {};
  const risk = detail.risk_metrics || {};
  return {
    ...detail.wallet,
    favorite_category: reputation.favorite_category || detail.wallet.favorite_category || null,
    realized_pnl: numberOrNull(risk.realized_pnl ?? reputation.realized_pnl ?? detail.wallet.realized_pnl),
    completed_event_count: numberOrNull(risk.completed_event_count ?? reputation.completed_event_count ?? detail.wallet.completed_event_count),
    profitable_event_count: numberOrNull(risk.profitable_event_count ?? reputation.profitable_event_count ?? detail.wallet.profitable_event_count),
    losing_event_count: numberOrNull(risk.losing_event_count ?? reputation.losing_event_count ?? detail.wallet.losing_event_count),
    win_rate: numberOrNull(risk.win_rate ?? reputation.win_rate ?? detail.wallet.win_rate),
    avg_event_roi: numberOrNull(risk.avg_event_roi ?? reputation.avg_event_roi ?? detail.wallet.avg_event_roi),
    first_trade_at: reputation.first_trade_at || detail.wallet.first_trade_at || detail.wallet.first_activity_at || null,
    last_trade_at: reputation.last_trade_at || detail.wallet.last_trade_at || detail.wallet.last_activity_at || null,
  };
}

function loadSummaryCache() {
  try {
    return JSON.parse(window.localStorage.getItem(SUMMARY_CACHE_KEY) || "{}") as Record<string, WalletSummary>;
  } catch {
    return {};
  }
}

function saveSummaryCache(cache: Record<string, WalletSummary>) {
  try {
    window.localStorage.setItem(SUMMARY_CACHE_KEY, JSON.stringify(cache));
  } catch {
    // localStorage can be unavailable in private contexts.
  }
}

function loadLiveTradesCache() {
  try {
    const rows = JSON.parse(window.localStorage.getItem(LIVE_TRADES_CACHE_KEY) || "[]") as RecentTrade[];
    return mergeTradeRows([], Array.isArray(rows) ? rows : [], LIVE_TRADES_CACHE_LIMIT);
  } catch {
    return [];
  }
}

function saveLiveTradesCache(rows: RecentTrade[]) {
  try {
    window.localStorage.setItem(LIVE_TRADES_CACHE_KEY, JSON.stringify(rows.slice(0, LIVE_TRADES_CACHE_LIMIT)));
  } catch {
    // localStorage can be unavailable or full.
  }
}

function loadOfficialTradeSettings() {
  try {
    const saved = JSON.parse(window.localStorage.getItem(OFFICIAL_TRADE_SETTINGS_KEY) || "{}") as Partial<OfficialTradeFeedSettings>;
    return { ...DEFAULT_OFFICIAL_TRADE_SETTINGS, ...saved };
  } catch {
    return DEFAULT_OFFICIAL_TRADE_SETTINGS;
  }
}

function saveOfficialTradeSettings(settings: OfficialTradeFeedSettings) {
  try {
    window.localStorage.setItem(OFFICIAL_TRADE_SETTINGS_KEY, JSON.stringify(settings));
  } catch {
    // localStorage can be unavailable or full.
  }
}

function tradeStreamUrl() {
  if (/^wss?:\/\//i.test(TRADE_STREAM_PATH)) return TRADE_STREAM_PATH;
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const path = TRADE_STREAM_PATH.startsWith("/") ? TRADE_STREAM_PATH : `/${TRADE_STREAM_PATH}`;
  return `${protocol}//${window.location.host}${path}`;
}

function officialTradeFeedUrl() {
  if (/^wss?:\/\//i.test(OFFICIAL_TRADE_FEED_PATH)) return OFFICIAL_TRADE_FEED_PATH;
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const path = OFFICIAL_TRADE_FEED_PATH.startsWith("/") ? OFFICIAL_TRADE_FEED_PATH : `/${OFFICIAL_TRADE_FEED_PATH}`;
  return `${protocol}//${window.location.host}${path}`;
}

function parseTradeStreamMessage(value: string) {
  try {
    const message = JSON.parse(value) as TradeStreamMessage;
    return message && typeof message === "object" ? message : null;
  } catch {
    return null;
  }
}

function tradeFromStreamMessage(message: TradeStreamMessage) {
  if (message.trade && typeof message.trade === "object") {
    return message.trade;
  }
  const price = numberOrUndefined(message.price);
  const size = numberOrUndefined(message.size);
  const notional = numberOrUndefined(message.notional) ?? (Number.isFinite(price || NaN) && Number.isFinite(size || NaN) ? Number(price) * Number(size) : undefined);
  const userAddress = normalizeAddress(String(message.user_address || message.maker_address || ""));
  if (!message.timestamp || !userAddress) return null;
  return {
    trade_id: message.trade_id,
    transaction_hash: message.transaction_hash || message.hash,
    timestamp: message.timestamp,
    market_id: message.market_id,
    condition_id: message.condition_id,
    token_id: message.token_id || message.asset_id,
    user_address: userAddress,
    side: String(message.side || "").toUpperCase(),
    price,
    size,
    notional,
    question: message.question,
    market_slug: message.slug || message.market,
    event_title: message.event_title,
    event_slug: message.event_slug,
    category: message.category,
    outcome: message.outcome,
  } satisfies RecentTrade;
}

function mergeTradeRows(existing: RecentTrade[], incoming: RecentTrade[], limit: number) {
  const byKey = new Map<string, RecentTrade>();
  for (const row of [...existing, ...incoming]) {
    const key = tradeCacheKey(row);
    const current = byKey.get(key);
    if (!current || tradeTimestamp(row) >= tradeTimestamp(current)) {
      byKey.set(key, mergeTradeRow(current, row));
    }
  }
  return [...byKey.values()]
    .sort((a, b) => tradeTimestamp(b) - tradeTimestamp(a))
    .slice(0, limit);
}

function mergeTradeRow(existing: RecentTrade | undefined, incoming: RecentTrade) {
  if (!existing) return incoming;
  const next = { ...existing, ...incoming };
  for (const key of ["question", "outcome", "market_slug", "event_slug", "event_title", "category"] as const) {
    if (isUsefulTradeText(incoming[key], existing[key])) {
      next[key] = incoming[key];
    } else {
      next[key] = existing[key];
    }
  }
  return next;
}

function isUsefulTradeText(incoming: string | undefined, existing: string | undefined) {
  const newValue = String(incoming || "").trim();
  const oldValue = String(existing || "").trim();
  if (!newValue) return false;
  if (!oldValue) return true;
  if (oldValue.startsWith("Token ") && !newValue.startsWith("Token ")) return true;
  return oldValue === newValue;
}

function tradeCacheKey(row: RecentTrade) {
  if (row.trade_id) return `id:${row.trade_id}`;
  const tx = String(row.transaction_hash || "");
  if (tx) {
    return [
      "tx",
      tx,
      row.token_id || "",
      row.user_address || "",
      row.side || "",
      row.price || "",
      row.size || "",
    ].join("|");
  }
  return [
    "row",
    row.timestamp || "",
    row.user_address || "",
    row.condition_id || "",
    row.token_id || "",
    row.side || "",
    row.price || "",
    row.size || "",
  ].join("|");
}

function tradeTimestamp(row: RecentTrade) {
  const time = parseApiDate(row.timestamp).getTime();
  return Number.isFinite(time) ? time : 0;
}

function buildHeatCells(points: Array<{ timestamp?: number; datetime?: string | null; pnl?: number }>) {
  const sorted = [...points]
    .filter((point) => Number.isFinite(Number(point.pnl)))
    .sort((a, b) => pointTime(a) - pointTime(b));
  const cells: Array<{ label: string; cls: string }> = [];
  for (let index = Math.max(0, sorted.length - 28); index < sorted.length; index += 1) {
    const current = sorted[index];
    const previous = sorted[index - 1];
    const delta = Number(current.pnl || 0) - Number(previous?.pnl ?? current.pnl ?? 0);
    cells.push({
      label: Math.abs(delta) < 0.01 ? "" : formatSignedCurrency(delta),
      cls: delta > 0 ? "green" : delta < 0 ? "red" : "dim",
    });
  }
  while (cells.length < 28) cells.unshift({ label: "", cls: "dim" });
  return cells.slice(-28);
}

function pointTime(point: { timestamp?: number; datetime?: string | null }) {
  if (point.timestamp) {
    return Number(point.timestamp) < 10_000_000_000 ? Number(point.timestamp) * 1000 : Number(point.timestamp);
  }
  return parseApiDate(point.datetime).getTime();
}

function computeMaxDrawdown(points: Array<{ pnl?: number }>) {
  let peak = Number.NEGATIVE_INFINITY;
  let drawdown = 0;
  for (const point of points) {
    const value = Number(point.pnl);
    if (!Number.isFinite(value)) continue;
    peak = Math.max(peak, value);
    drawdown = Math.min(drawdown, value - peak);
  }
  return drawdown === 0 ? null : Math.abs(drawdown);
}

function uniqueRecentMarkets(rows: WalletActivity[]) {
  const since = Date.now() - 7 * 86400 * 1000;
  const markets = new Set<string>();
  for (const row of rows) {
    const time = parseApiDate(row.timestamp).getTime();
    if (Number.isFinite(time) && time >= since && row.condition_id) markets.add(row.condition_id);
  }
  return markets.size || null;
}

function bestPosition(positions: WalletPosition[]) {
  return positions.filter((position) => Number.isFinite(Number(position.cash_pnl))).sort((a, b) => Number(b.cash_pnl || 0) - Number(a.cash_pnl || 0))[0] || null;
}

function worstPosition(positions: WalletPosition[]) {
  return positions.filter((position) => Number.isFinite(Number(position.cash_pnl))).sort((a, b) => Number(a.cash_pnl || 0) - Number(b.cash_pnl || 0))[0] || null;
}

function avgTrade(row: SmartWallet) {
  if (!isMissing(row.avg_bet)) return Number(row.avg_bet);
  const notional = Number(row.traded_notional || 0);
  const count = Number(row.trade_count || 0);
  return count > 0 ? notional / count : null;
}

function smartWalletPnl(row: SmartWallet) {
  return row.scope === "fifa" ? firstNumber(row.fifa_total_pnl, row.total_pnl) : row.total_pnl;
}

function smartWalletRoi(row: SmartWallet) {
  return row.scope === "fifa" ? firstNumber(row.fifa_total_pnl_roi, row.pnl_roi) : row.pnl_roi;
}

function unusualWalletRows(rows: UnusualWallet[], largeThreshold: number) {
  return rows.filter((row) => Number(row.total_notional || 0) >= largeThreshold);
}

function userSidePrice(value: unknown, side: string | undefined) {
  const price = Number(value || 0);
  if (!Number.isFinite(price)) return 0;
  if (String(side || "").toUpperCase() === "SELL") return Math.max(0, Math.min(1, 1 - price));
  return price;
}

function sideLabelText(value: string | undefined) {
  const side = String(value || "").toUpperCase();
  if (side === "SELL") return "卖出";
  if (side === "BUY") return "买入";
  return side || "--";
}

function severityLabel(value: string | undefined) {
  return (
    {
      critical: "严重",
      high: "高风险",
      medium: "异常",
      low: "关注",
      none: "正常",
    } as Record<string, string>
  )[String(value || "none")] || "关注";
}

function medal(rank: number) {
  if (rank === 1) return <span className="medal gold">1</span>;
  if (rank === 2) return <span className="medal silver">2</span>;
  if (rank === 3) return <span className="medal bronze">3</span>;
  return <span className="rank-number">{rank}</span>;
}

function resultPill(result: string | undefined) {
  if (!result) return <span className="muted">--</span>;
  return <span className={result === "No" ? "result-pill no" : "result-pill"}>{result}</span>;
}

function walletBadgeColor(index: number, size = "") {
  const colors = ["#9a44dc", "#83b1e8", "#d5b283", "#c74b2f", "#63d58a", "#b45d20", "#87d06b", "#bb674a"];
  return <span className={`rank-avatar ${size}`} style={{ "--avatar-color": colors[index % colors.length] } as React.CSSProperties}><span className="cat-icon" /></span>;
}

function walletTypeLabel(value: LeaderFilters["walletType"]) {
  return ({ all: "全部", new: "新钱包", smart: "聪明钱", whale: "鲸鱼" } as const)[value];
}

function smartSegmentLabel(value: string) {
  return (
    {
      strict_smart: "严格",
      smart: "严格",
      candidate_smart: "候选",
      whale: "鲸鱼",
      watch: "观察",
      recent_flow: "近期",
      active: "活跃",
    } as Record<string, string>
  )[value] || value;
}

function sideLabel(value: LeaderFilters["side"]) {
  return value === "BUY" ? "买入" : value === "SELL" ? "卖出" : "全部";
}

function categoryLabel(value: string) {
  return value === "all" ? "全部" : value;
}

function filterTradesClientSide(rows: RecentTrade[], filters: LeaderFilters) {
  const search = filters.search.trim().toLowerCase();
  const minNotional = Number(filters.minNotional);
  const maxNotional = Number(filters.maxNotional);
  return rows.filter((row) => {
    const side = String(row.side || "").toUpperCase();
    if (filters.side !== "all" && side !== filters.side) return false;
    const notional = Number(row.notional || 0);
    if (Number.isFinite(minNotional) && minNotional > 0 && notional < minNotional) return false;
    if (Number.isFinite(maxNotional) && maxNotional > 0 && notional > maxNotional) return false;
    if (!tradeMatchesCategory(row, filters.category)) return false;
    if (!search) return true;
    const haystack = [
      row.user_address,
      row.trader_name,
      row.trader_pseudonym,
      row.question,
      row.event_title,
      row.market_slug,
      row.event_slug,
      row.outcome,
    ].join(" ").toLowerCase();
    return haystack.includes(search);
  });
}

function filterOfficialTrades(rows: RecentTrade[], filters: LeaderFilters, trackedWallets: Set<string>) {
  return rows.filter((row) => officialTradeMatchesFilters(row, filters, trackedWallets));
}

function officialTradeMatchesFilters(row: RecentTrade, filters: LeaderFilters, trackedWallets: Set<string>) {
  const side = String(row.side || "").toUpperCase();
  if (filters.side !== "all" && side !== filters.side) return false;
  const notional = Number(row.notional || 0);
  const minNotional = Number(filters.minNotional);
  const maxNotional = Number(filters.maxNotional);
  if (Number.isFinite(minNotional) && minNotional > 0 && notional < minNotional) return false;
  if (Number.isFinite(maxNotional) && maxNotional > 0 && notional > maxNotional) return false;
  if (filters.walletType === "smart" && !row.is_smart) return false;
  if (filters.walletType === "whale" && !row.is_whale) return false;
  if (filters.walletType === "new" && trackedWallets.has(normalizeAddress(row.user_address || ""))) return false;
  if (!tradeMatchesCategory(row, filters.category)) return false;
  const search = filters.search.trim().toLowerCase();
  if (search) {
    const haystack = [
      row.user_address,
      row.trader_name,
      row.trader_pseudonym,
      row.question,
      row.event_title,
      row.market_slug,
      row.event_slug,
      row.outcome,
    ].join(" ").toLowerCase();
    if (!haystack.includes(search)) return false;
  }
  return true;
}

function officialGifMinNotional(settings: OfficialTradeFeedSettings) {
  return numberSetting(settings.gifMinNotional || "", Number.POSITIVE_INFINITY);
}

function numberSetting(value: string, fallback: number) {
  const number = Number(String(value || "").replace(/[$,\s]/g, ""));
  return Number.isFinite(number) && number >= 0 ? number : fallback;
}

function tradeMatchesCategory(row: RecentTrade, category: string) {
  const normalized = String(category || "").trim().toLowerCase();
  if (!normalized || normalized === "all" || normalized === "全部") return true;
  const terms = categoryTerms(normalized);
  const haystack = [
    row.category,
    row.question,
    row.event_title,
    row.market_slug,
    row.event_slug,
  ].join(" ").toLowerCase();
  if ((normalized === "sports" || normalized === "体育") && isEsportsHaystack(haystack)) return false;
  return terms.some((term) => haystack.includes(term.toLowerCase()));
}

function isEsportsHaystack(haystack: string) {
  return ["esports", "e-sports", "valorant", "counter-strike", "counter strike", "cs2", "dota"].some((term) => haystack.includes(term));
}

function categoryTerms(category: string) {
  const terms: Record<string, string[]> = {
    sports: ["sports", "sport", "nba", "fifa", "world cup", "soccer", "football", "tennis", "dublin", "halle", "argentina", "france", "portugal"],
    体育: ["sports", "sport", "nba", "fifa", "world cup", "soccer", "football", "tennis", "dublin", "halle", "argentina", "france", "portugal"],
    politics: ["politics", "election", "congress", "senate", "trump", "biden", "referendum", "government", "starmer"],
    政治: ["politics", "election", "congress", "senate", "trump", "biden", "referendum", "government", "starmer"],
    crypto: ["crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "dogecoin"],
    加密货币: ["crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "dogecoin"],
    esports: ["esports", "e-sports", "gaming", "lol", "dota", "valorant", "counter-strike"],
    电竞: ["esports", "e-sports", "gaming", "lol", "dota", "valorant", "counter-strike"],
    finance: ["finance", "business", "stock", "nasdaq", "s&p", "fed", "rates", "treasury"],
    金融: ["finance", "business", "stock", "nasdaq", "s&p", "fed", "rates", "treasury"],
    culture: ["culture", "pop-culture", "music", "movie", "celebrity", "oscars"],
    文化: ["culture", "pop-culture", "music", "movie", "celebrity", "oscars"],
    weather: ["weather", "temperature", "hurricane", "rain", "snow", "storm"],
    天气: ["weather", "temperature", "hurricane", "rain", "snow", "storm"],
  };
  return terms[category] || [category];
}

function formatLatency(value: unknown) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "";
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return `${minutes}m ${rest}s`;
}

function tradeStreamStatusLabel(meta: LiveTradesMeta) {
  if (meta.stream_connected) return "WS";
  if (meta.stream_status === "reconnecting") return "重连中";
  if (meta.stream_status === "error") return "断开";
  return formatLatency(meta.latency_seconds) || "Live";
}

function officialFeedStatusLabel(meta: OfficialTradeFeedMeta) {
  if (meta.connected && meta.upstream_connected) return "RTDS";
  if (meta.status === "reconnecting") return "重连中";
  if (meta.status === "error") return "断开";
  if (meta.connected) return "已连接";
  return "连接中";
}

function formatDuration(value: unknown) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "--";
  const totalMinutes = Math.max(0, Math.round(seconds / 60));
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;
  const parts = [];
  if (days) parts.push(`${days}d`);
  if (hours || days) parts.push(`${hours}h`);
  if (!days) parts.push(`${minutes}m`);
  return parts.join(" ") || "<1m";
}

function currentMonthLabel() {
  return new Intl.DateTimeFormat("en-US", { month: "short", year: "numeric", timeZone: SHANGHAI_TIME_ZONE }).format(new Date());
}

function walletAge(value: string | null | undefined) {
  const time = parseApiDate(value).getTime();
  if (!Number.isFinite(time)) return "--";
  const days = Math.max(0, Math.floor((Date.now() - time) / 86400000));
  return `${days}d`;
}

function parseApiDate(value: string | number | null | undefined) {
  if (value === null || value === undefined || value === "") return new Date(NaN);
  if (typeof value === "number") return new Date(value < 10_000_000_000 ? value * 1000 : value);
  const text = String(value).trim();
  if (!text) return new Date(NaN);
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(text)) {
    return new Date(`${text.replace(" ", "T")}Z`);
  }
  return new Date(text);
}

function formatDateTime(value: string | number | null | undefined) {
  const date = parseApiDate(value);
  if (Number.isNaN(date.getTime())) return "--";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: SHANGHAI_TIME_ZONE,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function formatShortDate(value: string | number | null | undefined) {
  const date = parseApiDate(value);
  if (Number.isNaN(date.getTime())) return "--";
  return new Intl.DateTimeFormat("zh-CN", { timeZone: SHANGHAI_TIME_ZONE, month: "2-digit", day: "2-digit" }).format(date);
}

function formatRelativeTime(value: string | number | null | undefined) {
  const time = parseApiDate(value).getTime();
  if (!Number.isFinite(time)) return "--";
  const seconds = Math.max(0, Math.floor((Date.now() - time) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function shortAddress(value: string | null | undefined) {
  const text = String(value || "");
  if (text.length <= 15) return text || "--";
  return `${text.slice(0, 6)}...${text.slice(-6)}`;
}

function displayTraderName(trade: RecentTrade) {
  const address = normalizeAddress(trade.user_address || "");
  const candidates = [trade.trader_name, trade.trader_pseudonym];
  for (const candidate of candidates) {
    const text = String(candidate || "").trim();
    if (!text) continue;
    const embeddedAddress = text.match(/0x[a-fA-F0-9]{40}/)?.[0];
    if (embeddedAddress) return shortAddress(embeddedAddress.toLowerCase());
    return text;
  }
  return shortAddress(address);
}

function firstNumber(...values: unknown[]) {
  for (const value of values) {
    const number = Number(value);
    if (Number.isFinite(number)) return number;
  }
  return null;
}

function numberOrNull(value: unknown) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function numberOrUndefined(value: unknown) {
  const number = Number(value);
  return Number.isFinite(number) ? number : undefined;
}

function playTradeSound(volume: number, side: string | undefined) {
  try {
    const AudioContextClass = window.AudioContext || (window as typeof window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!AudioContextClass) return;
    const context = new AudioContextClass();
    const oscillator = context.createOscillator();
    const gain = context.createGain();
    const now = context.currentTime;
    oscillator.type = "sine";
    oscillator.frequency.value = String(side || "").toUpperCase() === "SELL" ? 380 : 620;
    gain.gain.setValueAtTime(0, now);
    gain.gain.linearRampToValueAtTime(Math.max(0, Math.min(1, volume / 100)) * 0.18, now + 0.015);
    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.18);
    oscillator.connect(gain);
    gain.connect(context.destination);
    oscillator.start(now);
    oscillator.stop(now + 0.2);
    window.setTimeout(() => void context.close(), 300);
  } catch {
    // Browsers can block audio until the user interacts with the page.
  }
}

function isMissing(value: unknown) {
  return value === null || value === undefined || value === "" || Number.isNaN(Number(value));
}

function formatNumber(value: unknown, maximumFractionDigits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits }).format(number);
}

function formatCompactNumber(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 2 }).format(number);
}

function formatCount(value: unknown) {
  if (isMissing(value)) return "--";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(value));
}

function formatCurrency(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  const abs = Math.abs(number);
  if (abs >= 1_000_000) return `${number < 0 ? "-" : ""}$${formatNumber(abs / 1_000_000, 2)}M`;
  if (abs >= 1_000) return `${number < 0 ? "-" : ""}$${formatNumber(abs / 1_000, 2)}K`;
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: abs < 10 ? 2 : 1 }).format(number);
}

function formatSignedCurrency(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  if (number === 0) return "$0";
  return `${number > 0 ? "+" : ""}${formatCurrency(number)}`;
}

function formatRatioPercent(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return `${formatNumber(number * 100, 2)}%`;
}

function formatSignedRatioPercent(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  if (number === 0) return "0%";
  return `${number > 0 ? "+" : ""}${formatNumber(number * 100, 2)}%`;
}

function formatPercentValue(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return `${number > 0 ? "+" : ""}${formatNumber(number, 2)}%`;
}

function formatPnlShortPercent(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  return number === 0 ? "0%" : "";
}

function formatPrice(value: unknown) {
  if (isMissing(value)) return "--";
  const number = Number(value);
  if (!Number.isFinite(number)) return "--";
  return `${formatNumber(number * 100, 1)}¢`;
}

function pnlPercent(pnl: unknown, basis: unknown) {
  const pnlNumber = Number(pnl);
  const basisNumber = Number(basis);
  if (!Number.isFinite(pnlNumber) || !Number.isFinite(basisNumber) || basisNumber === 0) return "";
  return `(${formatPercentValue((pnlNumber / Math.max(Math.abs(basisNumber - pnlNumber), 1)) * 100)})`;
}

function classForNumber(value: unknown, base: string) {
  const number = Number(value);
  if (!Number.isFinite(number) || number === 0) return base;
  return `${base} ${number > 0 ? "positive" : "negative"}`;
}

function tone(value: unknown) {
  const number = Number(value);
  if (!Number.isFinite(number) || number === 0) return "";
  return number > 0 ? "positive" : "negative";
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

createRoot(document.getElementById("root")!).render(<App />);
