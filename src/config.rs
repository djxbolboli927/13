use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub metis: MetisConfig,
    pub trading: TradingConfig,
    pub jito: JitoConfig,
    pub rpc: RpcConfig,
    pub yellowstone_grpc: YellowstoneGrpcConfig,
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub simulation: SimulationConfig,
    #[serde(default)]
    pub jito_grpc: JitoGrpcConfig,
    #[serde(default)]
    pub template_cache: TemplateCacheConfig,
    /// Legacy circular-scan strategy toggle.
    #[serde(default)]
    pub scanner: ScannerConfig,
    /// ShredStream / Pump.fun ↔ Meteora arbitrage strategy.
    #[serde(default)]
    pub shred_arb: ShredArbConfig,
}

/// Toggle for the legacy Metis circular-scan strategy.
#[derive(Debug, Deserialize, Clone)]
pub struct ScannerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// ShredStream-triggered arbitrage between Pump.fun AMM and Meteora DAMM v2.
#[derive(Debug, Deserialize, Clone)]
pub struct ShredArbConfig {
    /// Master on/off switch for the strategy.
    #[serde(default)]
    pub enabled: bool,
    /// Local jito-shredstream-proxy gRPC surface (SubscribeEntries).
    #[serde(default = "default_shredstream_endpoint")]
    pub shredstream_endpoint: String,
    /// Auto-launch and supervise `jito-shredstream-proxy` as a child process so
    /// everything comes up with the bot (operator only supplies the keypair).
    /// Set false to run the proxy externally.
    #[serde(default = "default_true")]
    pub proxy_autostart: bool,
    /// Proxy binary (name on PATH or absolute path).
    #[serde(default = "default_proxy_bin")]
    pub proxy_bin: String,
    /// Jito Block Engine URL the proxy authenticates against.
    #[serde(default = "default_block_engine_url")]
    pub block_engine_url: String,
    /// Comma-separated ShredStream regions (max 2).
    #[serde(default = "default_desired_regions")]
    pub desired_regions: String,
    /// UDP fan-out target for raw shreds (required by the proxy; a local
    /// throwaway is fine when only the gRPC entries path is consumed).
    #[serde(default = "default_dest_ip_ports")]
    pub proxy_dest_ip_ports: String,
    /// Local UDP port the proxy binds to receive shreds.
    #[serde(default = "default_src_bind_port")]
    pub proxy_src_bind_port: u16,
    /// Extra raw args passed through to the proxy verbatim.
    #[serde(default)]
    pub proxy_extra_args: Vec<String>,
    /// Yellowstone gRPC endpoint for live pool state (narrow account filter).
    #[serde(default)]
    pub pool_state_endpoint: String,
    /// x-token for the pool-state gRPC endpoint.
    #[serde(default)]
    pub pool_state_x_token: String,
    /// Shared Metis pool cache file — the bot reads the SAME pools as Metis.
    #[serde(default = "default_mix_path")]
    pub mix_cache_path: String,
    /// Whitelisted ShredStream keypair (used by the proxy sidecar; recorded
    /// here for reference/ops).
    #[serde(default = "default_shred_keypair")]
    #[allow(dead_code)]
    pub shred_keypair: String,
    /// Compute-unit limit for the 2-hop arb tx. Competitor arb txs use ~178k
    /// CU, so keep headroom. Default 300000.
    #[serde(default = "default_shred_cu_limit")]
    pub cu_limit: u32,
    /// Fixed Jito tip in test phase.
    #[serde(default = "default_tip")]
    pub tip_lamports: u64,
    /// Network base fee.
    #[serde(default = "default_net_fee")]
    pub network_fee_lamports: u64,
    /// Effective Meteora fee in bps (dynamic fee not yet modelled — verify).
    #[serde(default = "default_meteora_fee_bps")]
    pub meteora_fee_bps: u64,
    /// Ignore observed Pump trades whose SOL-side arg is below this (SOL).
    #[serde(default = "default_min_trigger_sol")]
    pub min_trigger_sol: f64,
    /// Lower bound of the input-size search (SOL).
    #[serde(default = "default_min_amount_sol")]
    pub min_amount_sol: f64,
    /// Upper bound of the input-size search (SOL).
    #[serde(default = "default_max_amount_sol")]
    pub max_amount_sol: f64,
    /// Per-pool cooldown between fires (ms).
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    /// Bounded signal channel capacity.
    #[serde(default = "default_signal_buffer")]
    pub signal_buffer: usize,
    /// Size ceiling as a percent of the BUY pool's WSOL reserve. The optimizer
    /// finds the net-maximizing size within this ceiling. Default 100%.
    #[serde(default = "default_max_price_impact_pct")]
    pub max_price_impact_pct: f64,
    /// Enter this percent below the computed optimum for slippage headroom.
    #[serde(default = "default_size_safety_margin_pct")]
    pub size_safety_margin_pct: f64,
    /// Reject opportunities whose predicted net exceeds this percent of the
    /// input — a dead-pool mispricing. Default 50%.
    #[serde(default = "default_max_profit_fraction_pct")]
    pub max_profit_fraction_pct: f64,
    /// DEPRECATED (test phase over). Kept so old config.toml files still parse.
    #[serde(default)]
    #[allow(dead_code)]
    pub force_send_test: bool,
    /// Minimum predicted NET profit (lamports, above the network fee) an
    /// opportunity must clear before we fetch instructions and send. Production
    /// default 5000 (= one network base fee). Below this we don't bother.
    #[serde(default = "default_min_net_profit")]
    pub min_net_profit_lamports: u64,
    /// Send arb transactions DIRECTLY to the network via RPC instead of through
    /// Jito bundles (Jito adds latency + a tip cost). Default true.
    #[serde(default = "default_true")]
    pub direct_send: bool,
    /// Micro-lamports per compute unit for the priority fee on direct sends
    /// (0 = no priority fee). Only used when `direct_send = true`.
    #[serde(default)]
    pub direct_priority_fee_microlamports: u64,
    /// Exact Metis `dexes=` label for the Pump.fun AMM leg. Case/spacing
    /// sensitive. Configurable so you can fix it without a recompile if Metis
    /// uses a slightly different string.
    #[serde(default = "default_pump_label")]
    pub metis_pump_label: String,
    /// Exact Metis `dexes=` label for the Meteora DAMM v2 leg. If forced quotes
    /// keep returning "No routes found" on the Meteora leg, try variants here
    /// (e.g. "Meteora DAMM V2", "Meteora DAMM v2").
    #[serde(default = "default_meteora_label")]
    pub metis_meteora_label: String,
    /// Use Jupiter's shared-accounts program. NOTE: this BREAKS our hand-merged
    /// circular quote (Metis returns an error building swap-instructions), so it
    /// must stay false for this strategy. Kept configurable for completeness.
    #[serde(default)]
    pub metis_use_shared_accounts: bool,
    /// `maxAccounts` requested from Metis for each forced leg. Lower = fewer
    /// accounts pulled into the tx = smaller serialized size (Solana caps a tx
    /// at 1232 raw bytes). 32 keeps a 2-hop circular comfortably under the cap.
    #[serde(default = "default_metis_max_accounts")]
    pub metis_max_accounts: u64,
    /// If > 0, add a `SetLoadedAccountsDataSizeLimit` compute-budget instruction
    /// with this byte value (competitors use it to cut the CU billed for
    /// account loading, which helps landing). 0 = don't add it. Note: it does
    /// NOT reduce tx size — it adds a few bytes — so leave off if size-bound.
    #[serde(default)]
    pub direct_loaded_accounts_data_limit: u32,
    /// SetComputeUnitPrice priority fee, micro-lamports per CU (0 = don't add).
    /// Applies on the JITO path (the direct path uses
    /// `direct_priority_fee_microlamports`). Competitors set a priority fee on top
    /// of the Jito tip; toggle this to A/B test whether it helps landing.
    #[serde(default)]
    pub compute_unit_price_microlamports: u64,
    /// Max consecutive Metis "No routes found" failures on a pair before it is
    /// disabled (stops wasting compute on a token Metis won't route). The
    /// disable reason is written to the error log. 0 = built-in default (10).
    #[serde(default = "default_metis_load_retry_limit")]
    pub metis_load_retry_limit: u32,
    /// Skip an opportunity if the Meteora pool's cached state is more than this
    /// many slots behind the newest slot seen. Pricing off a stale Meteora
    /// sqrt_price is the proven cause of the 0x1771 over-prediction reverts, so
    /// this refuses to trade on stale state. 0 = gate off. Try 10-25.
    #[serde(default)]
    pub meteora_max_stale_slots: u64,
    /// Token mints whose ATA is assumed to ALWAYS exist — never checked, never
    /// created at startup. Put SOL/WSOL/USDC/USDT (and any other permanent
    /// holdings) here. Edited in config.toml under `[shred_arb]`.
    #[serde(default = "default_always_exist_mints")]
    pub always_exist_mints: Vec<String>,
    /// Auto-discover new shared Pump.fun/Meteora pools via public APIs and add
    /// them to the strategy at runtime. Default true.
    #[serde(default = "default_true")]
    pub discovery_enabled: bool,
    /// Discovery poll interval (seconds). Latency-tolerant — arb, not sniper.
    #[serde(default = "default_discovery_interval")]
    pub discovery_interval_secs: u64,
    /// Re-check EVERY tracked token for newly-created counter pools this often
    /// (seconds). A token we already trade can get a brand-new Meteora pool at
    /// any time; this is how the bot notices and adds it. Default 60.
    #[serde(default = "default_discovery_recheck_interval")]
    pub discovery_recheck_interval_secs: u64,
    /// GeckoTerminal "new pools" feed (the new-token source).
    #[serde(default = "default_gecko_new_pools_url")]
    pub discovery_new_pools_url: String,
    /// DexScreener token→pairs endpoint (the cross-DEX resolver); `{mint}` is
    /// substituted with the token mint.
    #[serde(default = "default_dexscreener_token_url")]
    pub discovery_token_pairs_url: String,
    /// STARTUP BOOTSTRAP: feed URLs (GeckoTerminal-shaped JSON) scanned once at
    /// startup to seed the strategy with hot/top/trending tokens that already
    /// trade on BOTH venues — so the bot isn't limited to the handful in
    /// mix.json. Each pool's base+quote token is a candidate; only those with a
    /// live Pump.fun AND Meteora pool are added.
    #[serde(default = "default_discovery_seed_urls")]
    pub discovery_seed_urls: Vec<String>,
    /// Max tokens to resolve during the startup bootstrap (caps API/RPC work).
    #[serde(default = "default_bootstrap_max")]
    pub discovery_bootstrap_max: usize,
    /// Minimum 1-hour USD volume for a token to be considered "hot" and added.
    /// This — not pool age — is the real freshness signal: a token actively
    /// traded in the last hour, even if its pool is older. Filters out the
    /// stale ones whose last trade was many hours ago. Default 200. 0 disables.
    #[serde(default = "default_min_h1_volume_usd")]
    pub discovery_min_h1_volume_usd: f64,
    /// Minimum Pump.fun-side WSOL reserve (lamports) for a token to be added.
    /// Liquidity matters mostly on the Pump side; a pool below this is too thin
    /// (or rugged) to bother with. Default 0.05 SOL. 0 disables the check.
    #[serde(default = "default_min_pump_wsol")]
    pub discovery_min_pump_wsol_lamports: u64,
    /// Minimum Meteora-side WSOL vault balance (lamports) to add a token. The
    /// arb is capped by the THIN side, so a near-empty Meteora pool can never
    /// clear the fee no matter how big the gap. Default 0.002 SOL. 0 disables.
    #[serde(default = "default_min_meteora_wsol")]
    pub discovery_min_meteora_wsol_lamports: u64,
    /// Close a pool + its ATA if NO account update arrives for its Meteora pool
    /// for this many seconds while the bot is running (idle = abandoned/rugged).
    /// This — not a liquidity dip — is the primary rug signal. Default 14400 (4h).
    #[serde(default = "default_pool_idle_close_secs")]
    pub pool_idle_close_secs: u64,
    /// Only react to an observed Pump trade if its SOL-side size is at least this
    /// fraction of the Pump pool's WSOL reserve (e.g. 0.02 = 2%). Focuses work on
    /// trades that actually move price. 0 disables (fall back to min_trigger_sol).
    #[serde(default)]
    pub min_trigger_reserve_frac: f64,
    /// Minimum gap (ms) between two SENDS on the SAME pool. A standing price gap
    /// re-fires every cooldown (~100ms); without this we'd blast dozens of
    /// identical txs before the first even lands. Set LOW (100-200) so several
    /// distinct opportunities in the same block can each send; 0 = OFF (no dedup,
    /// every profitable eval sends — bounded only by the RPC rate). Default 150.
    #[serde(default = "default_send_dedup_ms")]
    pub send_dedup_ms: u64,
    /// "Instructions++": serve whole-route swap instructions from the in-RAM
    /// route cache (skip Metis on repeat opportunities). true = on.
    #[serde(default = "default_true", rename = "Instructions++")]
    pub instructions_pp: bool,
    /// Worst-case Meteora fee: price the volatility (dynamic) fee at the pool's
    /// `max_volatility_accumulator` ceiling so the fee is never understated
    /// (kills phantom profit on volatile new pools). Default false.
    #[serde(default)]
    pub meteora_fee_worst_case: bool,
    /// Minimum WSOL depth (lamports) a pool must hold on EACH side to be traded.
    /// Below this on either the Pump or Meteora side, the pool is skipped
    /// up-front. Default 800_000 (0.0008 WSOL).
    #[serde(default = "default_min_pool_wsol_lamports")]
    pub min_pool_wsol_lamports: u64,
    /// Seconds between fee-audit log lines (decoded fee per tracked pair, for
    /// hand-verification against real on-chain swaps). 0 = off. Default 120.
    #[serde(default = "default_fee_audit_log_secs")]
    pub fee_audit_log_secs: u64,
    /// Seconds to wait after a direct send before polling the tx's on-chain fate.
    #[serde(default = "default_status_check_delay_secs")]
    pub status_check_delay_secs: u64,
    /// Absolute path to a directory where an errors-only log file is written
    /// (errors + why a tx was NOT sent + why a sent tx was lost). Default /root/g.
    #[serde(default = "default_error_log_dir")]
    pub error_log_dir: String,
    /// Target wallets whose recent transactions are mined for hot shared
    /// Pump.fun/Meteora pools (competitors' pools). Empty = wallet mining off.
    #[serde(default)]
    pub target_wallets: Vec<String>,
    /// KEY competitor wallets whose Pump↔Meteora arb txs rarely revert. When
    /// one of these fires, the engine runs the two-scenario Meteora prediction
    /// (their leg lands vs not) and sends a tx for each. Empty = fall back to
    /// `target_wallets`.
    #[serde(default)]
    pub key_wallets: Vec<String>,
    /// Re-run the wallet mining pass every this many seconds. Default 1800 (30m).
    #[serde(default = "default_wallet_mine_interval_secs")]
    pub wallet_mine_interval_secs: u64,
    /// How many recent signatures per wallet to scan each pass. Default 1000.
    #[serde(default = "default_wallet_mine_tx_limit")]
    pub wallet_mine_tx_limit: usize,
    /// Extra RPC endpoints the wallet miner + pool discovery round-robin across
    /// (each with its own rate gate) so competitor scanning never trips a single
    /// endpoint's 429. Empty = use only [rpc].secondary_url.
    #[serde(default)]
    pub wallet_mine_rpc_urls: Vec<String>,
    /// Max RPC calls/sec PER endpoint for competitor scanning (shyft caps at 5).
    #[serde(default = "default_wallet_mine_rps")]
    pub wallet_mine_rpc_calls_per_sec: u32,
    /// File that persists our self-owned ALT pubkey(s) so a restart reuses the
    /// same on-chain table(s) instead of leaking rent. Default /root/g/our_alt.txt.
    #[serde(default = "default_alt_store_path")]
    pub alt_store_path: String,
    /// If true, NEVER close a pool/ATA — only ever open. Rug/idle/drain no longer
    /// tears anything down; instead the idle reporter records untraded tokens.
    #[serde(default = "default_true")]
    pub never_close_pools: bool,
    /// Every this many seconds, write the list of tokens with NO on-chain trade
    /// in the window to `idle_tokens_path`. Default 43200 (12h).
    #[serde(default = "default_idle_check_secs")]
    pub idle_token_check_secs: u64,
    /// File the idle-token report is written to. Default /root/g/idle-tokens.txt.
    #[serde(default = "default_idle_tokens_path")]
    pub idle_tokens_path: String,
    /// Free ALT providers, tried in order. Each entry is "name|baseUrl" of a
    /// Jupiter-compatible `/quote` + `/swap-instructions` API. We fetch the ALT
    /// covering each pool's route from these (free) instead of paying to build
    /// our own. Default: Jupiter Lite (add DFlow/Raptor once their URLs are set).
    #[serde(default = "default_alt_fetch_providers")]
    pub alt_fetch_providers: Vec<String>,
    /// Build/extend our OWN on-chain ALT (costs rent). Default false now that we
    /// fetch free ALTs from the providers above. Kept as a fallback toggle.
    #[serde(default)]
    pub alt_self_build: bool,
    /// Max ALTs kept per pool (the aggregator's route ALT already covers the
    /// route, so 1 is enough; raise only if a tx still comes back too large).
    #[serde(default = "default_alt_max_per_pool")]
    pub alt_max_per_pool: usize,
    /// Route-account coverage at which a competitor ALT is accepted as a pool's
    /// table and registered with Metis (of the 6 pool+vault accounts). Default 4.
    #[serde(default = "default_alt_min_coverage")]
    #[allow(dead_code)]
    pub alt_min_coverage: usize,
    /// Minimum Jito tip (lamports) added on top of the profit share. Default 1000.
    #[serde(default = "default_jito_tip_min")]
    pub jito_tip_min_lamports: u64,
    /// Fraction of the detected net profit paid to Jito as tip (0.20 = 20%).
    #[serde(default = "default_jito_tip_profit_frac")]
    pub jito_tip_profit_fraction: f64,
    /// DIAGNOSTIC: after each send, re-run the EXACT tx through the RPC
    /// `simulateTransaction` against live chain state and log our predicted
    /// output vs. the network's real output (plus the raw Pump/Meteora swap
    /// logs, so per-leg amounts and fees can be hand-compared). Off the hot
    /// path (runs in a spawned task; adds zero send latency). Default false.
    #[serde(default)]
    pub rpc_sim_compare: bool,
    /// Disable the "preempted" last-moment recheck (send even if the pool moved
    /// during the compute window). Default false.
    #[serde(default)]
    pub disable_preempt: bool,
    /// Force-send every profitable opportunity: bypass the send-dedup throttle
    /// and the preempt recheck, so only a Metis routing failure stops a send.
    /// Default false.
    #[serde(default)]
    pub force_send_profitable: bool,
}

fn default_alt_fetch_providers() -> Vec<String> {
    vec!["jupiter|https://lite-api.jup.ag/swap/v1".to_string()]
}
fn default_alt_max_per_pool() -> usize {
    1
}
fn default_alt_min_coverage() -> usize {
    4
}
fn default_jito_tip_min() -> u64 {
    1000
}
fn default_jito_tip_profit_frac() -> f64 {
    0.20
}

fn default_alt_store_path() -> String {
    "/root/g/our_alt.txt".to_string()
}
fn default_idle_check_secs() -> u64 {
    43_200
}
fn default_idle_tokens_path() -> String {
    "/root/g/idle-tokens.txt".to_string()
}

fn default_send_dedup_ms() -> u64 {
    150
}

fn default_fee_audit_log_secs() -> u64 {
    120
}

fn default_min_pool_wsol_lamports() -> u64 {
    800_000
}
fn default_status_check_delay_secs() -> u64 {
    12
}
fn default_error_log_dir() -> String {
    "/root/g".to_string()
}
fn default_wallet_mine_interval_secs() -> u64 {
    // First pass is a full ~1000-tx scan; every pass after is incremental (only
    // new signatures), so a short interval is cheap and keeps the pool set fresh.
    300
}
fn default_wallet_mine_tx_limit() -> usize {
    1000
}
fn default_wallet_mine_rps() -> u32 {
    5
}

impl Default for ShredArbConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shredstream_endpoint: default_shredstream_endpoint(),
            proxy_autostart: true,
            proxy_bin: default_proxy_bin(),
            block_engine_url: default_block_engine_url(),
            desired_regions: default_desired_regions(),
            proxy_dest_ip_ports: default_dest_ip_ports(),
            proxy_src_bind_port: default_src_bind_port(),
            proxy_extra_args: Vec::new(),
            pool_state_endpoint: String::new(),
            pool_state_x_token: String::new(),
            mix_cache_path: default_mix_path(),
            shred_keypair: default_shred_keypair(),
            cu_limit: default_shred_cu_limit(),
            tip_lamports: default_tip(),
            network_fee_lamports: default_net_fee(),
            meteora_fee_bps: default_meteora_fee_bps(),
            min_trigger_sol: default_min_trigger_sol(),
            min_amount_sol: default_min_amount_sol(),
            max_amount_sol: default_max_amount_sol(),
            cooldown_ms: default_cooldown_ms(),
            signal_buffer: default_signal_buffer(),
            max_price_impact_pct: default_max_price_impact_pct(),
            size_safety_margin_pct: default_size_safety_margin_pct(),
            max_profit_fraction_pct: default_max_profit_fraction_pct(),
            force_send_test: false,
            min_net_profit_lamports: default_min_net_profit(),
            direct_send: true,
            direct_priority_fee_microlamports: 0,
            always_exist_mints: default_always_exist_mints(),
            discovery_enabled: true,
            discovery_interval_secs: default_discovery_interval(),
            discovery_recheck_interval_secs: default_discovery_recheck_interval(),
            discovery_new_pools_url: default_gecko_new_pools_url(),
            discovery_token_pairs_url: default_dexscreener_token_url(),
            discovery_seed_urls: default_discovery_seed_urls(),
            discovery_bootstrap_max: default_bootstrap_max(),
            discovery_min_h1_volume_usd: default_min_h1_volume_usd(),
            discovery_min_pump_wsol_lamports: default_min_pump_wsol(),
            discovery_min_meteora_wsol_lamports: default_min_meteora_wsol(),
            pool_idle_close_secs: default_pool_idle_close_secs(),
            min_trigger_reserve_frac: 0.0,
            send_dedup_ms: default_send_dedup_ms(),
            instructions_pp: true,
            meteora_fee_worst_case: false,
            min_pool_wsol_lamports: default_min_pool_wsol_lamports(),
            fee_audit_log_secs: default_fee_audit_log_secs(),
            status_check_delay_secs: default_status_check_delay_secs(),
            error_log_dir: default_error_log_dir(),
            target_wallets: Vec::new(),
            key_wallets: Vec::new(),
            wallet_mine_interval_secs: default_wallet_mine_interval_secs(),
            wallet_mine_tx_limit: default_wallet_mine_tx_limit(),
            wallet_mine_rpc_urls: Vec::new(),
            wallet_mine_rpc_calls_per_sec: default_wallet_mine_rps(),
            alt_store_path: default_alt_store_path(),
            never_close_pools: true,
            idle_token_check_secs: default_idle_check_secs(),
            idle_tokens_path: default_idle_tokens_path(),
            alt_fetch_providers: default_alt_fetch_providers(),
            alt_self_build: false,
            alt_max_per_pool: default_alt_max_per_pool(),
            alt_min_coverage: default_alt_min_coverage(),
            jito_tip_min_lamports: default_jito_tip_min(),
            jito_tip_profit_fraction: default_jito_tip_profit_frac(),
            metis_pump_label: default_pump_label(),
            metis_meteora_label: default_meteora_label(),
            metis_use_shared_accounts: false,
            metis_max_accounts: default_metis_max_accounts(),
            direct_loaded_accounts_data_limit: 0,
            compute_unit_price_microlamports: 0,
            metis_load_retry_limit: default_metis_load_retry_limit(),
            meteora_max_stale_slots: 0,
            rpc_sim_compare: false,
            disable_preempt: false,
            force_send_profitable: false,
        }
    }
}

fn default_metis_max_accounts() -> u64 {
    50
}
fn default_metis_load_retry_limit() -> u32 {
    10
}
fn default_pump_label() -> String {
    "Pump.fun Amm".to_string()
}
fn default_meteora_label() -> String {
    "Meteora DAMM v2".to_string()
}
fn default_min_h1_volume_usd() -> f64 {
    200.0
}
fn default_min_pump_wsol() -> u64 {
    50_000_000
}
fn default_min_meteora_wsol() -> u64 {
    2_000_000
}
fn default_pool_idle_close_secs() -> u64 {
    1_800
}

fn default_discovery_seed_urls() -> Vec<String> {
    vec![
        // Trending (actively traded RIGHT NOW) first, then newest pools. The
        // 1-hour-volume filter keeps only hot tokens; the on-chain owner check
        // keeps those on BOTH Pump.fun and Meteora.
        "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=1".to_string(),
        "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=2".to_string(),
        "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1".to_string(),
        "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=2".to_string(),
    ]
}
fn default_bootstrap_max() -> usize {
    100
}

fn default_discovery_interval() -> u64 {
    5
}

fn default_discovery_recheck_interval() -> u64 {
    60
}
fn default_min_net_profit() -> u64 {
    5000
}
fn default_always_exist_mints() -> Vec<String> {
    vec![
        // SOL / Wrapped SOL (same mint), USDC, USDT — permanent holdings whose
        // ATAs we never need to create.
        "So11111111111111111111111111111111111111112".to_string(), // WSOL / SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(), // USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string(), // USDT
    ]
}
fn default_gecko_new_pools_url() -> String {
    "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1".to_string()
}
fn default_dexscreener_token_url() -> String {
    "https://api.dexscreener.com/latest/dex/tokens/{mint}".to_string()
}

fn default_max_price_impact_pct() -> f64 {
    100.0
}
fn default_size_safety_margin_pct() -> f64 {
    3.0
}
fn default_max_profit_fraction_pct() -> f64 {
    50.0
}

fn default_shredstream_endpoint() -> String {
    "http://127.0.0.1:9999".to_string()
}
fn default_proxy_bin() -> String {
    "jito-shredstream-proxy".to_string()
}
fn default_block_engine_url() -> String {
    "https://mainnet.block-engine.jito.wtf".to_string()
}
fn default_desired_regions() -> String {
    "amsterdam,frankfurt".to_string()
}
fn default_dest_ip_ports() -> String {
    "127.0.0.1:20001".to_string()
}
fn default_src_bind_port() -> u16 {
    20000
}
fn default_mix_path() -> String {
    "/root/g/metis/mix.json".to_string()
}
fn default_shred_keypair() -> String {
    "/root/g/wallet/shred.json".to_string()
}
fn default_tip() -> u64 {
    1600
}
fn default_shred_cu_limit() -> u32 {
    300_000
}
fn default_net_fee() -> u64 {
    5000
}
fn default_meteora_fee_bps() -> u64 {
    25
}
fn default_min_trigger_sol() -> f64 {
    1.0
}
fn default_min_amount_sol() -> f64 {
    0.001
}
fn default_max_amount_sol() -> f64 {
    0.05
}
fn default_cooldown_ms() -> u64 {
    200
}
fn default_signal_buffer() -> usize {
    1024
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetisConfig {
    pub url: String,
    #[allow(dead_code)]
    pub binary_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradingConfig {
    pub min_amount_sol: f64,
    pub max_amount_sol: f64,
    pub step_sol: f64,
    pub min_profit_lamports: u64,
    /// Standard Solana transaction fee in lamports (5000 = one signature fee).
    #[allow(dead_code)]
    pub base_fee_lamports: u64,
    pub tokens_file: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JitoConfig {
    /// Multiple Jito block engine URLs -- bundles are sent to ALL concurrently.
    pub urls: Vec<String>,
    pub uuid: String,
    pub trading_keypair: String,
    #[allow(dead_code)]
    pub tip_min_lamports: u64,
    #[allow(dead_code)]
    pub tip_max_lamports: u64,
    #[allow(dead_code)]
    pub tip_profit_percent: f64,
    pub max_bundles_per_second: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub url: String,
    /// Optional secondary RPC for HIGH-VOLUME, non-trade-critical reads
    /// (competitor-wallet mining, pool discovery). Keeps that traffic off the
    /// trading RPC so blockhash fetches and sends never hit its rate limit.
    /// Falls back to `url` when empty.
    #[serde(default)]
    pub secondary_url: String,
}

impl RpcConfig {
    /// The RPC to use for background/high-volume reads (wallet miner, discovery).
    pub fn secondary(&self) -> &str {
        if self.secondary_url.trim().is_empty() {
            &self.url
        } else {
            &self.secondary_url
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct YellowstoneGrpcConfig {
    pub endpoint: String,
    pub x_token: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SimulationConfig {
    /// If false, bot sends every profitable tx without any local sim gate
    /// (pre-LiteSVM behaviour). Default: disabled so legacy configs keep
    /// working until the operator opts in.
    #[serde(default)]
    pub enabled: bool,
    /// Directory containing the DEX .so binaries listed in `program_registry`.
    #[serde(default = "default_so_dir")]
    pub so_dir: String,
    /// Directory containing per-pool account files (`dex/<DEX>/<pool>.toml`).
    /// These are pre-fetched at startup and the vault accounts within are
    /// subscribed for live Yellowstone updates.
    #[serde(default = "default_dex_dir")]
    pub dex_dir: String,
    /// When sim reverts or errors, `fail_closed=true` drops the send (safest);
    /// `false` logs and forwards to Jito anyway (useful during rollout).
    #[serde(default = "default_true")]
    pub fail_closed: bool,
    /// Number of INDEPENDENT Simulator instances to spin up. Each Simulator
    /// owns its own `Mutex<LiteSVM>`, so N workers = N sims in parallel.
    /// Sizing guidance: in steady state each sim takes ~2-5ms of CPU, so
    /// `workers` should roughly equal the peak number of profitable
    /// opportunities that arrive per 5ms window. In production, 8 is a
    /// sensible default (handles ~1600 sims/sec with headroom).
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Whether the sim may BLOCK a send (drop on revert). OFF by default: the
    /// sim is a safety net, and a mis-simulating pool must never silently halt
    /// trading. Turn on only after the SIM ok/reverted counters look healthy.
    #[serde(default)]
    pub gate_sends: bool,
    /// Global ceiling on RPC calls/sec used to seed base account owner/lamports
    /// (the shyft plan caps at 5). Live pool state comes from gRPC, so this only
    /// throttles the one-time base seeds — never the hot path.
    #[serde(default = "default_sim_rpc_cps")]
    pub rpc_calls_per_sec: u32,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            so_dir: default_so_dir(),
            dex_dir: default_dex_dir(),
            fail_closed: true,
            workers: default_workers(),
            gate_sends: false,
            rpc_calls_per_sec: default_sim_rpc_cps(),
        }
    }
}

fn default_sim_rpc_cps() -> u32 {
    5
}

fn default_so_dir() -> String {
    "/home/soluser/m/so".to_string()
}

fn default_dex_dir() -> String {
    "vendor/litesvm/dex".to_string()
}

fn default_true() -> bool {
    true
}

fn default_workers() -> usize {
    8
}

/// Second Jito submission path via SearcherService gRPC.
///
/// Runs alongside the REST UUID client in `jito.rs`. Each path has its
/// own rate limiter, so the effective Jito throughput is
/// `jito.max_bundles_per_second + jito_grpc.max_bundles_per_second`.
///
/// Like the REST client, gRPC fans out to every regional Block Engine
/// endpoint concurrently — first regional success wins. Per-region auth
/// is attempted using the whitelisted keypair, which gives 5 req/s per
/// region. Regions whose auth fails downgrade to no-auth mode (1 req/s).
#[derive(Debug, Deserialize, Clone)]
pub struct JitoGrpcConfig {
    /// If false, only the REST UUID path is used (pre-gRPC behaviour).
    #[serde(default)]
    pub enabled: bool,
    /// All Jito Block Engine gRPC endpoints. Bundles are broadcast to ALL
    /// of these per send call, mirroring the REST multi-region fan-out.
    #[serde(default = "default_jito_grpc_endpoints")]
    pub endpoints: Vec<String>,
    /// Path to the Solana keypair JSON whose pubkey Jito has whitelisted
    /// for gRPC auth. This wallet holds no funds — it is an identity only.
    /// If empty or auth fails, regions fall back to no-auth (1 req/s).
    #[serde(default)]
    pub auth_keypair: String,
    /// Per-second rate limit applied *before* the gRPC SendBundle call.
    /// REST and gRPC limiters operate independently.
    #[serde(default = "default_grpc_rate")]
    pub max_bundles_per_second: u32,
}

impl Default for JitoGrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: default_jito_grpc_endpoints(),
            auth_keypair: String::new(),
            max_bundles_per_second: default_grpc_rate(),
        }
    }
}

fn default_jito_grpc_endpoints() -> Vec<String> {
    vec![
        "https://amsterdam.mainnet.block-engine.jito.wtf".to_string(),
        "https://dublin.mainnet.block-engine.jito.wtf".to_string(),
        "https://frankfurt.mainnet.block-engine.jito.wtf".to_string(),
        "https://london.mainnet.block-engine.jito.wtf".to_string(),
        "https://ny.mainnet.block-engine.jito.wtf".to_string(),
        "https://slc.mainnet.block-engine.jito.wtf".to_string(),
        "https://singapore.mainnet.block-engine.jito.wtf".to_string(),
        "https://tokyo.mainnet.block-engine.jito.wtf".to_string(),
    ]
}

fn default_grpc_rate() -> u32 {
    5
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    /// Number of tokio worker threads (multi-thread runtime).
    pub threads: usize,
    pub quote_timeout_ms: u64,
    /// CU limits per hop count: index 0 = 2 hops, index 1 = 3 hops, etc.
    /// If hops exceed the array, the last value is used.
    pub cu_limits: Vec<u32>,
    /// Maximum in-flight Metis quote requests per scan chunk.
    /// Keeps the HTTP connection pool from being overwhelmed.
    #[serde(default = "default_max_concurrent_quotes")]
    pub max_concurrent_quotes: usize,
    /// Maximum concurrent Stage-2 calc workers (merge quotes + fire
    /// swap_instructions). With fire-and-forget each worker holds its slot
    /// only for microseconds, so this can be set high to rule out the calc
    /// stage as a bottleneck. Default 6 (legacy value).
    #[serde(default = "default_calc_workers")]
    pub calc_workers: usize,
    /// Max time (ms) a swap_instructions result may wait in the LIFO queue
    /// before being dropped by a calc worker. Tune higher to tolerate slower
    /// Metis responses; lower to discard stale opportunities faster.
    #[serde(default = "default_queue_max_age_ms")]
    pub queue_max_age_ms: u64,
    #[serde(default)]
    pub bot_cpu_cores: Vec<usize>,
}

fn default_max_concurrent_quotes() -> usize {
    512
}

fn default_calc_workers() -> usize {
    6
}

fn default_queue_max_age_ms() -> u64 {
    5000
}

/// Template cache configuration.
///
/// Rollout order:
///   1. save_new=true       — extract and store route/hop templates from Metis
///                            responses. No behaviour change yet.
///   2. serve_route=true    — serve from RouteTemplate on hit, patching
///                            in_amount / quoted_out_amount in the Borsh data.
///                            Falls back to Metis when patching is not possible.
///   3. serve_from_metis=false — RAM-only (miss = drop, no Metis call).
#[derive(Debug, Deserialize, Clone)]
pub struct TemplateCacheConfig {
    /// Extract and save route/hop templates from every Metis response.
    #[serde(default)]
    pub save_new: bool,
    /// Serve from RouteTemplate when available (patches amounts if needed).
    #[serde(default)]
    pub serve_route: bool,
    /// Call Metis for instructions when no route template hits.
    #[serde(default = "default_true_tc")]
    pub serve_from_metis: bool,
}

impl Default for TemplateCacheConfig {
    fn default() -> Self {
        Self {
            save_new: false,
            serve_route: false,
            serve_from_metis: true,
        }
    }
}

fn default_true_tc() -> bool {
    true
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
