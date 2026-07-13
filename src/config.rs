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
            discovery_new_pools_url: default_gecko_new_pools_url(),
            discovery_token_pairs_url: default_dexscreener_token_url(),
            discovery_seed_urls: default_discovery_seed_urls(),
            discovery_bootstrap_max: default_bootstrap_max(),
            discovery_min_h1_volume_usd: default_min_h1_volume_usd(),
            discovery_min_pump_wsol_lamports: default_min_pump_wsol(),
            discovery_min_meteora_wsol_lamports: default_min_meteora_wsol(),
            pool_idle_close_secs: default_pool_idle_close_secs(),
            min_trigger_reserve_frac: 0.0,
            metis_pump_label: default_pump_label(),
            metis_meteora_label: default_meteora_label(),
            metis_max_accounts: default_metis_max_accounts(),
            direct_loaded_accounts_data_limit: 0,
        }
    }
}

fn default_metis_max_accounts() -> u64 {
    50
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
    14_400
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
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            so_dir: default_so_dir(),
            dex_dir: default_dex_dir(),
            fail_closed: true,
            workers: default_workers(),
        }
    }
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
