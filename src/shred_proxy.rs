//! Supervised launcher for `jito-shredstream-proxy`.
//!
//! Native Jito ShredStream is delivered over UDP and must be authenticated with
//! a whitelisted keypair and deshredded into entries — the `jito-shredstream-proxy`
//! binary does exactly that and re-serves reconstructed entries on a local gRPC
//! port. So the bot can be the ONLY thing the operator starts (besides Metis),
//! we spawn and supervise that proxy ourselves: the operator just drops in the
//! keypair, and everything comes up with the bot.
//!
//! The child inherits stdio (its logs interleave with ours) and is restarted
//! with a short backoff if it exits. The ShredStream consumer connects to the
//! proxy's local gRPC and already retries until it is ready.

use std::time::Duration;
use tracing::{error, info, warn};

/// Parameters for the proxy child process.
#[derive(Clone)]
pub struct ProxyConfig {
    pub bin: String,
    pub block_engine_url: String,
    pub auth_keypair: String,
    pub desired_regions: String,
    pub dest_ip_ports: String,
    pub src_bind_port: u16,
    pub grpc_service_port: u16,
    pub extra_args: Vec<String>,
}

impl ProxyConfig {
    fn to_args(&self) -> Vec<String> {
        let mut a = vec![
            // This proxy version dispatches on a subcommand; "shredstream"
            // requests shreds from Jito and forwards to local consumers.
            "shredstream".to_string(),
            "--block-engine-url".to_string(),
            self.block_engine_url.clone(),
            "--auth-keypair".to_string(),
            self.auth_keypair.clone(),
            "--desired-regions".to_string(),
            self.desired_regions.clone(),
            "--dest-ip-ports".to_string(),
            self.dest_ip_ports.clone(),
            "--src-bind-port".to_string(),
            self.src_bind_port.to_string(),
            "--grpc-service-port".to_string(),
            self.grpc_service_port.to_string(),
        ];
        a.extend(self.extra_args.iter().cloned());
        a
    }
}

/// Extract the port from a `http://host:port` gRPC endpoint. Falls back to
/// `default` if it cannot be parsed.
pub fn parse_grpc_port(endpoint: &str, default: u16) -> u16 {
    endpoint
        .rsplit(':')
        .next()
        .and_then(|s| s.trim_end_matches('/').parse::<u16>().ok())
        .unwrap_or(default)
}

/// Spawn a background task that runs the proxy and restarts it if it dies.
pub fn spawn_supervised(cfg: ProxyConfig) {
    tokio::spawn(async move {
        let args = cfg.to_args();
        let mut backoff = Duration::from_secs(2);
        loop {
            info!(bin = %cfg.bin, "launching jito-shredstream-proxy");
            let mut command = tokio::process::Command::new(&cfg.bin);
            command.args(&args).kill_on_drop(true);
            // ALWAYS silence the proxy's `solana_metrics` datapoint spam (the
            // per-second `packets_count=...` INFO lines and the giant
            // `deshred_missed_fec_sets` WARN dumps). We take whatever RUST_LOG the
            // operator set and append `solana_metrics=off`, so their chosen level
            // is preserved for every OTHER target but the metrics target is muted.
            let child_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
            command.env("RUST_LOG", format!("{child_log},solana_metrics=off"));
            match command.status().await {
                Ok(status) => {
                    warn!(?status, "shredstream proxy exited; restarting after backoff");
                    backoff = Duration::from_secs(2);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        bin = %cfg.bin,
                        "failed to launch shredstream proxy — is the binary installed and on PATH? \
                         set shred_arb.proxy_bin to its full path, or proxy_autostart=false to run it externally"
                    );
                    // Slower backoff on a launch failure to avoid a hot loop.
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
            tokio::time::sleep(backoff).await;
        }
    });
}
