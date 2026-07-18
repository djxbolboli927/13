//! Jito Block-Engine gRPC client (SearcherService) — multi-region with auth.
//!
//! Mirrors the REST client design in `jito.rs`: each `send_bundle` call
//! broadcasts the same bundle to ALL configured regional endpoints
//! concurrently. The first regional success is returned to the caller.
//!
//! Per-region auth: each channel runs its own challenge/sign/token
//! handshake at startup, then attaches an `Authorization: Bearer <token>`
//! header to every SendBundle. Whitelisted keypairs get the higher Jito
//! rate limit (5 req/s/region vs 1 req/s for unauthenticated).
//!
//! If a region's auth fails (e.g. transient network error, keypair not
//! activated yet) that region downgrades to no-auth mode and keeps
//! sending without a Bearer token — other regions are unaffected.
//!
//! When auth fails, the underlying `tonic::Status` (code + message +
//! metadata) is logged so the operator can distinguish "challenge
//! expired", "invalid signature", "pubkey not approved", and rate-limit
//! responses from Jito's auth service.
//!
//! Duplicate prevention is the dispatcher's job: in `arbitrage.rs` each
//! profitable opportunity goes to EITHER the REST path OR the gRPC path
//! (REST first, gRPC fallback when REST limiter is empty).

use anyhow::{anyhow, Context, Result};
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};
use tracing::{debug, error, info, warn};

// Generated from proto/*.proto via build.rs + tonic_build.
mod pb {
    pub mod auth {
        tonic::include_proto!("auth");
    }
    pub mod bundle {
        tonic::include_proto!("bundle");
    }
    pub mod searcher {
        tonic::include_proto!("searcher");
    }
    pub mod packet {
        tonic::include_proto!("packet");
    }
    pub mod shared {
        tonic::include_proto!("shared");
    }
}

use pb::auth::{
    auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role,
};
use pb::bundle::Bundle;
use pb::packet::Packet;
use pb::searcher::{searcher_service_client::SearcherServiceClient, SendBundleRequest};

/// Active tokens returned by AuthService. Times are unix seconds.
#[derive(Clone)]
struct Tokens {
    access: String,
    access_expires_at: u64,
    refresh: String,
    refresh_expires_at: u64,
}

/// One Jito Block Engine region: its own channel and its own auth state.
struct Region {
    endpoint: String,
    channel: Channel,
    /// `None` = region runs in no-auth mode (auth never succeeded).
    tokens: Arc<RwLock<Option<Tokens>>>,
}

pub struct JitoGrpcClient {
    regions: Vec<Arc<Region>>,
}

impl JitoGrpcClient {
    /// Connect to every endpoint, attempt per-region auth, and spawn a
    /// refresh task for each successfully-authenticated region.
    /// Regions that fail to connect are skipped; regions that connect but
    /// fail auth run in no-auth mode.
    pub async fn new(endpoints: &[String], keypair_path: &str) -> Result<Self> {
        let keypair = Arc::new(
            crate::wallet::read_keypair(keypair_path)
                .with_context(|| format!("failed to load gRPC auth keypair from {keypair_path}"))?,
        );
        let auth_pubkey = keypair.pubkey();

        let mut regions: Vec<Arc<Region>> = Vec::new();
        let mut authed = 0usize;
        let mut no_auth = 0usize;

        for endpoint in endpoints {
            match Self::connect_region(endpoint, &keypair).await {
                Ok((region, was_authed)) => {
                    if was_authed {
                        authed += 1;
                    } else {
                        no_auth += 1;
                    }
                    regions.push(Arc::new(region));
                }
                Err(e) => {
                    warn!(
                        endpoint = %endpoint,
                        error = %e,
                        "Jito gRPC region failed to connect, skipping"
                    );
                }
            }
        }

        if regions.is_empty() {
            anyhow::bail!("no Jito gRPC regions could be reached");
        }

        info!(
            auth_pubkey = %auth_pubkey,
            regions_total = regions.len(),
            regions_authed = authed,
            regions_no_auth = no_auth,
            "Jito gRPC multi-region client initialized"
        );

        Ok(Self { regions })
    }

    /// Open one regional channel. Always returns successfully if the TCP/TLS
    /// connect works — auth failure downgrades to no-auth mode rather than
    /// erroring out.
    async fn connect_region(endpoint: &str, keypair: &Arc<Keypair>) -> Result<(Region, bool)> {
        let tls = ClientTlsConfig::new().with_webpki_roots();
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("invalid Jito gRPC endpoint {endpoint}"))?
            .tls_config(tls)
            .context("failed to configure TLS for Jito gRPC")?
            .timeout(Duration::from_secs(1))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .with_context(|| format!("failed to connect to Jito gRPC at {endpoint}"))?;

        let (tokens, was_authed) = match authenticate(&channel, keypair).await {
            Ok(initial) => {
                info!(
                    endpoint = %endpoint,
                    access_expires_at = initial.access_expires_at,
                    "Jito gRPC region authenticated"
                );
                let arc = Arc::new(RwLock::new(Some(initial)));

                let refresh_endpoint = endpoint.to_string();
                let refresh_channel = channel.clone();
                let refresh_tokens = arc.clone();
                let refresh_keypair = keypair.clone();
                tokio::spawn(async move {
                    token_refresh_loop(
                        refresh_endpoint,
                        refresh_channel,
                        refresh_tokens,
                        refresh_keypair,
                    )
                    .await;
                });

                (arc, true)
            }
            Err(e) => {
                // Surface every detail Jito returned so the operator can tell
                // "challenge expired" / "invalid signature" / "pubkey not
                // approved" / rate-limit apart.
                warn!(
                    endpoint = %endpoint,
                    error = %e,
                    error_chain = ?e,
                    "Jito gRPC region auth failed -- this region will send unauthenticated"
                );
                (Arc::new(RwLock::new(None)), false)
            }
        };

        Ok((
            Region {
                endpoint: endpoint.to_string(),
                channel,
                tokens,
            },
            was_authed,
        ))
    }

    /// Serialize `tx` once, broadcast to ALL regions concurrently.
    /// Returns the first regional success, or the last error if every
    /// region failed.
    ///
    /// Each region's SendBundle runs on a DETACHED task so the early return on
    /// first success no longer cancels the other regions' in-flight RPCs — the
    /// bundle really reaches every region.
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;

        let (res_tx, mut res_rx) =
            tokio::sync::mpsc::channel::<Result<String>>(self.regions.len().max(1));
        for region in &self.regions {
            let region = region.clone();
            let tx_bytes = tx_bytes.clone();
            let res_tx = res_tx.clone();
            tokio::spawn(async move {
                let r = send_to_region(&region, tx_bytes).await;
                let _ = res_tx.send(r).await;
            });
        }
        drop(res_tx);

        let mut last_err = None;
        while let Some(result) = res_rx.recv().await {
            match result {
                Ok(uuid) => return Ok(uuid), // other regions keep sending
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no Jito gRPC regions configured")))
    }
}

/// SendBundle to one region. Adds Bearer token if the region authenticated.
async fn send_to_region(region: &Region, tx_bytes: Vec<u8>) -> Result<String> {
    let bundle = Bundle {
        header: None,
        packets: vec![Packet {
            data: tx_bytes,
            meta: None,
        }],
    };

    let mut client = SearcherServiceClient::new(region.channel.clone());
    let mut req = Request::new(SendBundleRequest {
        bundle: Some(bundle),
    });

    if let Some(ref t) = *region.tokens.read().await {
        let auth_value: MetadataValue<_> = format!("Bearer {}", t.access)
            .parse()
            .map_err(|e| anyhow!("invalid access token: {e:?}"))?;
        req.metadata_mut().insert("authorization", auth_value);
    }

    let resp = tokio::time::timeout(Duration::from_secs(1), client.send_bundle(req))
        .await
        .with_context(|| format!("Jito gRPC SendBundle timed out at {}", region.endpoint))?
        .with_context(|| format!("Jito gRPC SendBundle failed at {}", region.endpoint))?;
    let uuid = resp.into_inner().uuid;
    debug!(endpoint = %region.endpoint, uuid = %uuid, "gRPC bundle accepted");
    Ok(uuid)
}

/// Run the AuthService challenge/sign/exchange dance and return fresh tokens.
///
/// Surfaces the underlying `tonic::Status` (code + message) on failure so
/// the operator can distinguish auth-server reasons (invalid signature,
/// pubkey not approved, challenge expired, rate limit).
async fn authenticate(channel: &Channel, keypair: &Keypair) -> Result<Tokens> {
    let mut auth = AuthServiceClient::new(channel.clone());
    let pubkey = keypair.pubkey();

    let challenge_resp = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await
        .map_err(status_to_anyhow("GenerateAuthChallenge"))?
        .into_inner();

    // Jito's auth service stores the challenge keyed by the pubkey-prefixed
    // form. BOTH the signed bytes AND the `challenge` field on the token
    // request must be `"{pubkey_base58}-{challenge}"` — sending the raw
    // `challenge_resp.challenge` back gives PermissionDenied "challenge not
    // found". Matches the variable shadowing in
    // jito-labs/searcher-examples/searcher_client/src/token_authenticator.rs.
    let challenge = format!("{}-{}", pubkey, challenge_resp.challenge);
    let signed = keypair.sign_message(challenge.as_bytes());
    let signed_bytes = signed.as_ref().to_vec();

    let token_resp = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_bytes().to_vec(),
            signed_challenge: signed_bytes,
        })
        .await
        .map_err(status_to_anyhow("GenerateAuthTokens"))?
        .into_inner();

    let access = token_resp
        .access_token
        .ok_or_else(|| anyhow!("no access_token in response"))?;
    let refresh = token_resp
        .refresh_token
        .ok_or_else(|| anyhow!("no refresh_token in response"))?;

    Ok(Tokens {
        access_expires_at: timestamp_secs(access.expires_at_utc.as_ref()),
        access: access.value,
        refresh_expires_at: timestamp_secs(refresh.expires_at_utc.as_ref()),
        refresh: refresh.value,
    })
}

/// Convert a `tonic::Status` into an anyhow error that preserves the gRPC
/// status code, message, and any trailing metadata returned by Jito.
fn status_to_anyhow(rpc: &'static str) -> impl Fn(Status) -> anyhow::Error {
    move |status: Status| {
        anyhow!(
            "{rpc} failed: code={:?} message={:?} metadata={:?}",
            status.code(),
            status.message(),
            status.metadata()
        )
    }
}

/// Per-region background loop that refreshes the access token before it
/// expires, falling back to a full re-auth when the refresh token is also
/// stale or RefreshAccessToken errors.
async fn token_refresh_loop(
    endpoint: String,
    channel: Channel,
    tokens: Arc<RwLock<Option<Tokens>>>,
    keypair: Arc<Keypair>,
) {
    loop {
        let sleep_secs = {
            let t = tokens.read().await;
            if let Some(ref t) = *t {
                let now = now_secs();
                t.access_expires_at
                    .saturating_sub(now)
                    .saturating_sub(60)
                    .max(10)
                    .min(300)
            } else {
                300
            }
        };
        tokio::time::sleep(Duration::from_secs(sleep_secs)).await;

        let needs_full_reauth = {
            let t = tokens.read().await;
            match *t {
                Some(ref t) => t.refresh_expires_at.saturating_sub(now_secs()) < 120,
                None => true,
            }
        };

        if needs_full_reauth {
            match authenticate(&channel, &keypair).await {
                Ok(new) => {
                    *tokens.write().await = Some(new);
                    info!(endpoint = %endpoint, "Jito gRPC tokens re-issued via full auth");
                }
                Err(e) => {
                    error!(endpoint = %endpoint, error = %e, "Jito gRPC re-auth failed, retry in 30s");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
            continue;
        }

        let refresh_token = {
            let t = tokens.read().await;
            t.as_ref().map(|t| t.refresh.clone())
        };
        let Some(refresh_token) = refresh_token else {
            continue;
        };

        let mut auth = AuthServiceClient::new(channel.clone());
        match auth
            .refresh_access_token(RefreshAccessTokenRequest { refresh_token })
            .await
        {
            Ok(resp) => {
                let inner = resp.into_inner();
                if let Some(new_access) = inner.access_token {
                    let mut w = tokens.write().await;
                    if let Some(ref mut t) = *w {
                        t.access_expires_at = timestamp_secs(new_access.expires_at_utc.as_ref());
                        t.access = new_access.value;
                    }
                    debug!(endpoint = %endpoint, "Jito gRPC access token refreshed");
                } else {
                    warn!(endpoint = %endpoint, "RefreshAccessToken returned empty access token, will re-auth");
                }
            }
            Err(e) => {
                warn!(
                    endpoint = %endpoint,
                    code = ?e.code(),
                    message = %e.message(),
                    "RefreshAccessToken failed, falling back to full auth"
                );
                match authenticate(&channel, &keypair).await {
                    Ok(new) => {
                        *tokens.write().await = Some(new);
                    }
                    Err(e2) => {
                        error!(endpoint = %endpoint, error = %e2, "Jito gRPC re-auth also failed");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
            }
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn timestamp_secs(ts: Option<&prost_types::Timestamp>) -> u64 {
    match ts {
        Some(t) => t.seconds.max(0) as u64,
        None => 0,
    }
}
