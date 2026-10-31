use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use engineering_challenge::collector::exchange::kalshi::{Credentials, Kalshi};
use engineering_challenge::collector::{self, Config, output};
use tokio_util::sync::CancellationToken;

/// Collect orderbooks for every live Kalshi market, writing SBE snapshots
/// (every minute and on source changes) and price level updates.
#[derive(Parser, Debug)]
struct Cli {
    /// Output file; "-" writes to stdout. Logs always go to stderr.
    #[arg(long, short, default_value = "-")]
    output: String,

    /// Redundant websockets per shard.
    #[arg(long, default_value_t = 2)]
    replicas: usize,

    /// Markets per shard (each shard has `--replicas` sockets). Kalshi has
    /// ~170k mostly-illiquid markets.
    #[arg(long, default_value_t = 1000)]
    symbols_per_shard: usize,

    /// Only collect this many symbols (useful for testing).
    #[arg(long)]
    max_symbols: Option<usize>,

    #[arg(long, default_value_t = 60)]
    discovery_interval_secs: u64,

    /// Reconnect each socket roughly this often (±25% jitter).
    #[arg(long, default_value_t = 1800)]
    recycle_interval_secs: u64,

    /// Total websockets to open. Defaults to 95% of the exchange's limit
    /// (Kalshi: 200 per account); beyond it, shards grow past --symbols-per-shard.
    #[arg(long)]
    max_connections: Option<usize>,

    #[arg(long, env = "KALSHI_API_KEY_ID")]
    kalshi_key_id: Option<String>,

    /// Path to the PEM private key for the Kalshi API key (RSA or Ed25519).
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    kalshi_private_key: Option<PathBuf>,

    /// Use Kalshi's demo environment.
    #[arg(long)]
    kalshi_demo: bool,
}

fn main() -> anyhow::Result<()> {
    // Load `.env` (if any) first, so it can supply RUST_LOG and the
    // env-backed flags. Variables already set in the environment win.
    let dotenv = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    match dotenv {
        Ok(path) => tracing::info!("loaded {}", path.display()),
        Err(e) if e.not_found() => {}
        Err(e) => anyhow::bail!("reading .env: {e}"),
    }
    let cli = Cli::parse();
    anyhow::ensure!((1..=32).contains(&cli.replicas), "--replicas must be 1..=32");
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cfg = Config {
        replicas: cli.replicas,
        symbols_per_shard: cli.symbols_per_shard,
        max_symbols: cli.max_symbols,
        discovery_interval: Duration::from_secs(cli.discovery_interval_secs),
        recycle_interval: Duration::from_secs(cli.recycle_interval_secs),
        max_connections: cli.max_connections,
        ..Config::default()
    };

    let path = if cli.output == "-" {
        PathBuf::from("/dev/stdout")
    } else {
        PathBuf::from(&cli.output)
    };
    let cancel = CancellationToken::new();
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
    let writer = output::spawn_writer(path, out_rx, cancel.clone())?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    tracing::info!("SIGINT received; closing websockets and output");
                    cancel.cancel();
                }
                // A second SIGINT skips the graceful shutdown.
                if tokio::signal::ctrl_c().await.is_ok() {
                    std::process::exit(130);
                }
            }
        });

        let credentials = match (&cli.kalshi_key_id, &cli.kalshi_private_key) {
            (Some(id), Some(path)) => {
                let path = expand_home(path);
                let pem = std::fs::read_to_string(&path)
                    .with_context(|| format!("reading {}", path.display()))?;
                Some(Credentials::new(id.clone(), &pem)?)
            }
            _ => {
                tracing::warn!(
                    "no Kalshi credentials (KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH); \
                     the websocket will likely reject the connection"
                );
                None
            }
        };
        let exchange = Arc::new(Kalshi::new(cli.kalshi_demo, credentials));
        collector::run(exchange, cfg, out_tx, cancel).await;
        anyhow::Ok(())
    })?;
    // Don't wait on lingering tasks (e.g. an in-flight REST request).
    runtime.shutdown_timeout(Duration::from_secs(1));

    // Every sender is gone once all shards have exited, so the writer drains
    // the remaining messages, flushes, and closes the file.
    let stats = writer
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    tracing::info!(
        "output closed: {} snapshots, {} updates",
        stats.snapshots,
        stats.updates
    );
    Ok(())
}

/// Expand a leading `~/`, which shells do but `.env` files and Rust don't.
fn expand_home(path: &std::path::Path) -> PathBuf {
    match (path.strip_prefix("~"), std::env::var_os("HOME")) {
        (Ok(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => path.to_path_buf(),
    }
}
