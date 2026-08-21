use origin::{local_rustfs_config, serve_http_with_cache_dir};
use std::{net::SocketAddr, path::PathBuf};
use tokio::net::TcpListener;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let bind = std::env::var("ORIGIN_BIND").unwrap_or_else(|_| "0.0.0.0:9200".into());
    let cache_dir =
        PathBuf::from(std::env::var("ORIGIN_CACHE_DIR").unwrap_or_else(|_| ".origin-cache".into()));
    let address: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    let config = local_rustfs_config();
    tracing::info!(
        address = %address,
        cache_dir = %cache_dir.display(),
        rustfs_endpoint = %config.endpoint,
        "starting origin server"
    );
    serve_http_with_cache_dir(listener, config, cache_dir).await?;
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("origin=info"));
    fmt()
        .with_ansi(false)
        .with_env_filter(filter)
        .compact()
        .init();
}
