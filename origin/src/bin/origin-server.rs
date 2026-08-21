use origin::{local_rustfs_config, serve_http_with_cache_dir};
use std::{net::SocketAddr, path::PathBuf};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::var("ORIGIN_BIND").unwrap_or_else(|_| "0.0.0.0:9200".into());
    let cache_dir =
        PathBuf::from(std::env::var("ORIGIN_CACHE_DIR").unwrap_or_else(|_| ".origin-cache".into()));
    let address: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    serve_http_with_cache_dir(listener, local_rustfs_config(), cache_dir).await?;
    Ok(())
}
