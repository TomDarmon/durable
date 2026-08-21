use origin::{local_rustfs_config, serve_http};
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::var("ORIGIN_BIND").unwrap_or_else(|_| "0.0.0.0:9200".into());
    let address: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    serve_http(listener, local_rustfs_config()).await?;
    Ok(())
}
