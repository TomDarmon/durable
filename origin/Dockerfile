FROM rust:1.93-bookworm

WORKDIR /app
COPY durable ./durable
COPY origin ./origin

WORKDIR /app/origin
RUN cargo build --release --bin origin-server

EXPOSE 9200
CMD ["/app/origin/target/release/origin-server"]
