FROM rust:1.93-bookworm

WORKDIR /app
COPY durable ./durable
COPY origin ./origin

WORKDIR /app/origin
RUN cargo build --release --bins

EXPOSE 9200 9210
CMD ["/app/origin/target/release/origin-server"]
