FROM rust:1.89-slim-bullseye as builder


RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml ./
COPY src/ ./src/

RUN cargo build --release

FROM debian:bullseye-slim


RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/reddit_crawler_rs /app/

RUN mkdir -p /data/input /data/output


COPY scripts/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]