# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim

RUN pip install --no-cache-dir uv \
    && uv tool install mcp-proxy

COPY --from=builder /app/target/release/crunchyroll-mcp /usr/local/bin/crunchyroll-mcp

ENV PATH="/root/.local/bin:$PATH"

EXPOSE 8808

CMD ["mcp-proxy", \
     "--host", "0.0.0.0", \
     "--port", "8808", \
     "--allow-origin", "*", \
     "--", \
     "crunchyroll-mcp"]
