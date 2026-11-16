# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.15-slim

RUN pip install --no-cache-dir uv \
    && uv tool install mcp-proxy

COPY --from=builder /app/target/release/crunchyroll-mcp /usr/local/bin/crunchyroll-mcp

ENV PATH="/root/.local/bin:$PATH"

EXPOSE 8808

# MCP_ALLOWED_HOSTS: comma-separated Host authorities (e.g. 10.10.50.216:8808 or
# mcp.example.com:*) besides localhost that mcp-proxy accepts requests for.
CMD ["sh", "-c", "set --; for h in $(echo \"$MCP_ALLOWED_HOSTS\" | tr ',' ' '); do set -- \"$@\" --allowed-host \"$h\"; done; exec mcp-proxy --host 0.0.0.0 --port 8808 --allow-origin '*' \"$@\" --pass-environment -- crunchyroll-mcp"]
