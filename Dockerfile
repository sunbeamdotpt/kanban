# Build: docker buildx build --platform linux/amd64 -f apps/kanban/Dockerfile -t src.sunbeam.pt/studio/kanban:latest .
# Multi-stage build for kanban service (Rust + sqlx migrations)

FROM rust:1.88-bookworm AS builder

WORKDIR /app

# Install build dependencies (protobuf compiler for tonic-prost-build)
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy workspace and dependencies
COPY . .

# Build the kanban binary (sqlx::migrate! is embedded at compile time)
RUN cargo build --release --bin kanban -p kanban

# Runtime image: distroless with root CA certs
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /app/target/release/kanban /usr/local/bin/kanban

EXPOSE 8080
USER 65532
CMD ["/usr/local/bin/kanban"]
