# PostgreSQL with pg_duckdb, pg_parquet, and tidx_abi for Ethereum indexing
# - pg_duckdb: DuckDB's columnar analytical engine for OLAP queries
# - pg_parquet: Native Parquet COPY support using PostgreSQL indexes
# - tidx_abi: Ethereum ABI decoding functions for log parsing

ARG PGDUCKDB_VERSION=16-v0.3.1
ARG DUCKDB_VERSION=v1.2.0
ARG PG_PARQUET_VERSION=v0.5.1
ARG PGRX_VERSION=0.16.0

# =============================================================================
# Stage 1: Build tidx_abi DuckDB extension
# =============================================================================
FROM rust:1.87 AS tidx-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    python3 \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY tidx-abi/src ./src
COPY tidx-abi/Cargo.toml tidx-abi/Cargo.lock* ./

RUN cargo build --release

ARG DUCKDB_VERSION
RUN git clone --depth 1 --branch ${DUCKDB_VERSION} https://github.com/duckdb/extension-ci-tools.git /extension-ci-tools

RUN python3 /extension-ci-tools/scripts/append_extension_metadata.py \
    --library-file target/release/libtidx_abi.so \
    --extension-name tidx_abi \
    --duckdb-platform linux_amd64 \
    --extension-version "0.1.0" \
    --duckdb-version "${DUCKDB_VERSION}"

# =============================================================================
# Stage 2: Build pg_parquet extension
# =============================================================================
FROM pgduckdb/pgduckdb:${PGDUCKDB_VERSION} AS parquet-builder

ARG PG_PARQUET_VERSION
ARG PGRX_VERSION

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    pkg-config \
    libssl-dev \
    clang \
    libclang-dev \
    postgresql-server-dev-16 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cargo-pgrx (must match pg_parquet's pgrx version)
RUN cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked

# Initialize pgrx with the system PostgreSQL
RUN cargo pgrx init --pg16=$(which pg_config)

# Clone and build pg_parquet
WORKDIR /build
RUN git clone --depth 1 --branch ${PG_PARQUET_VERSION} https://github.com/CrunchyData/pg_parquet.git
WORKDIR /build/pg_parquet
RUN cargo pgrx install --release --pg-config=$(which pg_config)

# Verify extension was installed correctly
RUN ls -la $(pg_config --sharedir)/extension | grep pg_parquet
RUN ls -la $(pg_config --pkglibdir) | grep pg_parquet

# =============================================================================
# Stage 3: Final image with pg_duckdb + pg_parquet + tidx_abi
# =============================================================================
FROM pgduckdb/pgduckdb:${PGDUCKDB_VERSION}

# Create extensions directory for DuckDB
RUN mkdir -p /usr/share/duckdb/extensions

# Copy tidx_abi DuckDB extension
COPY --from=tidx-builder /build/tidx_abi.duckdb_extension /usr/share/duckdb/extensions/

# Copy pg_parquet extension files
COPY --from=parquet-builder /usr/share/postgresql/16/extension/pg_parquet* /usr/share/postgresql/16/extension/
COPY --from=parquet-builder /usr/lib/postgresql/16/lib/pg_parquet.so /usr/lib/postgresql/16/lib/

# Create init script to load extensions
COPY init-pg-duckdb.sql /docker-entrypoint-initdb.d/

# Create data directory for Parquet files with proper permissions
USER root
RUN mkdir -p /data && chown postgres:postgres /data && chmod 755 /data
USER postgres
