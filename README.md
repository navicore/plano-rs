[![Dependabot Updates](https://github.com/navicore/plano-rs/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/navicore/plano-rs/actions/workflows/dependabot/dependabot-updates) [![rust-clippy analyze](https://github.com/navicore/plano-rs/actions/workflows/rust-clippy.yml/badge.svg)](https://github.com/navicore/plano-rs/actions/workflows/rust-clippy.yml) [![Publish-Crate](https://github.com/navicore/plano-rs/actions/workflows/publish-crates.yml/badge.svg)](https://github.com/navicore/plano-rs/actions/workflows/publish-crates.yml)

# plano-rs

A Rust-based analytics sync and query server using Apache Arrow, Parquet,
Datafusion, and gRPC (tbd).

This is an experimental POC while I try to learn Apache Arrow / Datafusion.

To learn the parts and create a system that can be reasoned about the default
object store is wrapped with an implementation wired with the [metrics](https://crates.io/crates/metrics) crate
functions.  Also, to measure hits and misses / spill, the cache is wrapped with
the [orca](https://crates.io/crates/ocra) `PassThroughCache` implementation and that impl is injected with a
metrics implementation of `orca`'s `CachedStats` trait.

Status:

  * [x] A loader that will extract any table from Postres (AWS RDS) and create
  partitioned directories of Parquet files. (`complete`)

  * [x] A CLI and REPL that can process SQL against any Parquet file. (`complete`)

  * [x] an HTTP server processing SQL queries resolving from data lazily loaded
    and cached from partitioned directories of Parquet files. (`complete`)

  * [x] Metrics showing spills and cache hits and memory usage available at the
    standard Prometheus metrics endpoint. (`complete`)

  * [ ] gRPC (`todo`)

  * [ ] A proper REST-ish API so we can remove SQL from the wire (`todo`)

  * [ ] Refactor the engine wrapper into the core module so that the REPL can
    run it instead of the limited version it has embedded. (`todo`)


```
             ┌───────────────┐
             │    Client     │
             │(curl, Grafana)│
             └───────┬───────┘
                     │ HTTP
                     ▼
        ┌─────────────────────────────┐
        │     HTTP Server (warp)      │
        │  /query  → plano-serv API   │
        │  /metrics → Prometheus exp. │
        └─────────────┬───────────────┘
                      │ calls
                      ▼
        ┌─────────────────────────────┐
        │   Query Engine Layer        │
        │  (DataFusion SessionCtx)    │
        └─────────────┬───────────────┘
                      │ reads
                      ▼
┌───────────────────────────────────────────────────────┐
│                  ObjectStore Layer                    │
│ ┌───────────────────────────────────────────────────┐ │
│ │             ReadThroughCache                      │ │
│ │ ┌───────────────────────────────────────────────┐ │ │
│ │ │  InMemoryCache (LRU backend)                  │ │ │
│ │ └───────────────────────────────────────────────┘ │ │
│ │             │ invokes, caches, tracks stats       │ │
│ │ ┌───────────────────────────────────────────────┐ │ │
│ │ │  PlanoCacheStats impl of CacheStats           │ │ │
│ │ │  – AtomicU64s for reads/misses/usage          │ │ │
│ │ │  – emits counter!(…) & gauge!(…) metrics      │ │ │
│ │ └───────────────────────────────────────────────┘ │ │
│ │             │ delegates on miss                   │ │
│ │ ┌───────────────────────────────────────────────┐ │ │
│ │ │  Base ObjectStore                             │ │ │
│ │ │  – LocalFileSystem (file://) or               │ │ │
│ │ │  – AmazonS3Builder → S3 (s3://)               │ │ │
│ │ └───────────────────────────────────────────────┘ │ │
│ └───────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────┘
                      │ metrics
                      ▼
             ┌───────────────────┐
             │ Prometheus        │
             │ Exporter (/metrics│
             │   port 9898)      │
             └───────────────────┘

```


setup dev env and install via:

on mac:
``
brew install protobuf duckdb postgres
git clone <this repo>
cd <this repo>
cargo build --release --workspace
see target/release/
``

Run postgres

```
podman run --name plano-postgres \
  -e POSTGRES_USER=plano \
  -e POSTGRES_PASSWORD=plano \
  -e POSTGRES_DB=plano_dev \
  -p 5432:5432 \
  -d docker.io/library/postgres:15

#later...
restart $(docker ps -aq -f status=exited)
```

Connect

```
psql postgres://plano:plano@localhost:5432/plano_dev
```

Export ENV VAR

```
export DATABASE_URL=postgres://plano:plano@localhost:5432/plano_dev
```

Use psql to load schema and data

```
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  created_at TIMESTAMP DEFAULT now()
);

INSERT INTO users (name, email) VALUES
('Alice', 'alice@example.com'),
('Richard', 'richard@example.com'),
('Bob', 'bob@example.com');

CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  created_at TIMESTAMP DEFAULT now()
);

INSERT INTO employees (name, email) VALUES
('Mary', 'mary@big.com'),
('Joe', 'joe@big.com');

CREATE TABLE crm (
  rep_email TEXT,
  cust_email TEXT,
  created_at TIMESTAMP DEFAULT now()
);

INSERT INTO crm (rep_email, cust_email) VALUES
('mary@big.com', 'alice@example.com'),
('mary@big.com', 'bob@example.com'),
('joe@big.com', 'richard@example.com');
```

Run the sync / extract command

```
cargo run -p plano-sync -- -t signalk_2 -p name -p year --timestamp-col navigation_position_timestamp --output-dir /tmp/parquet
```

Query parquet files

```
duckdb -c "SELECT * FROM '/tmp/users.parquet' LIMIT 5"
```

Use the query cli for multi file multi table queries

```
cargo run -p plano-query -- \
  --table users="/tmp/users*.parquet" \
  --table employees="/tmp/employees*.parquet" \
  --table crm="/tmp/crm*.parquet" \
  --query "SELECT employees.id, employees.name, crm.cust_email FROM employees JOIN crm ON employees.email = crm.rep_email"
```

for plano-serv:

```
cargo run -p plano-serv -- --table-spec 'signalk=/tmp/parquet/signalk_2:name,year'
```

```
curl -H "Accept: application/json" -X POST -d "sql=SELECT * FROM signalk LIMIT 5" http://127.0.0.1:8080/query | jq

curl -H "Accept: text/plain" -X POST -d "sql=SELECT * FROM signalk LIMIT 5" http://127.0.0.1:8080/query

curl -H "Accept: text/csv" -X POST -d "sql=SELECT * FROM signalk LIMIT 5" http://127.0.0.1:8080/query
```
