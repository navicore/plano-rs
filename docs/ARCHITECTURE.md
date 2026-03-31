# Architecture

## Overview

plano-rs is a Rust workspace for extracting Postgres tables into Parquet files
and querying them with SQL via DataFusion. It supports local and S3-backed
object stores, partitioned Parquet datasets, and exposes an HTTP query server
with Prometheus metrics.

## Workspace Crates

```
crates/
  core/           plano-core     Shared utilities (output formatting: JSON, CSV, text)
  api/            plano-api      Protobuf definitions for gRPC (analytics proto, not yet wired)
  rds-sync/       rds-sync       Library for reading Postgres tables into Arrow RecordBatches
  bin/
    plano-sync/   plano-sync     CLI: extracts a Postgres table to Parquet files with optional partitioning
    plano-repl/   plano-repl     CLI/REPL: runs SQL queries against local Parquet files via DataFusion
    plano-serv/   plano-serv     HTTP server: accepts SQL queries, serves results from partitioned Parquet
```

## Key Data Flows

### Extract (plano-sync)
1. Connects to Postgres via `DATABASE_URL`
2. `rds-sync::infer_arrow_schema` reads `information_schema.columns` to build an Arrow schema
3. `rds-sync::sync_table` fetches all rows into a `RecordBatch`
4. Writes output as single Parquet file or partitioned directory (Hive-style: `col=val/`)
5. Partitioning supports time-derived keys (year, month, day, hour) from a `--timestamp-col`

### Query - REPL (plano-repl)
1. Registers local Parquet files (via glob patterns) as DataFusion tables
2. Accepts one-shot `--query` or interactive REPL mode (vi keybindings, persistent history)
3. Outputs via `plano-core::format` in text, CSV, or JSON

### Query - Server (plano-serv)
1. Parses `--table-spec` args: `name=path[:partition_cols]` (supports `file://` and `s3://`)
2. Registers each object store URL with DataFusion, wrapped in `MetricsObjectStore`
3. Registers `ListingTable`s with partition columns, deduplicating partition keys from file schema
4. Serves HTTP on `--bind` (default `127.0.0.1:8080`):
   - `POST /query` — accepts `sql=...` form body, returns JSON/CSV/text based on `Accept` header
   - `GET /tables` — lists registered tables
5. Prometheus metrics exposed on port 9898 at `/metrics`

## Object Store Layer

`MetricsObjectStore` wraps any `ObjectStore` impl and increments `metrics`
counters for each operation (get, put, list, copy, delete, etc.).

Caching via `ocra::ReadThroughCache` with LRU backend and `PlanoCacheStats`
(atomic counters + metrics gauges) is implemented but **currently disabled** due
to an `object_store` version conflict. The code is commented out in
`plano-serv/src/main.rs`.

## Dependencies

Core: `datafusion`, `arrow`, `parquet`, `object_store` (with S3 support)
Server: `warp`, `metrics`, `metrics-exporter-prometheus`
Sync: `sqlx` (Postgres), `clap`
REPL: `rustyline`, `glob`, `clap`
gRPC (unused): `tonic`, `prost`
