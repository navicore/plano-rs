# Roadmap

## Current State

The core extract-and-query pipeline works end to end:
- Postgres-to-Parquet sync with partitioning (plano-sync)
- SQL queries over Parquet via CLI/REPL (plano-repl)
- HTTP query server with Prometheus metrics (plano-serv)

## Known TODOs

From README and code comments:

- **gRPC API** — protobuf definitions exist in `crates/api` but are not wired into any server
- **REST API** — replace raw SQL-over-HTTP with a proper REST-ish API to remove SQL from the wire
- **Refactor engine into core** — the REPL currently has its own DataFusion setup; the server's engine logic should move to `plano-core` so both share it
- **Re-enable ocra caching** — `ReadThroughCache` with `InMemoryCache` and `PlanoCacheStats` is fully implemented but disabled due to `object_store` version conflict (ocra requires 0.11.2, workspace uses 0.12.1+)
