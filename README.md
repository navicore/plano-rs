# plano-rs

A Rust-based analytics sync and query server using Apache Arrow and gRPC.

# UNDER CONSTRUCTION

# UNDER CONSTRUCTION

# UNDER CONSTRUCTION

setup / install via:

``
brew install protobuf
brew install postgres # just to get psql
``

Run postgres

```
podman run --name plano-postgres \
  -e POSTGRES_USER=plano \
  -e POSTGRES_PASSWORD=plano \
  -e POSTGRES_DB=plano_dev \
  -p 5432:5432 \
  -d docker.io/library/postgres:15
```

Connect

```
psql postgres://plano:plano@localhost:5432/plano_dev
```

Export ENV VAR

```
export DATABASE_URL=postgres://plano:plano@localhost:5432/plano_dev
```


