# plano-rs

A Rust-based analytics sync and query server using Apache Arrow and gRPC.

# UNDER CONSTRUCTION

# UNDER CONSTRUCTION

# UNDER CONSTRUCTION

setup / install via:

``
brew install protobuf duckdb postgres
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
cargo run -p sync-cli -- --table users
cargo run -p sync-cli -- --table employees
cargo run -p sync-cli -- --table crm
```

Query parquet files

```
duckdb -c "SELECT * FROM '/tmp/users.parquet' LIMIT 5"
```

Use the query cli for multi file multi table queries

```
cargo run -p query-cli -- \
  --table users="/tmp/users*.parquet" \
  --table employees="/tmp/employees*.parquet" \
  --table crm="/tmp/crm*.parquet" \
  --query "SELECT employees.id, employees.name, crm.cust_email FROM employees JOIN crm ON employees.email = crm.rep_email"
```


