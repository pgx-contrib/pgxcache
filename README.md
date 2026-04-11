# pgxcache

[![CI](https://github.com/pgx-contrib/pgxcache/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxcache/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxcache)](https://github.com/pgx-contrib/pgxcache/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxcache.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxcache)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxcache)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pgx-contrib/pgxcache)](go.mod)
[![pgx Version](https://img.shields.io/badge/pgx-v5-blue)](https://github.com/jackc/pgx)

Query result caching layer for [pgx v5](https://github.com/jackc/pgx).

## Features

- Transparent caching of `Query`, `QueryRow`, `Exec`, and `SendBatch` results
- Inline SQL annotation directives control caching per query
- Built-in `MemoryQueryCacher` backed by [ristretto](https://github.com/dgraph-io/ristretto)
- Pluggable `QueryCacher` interface — bring your own backend (Redis, Memcached, etc.)
- Works with any `pgx`-compatible connection: `*pgx.Conn`, `*pgxpool.Pool`, or `pgx.Tx`

## Installation

```bash
go get github.com/pgx-contrib/pgxcache
```

## Usage

### Basic pool setup

```go
pool, err := pgxpool.New(ctx, os.Getenv("PGX_DATABASE_URL"))
if err != nil {
    panic(err)
}

querier := &pgxcache.Querier{
    // Default options applied when a query has no inline annotations.
    Options: &pgxcache.QueryOptions{
        MinRows:     1,
        MaxRows:     1000,
        MaxLifetime: 30 * time.Second,
    },
    Cacher:  pgxcache.NewMemoryQueryCacher(),
    Querier: pool,
}

rows, err := querier.Query(ctx, "SELECT id, name FROM users WHERE active = $1", true)
```

### Per-query annotations

Embed cache directives in SQL comments. Annotations override the default `Options` for that query:

```sql
-- @cache-max-lifetime 5m
-- @cache-min-rows 1
-- @cache-max-rows 500
SELECT id, name FROM users WHERE active = true
```

```go
rows, err := querier.Query(ctx, `
    -- @cache-max-lifetime 5m
    -- @cache-min-rows 1
    SELECT id, name FROM users WHERE active = $1`, true)
```

### Custom cacher

Implement the `QueryCacher` interface to use any cache backend:

```go
type QueryCacher interface {
    Get(context.Context, *QueryKey) (*QueryItem, error)
    Set(context.Context, *QueryKey, *QueryItem, time.Duration) error
    Reset(context.Context) error
}
```

## Annotation directives

| Directive | Value | Description |
|---|---|---|
| `@cache-max-lifetime` | `<n>s`, `<n>m`, `<n>h` | How long to cache the result |
| `@cache-min-rows` | `<n>` | Skip caching if the result has fewer than `n` rows |
| `@cache-max-rows` | `<n>` | Skip caching if the result has more than `n` rows |

## Development

### Using nix devshell

```bash
nix develop
go tool ginkgo run -r
```

### Using devcontainer

```bash
devcontainer up --workspace-folder .
devcontainer exec --workspace-folder . go tool ginkgo run -r
```

### Running tests

```bash
# Unit tests only (no database required)
go test ./...

# Full suite with Ginkgo CLI
go tool ginkgo run -r

# Integration tests (requires Postgres)
PGX_DATABASE_URL="postgres://vscode@localhost:5432/pgxcache?sslmode=disable" \
  go tool ginkgo run -r
```

## License

[MIT](LICENSE)
