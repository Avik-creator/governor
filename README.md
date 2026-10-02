# Governor

A resource governor for autonomous and fan-out workloads.

An agent that spawns sub-agents, a crawler that follows every link, a job that
retries at three layers: each can create far more work than anyone intended.
Governor is a small daemon, `governord`, that decides how much work a tree of
tasks may create. It does not run the work. It answers three questions, and it
answers them the same way for a task, its subtasks and the tenant that owns them:

- **May this task spend one more unit?** (quotas: HTTP calls, SQL transactions, retries, tool calls)
- **May this task run one more thing at once?** (leases: database connections, concurrent agents)
- **Is this task still allowed to run?** (deadlines and cancellation)

The exact semantics are in [SPEC.md](SPEC.md), which is the contract the tests check.

## How it works

```text
org (root)                    limits here are the shared pools: db, http, agents
 ├─ tenant A                  the fairness group, one API key
 │   ├─ task                  a root execution
 │   │   ├─ subtask
 │   │   └─ subtask
 │   └─ task
 └─ tenant B
```

- **Caps, not reservations.** A limit on a node is a ceiling for its whole subtree.
  Every operation is checked against the node and all of its ancestors, and is
  applied to all of them or to none.
- **Quota is never refunded.** A request that was sent stays sent.
- **Leases are crash-safe.** Every lease belongs to a session with a TTL. When a
  worker dies, its session lapses and its capacity goes to whoever is waiting.
- **Lease ids are fencing tokens.** They only ever grow, so a downstream system can
  reject a stale holder.
- **Tenants are isolated.** A session opened with a tenant's API key can only act
  inside that tenant's subtree.
- **Tenants are treated fairly.** Queued acquires are served by weighted deficit
  round robin across tenants, so a tenant with 10,000 waiters cannot starve one
  with 2.
- **Limits can adapt.** A controller raises a pool's limit by one while its
  downstream is healthy and cuts it to 70% when latency or overload rises.
- **Postgres is the durable record.** Every change is committed before the caller
  gets its reply, in batches, and a restart rebuilds the tree by replaying it.

## Quick start

You need Go 1.26 or later. Postgres is optional: without it `governord` keeps
everything in memory.

```sh
# 1. A Postgres for the durable record (optional).
docker run -d --name governor-pg -e POSTGRES_PASSWORD=governor -e POSTGRES_DB=governor \
  -p 5432:5432 postgres:17-alpine

# 2. Settings the example configuration reads from the environment.
export GOVERNOR_DATABASE_URL='postgres://postgres:governor@localhost:5432/governor?sslmode=disable'
export GOVERNOR_ADMIN_KEY=admin-secret TEAM_A_KEY=team-a-secret TEAM_B_KEY=team-b-secret

# 3. Run the daemon.
go run ./cmd/governord -config governor.example.yaml
```

`governor.example.yaml` declares the shared pools and one node per tenant. Set
`GOVERNOR_DATABASE_URL` to an empty string to run without Postgres.

## Using the SDK

```sh
go get github.com/Avik-creator/governor
```

The client reads `GOVERNOR_ADDR` (default `127.0.0.1:7600`) and `GOVERNOR_API_KEY`.
A program connects once and creates a task; from then on the `context.Context`
carries the task, so library code needs no handle.

```go
client, err := governor.Dial(ctx)
if err != nil {
    return err
}
defer client.Close()

// The context is cancelled when the task ends, for any reason.
ctx, task, err := client.NewTask(ctx, governor.Spec{
    Name:     "crawl-site",
    Quotas:   map[string]int64{"http": 500, "retry": 20},
    Deadline: time.Now().Add(10 * time.Minute),
})
if err != nil {
    return err
}
defer task.Close()

// Quota: denied with the cap that was hit and its largest consumer.
if err := governor.Consume(ctx, "tool_calls", 1); err != nil {
    return err
}

// HTTP: one "http" unit and one "http" lease per request.
httpClient := &http.Client{Transport: governor.Transport(nil)}

// SQL: one "sql" unit and one "db" lease per transaction, written with goqu.
db := governor.NewDB("postgres", sqlDB)
err = db.Tx(ctx, func(tx *goqu.TxDatabase) error {
    _, err := tx.Insert("pages").Rows(page).Executor().ExecContext(ctx)
    return err
})

// Retries at every layer draw on one shared "retry" budget.
err = governor.Retry(ctx, func(ctx context.Context) error { return fetch(ctx, httpClient) })

// Fan-out: each function is a subtask that waits for an "agents" lease.
g, ctx := governor.NewGroup(ctx, governor.ClassAgents)
for _, page := range pages {
    g.Go(governor.Spec{Name: page.URL}, func(ctx context.Context) error { return crawl(ctx, page) })
}
err = g.Wait()
```

The SDK fails closed. If `governord` cannot be reached, work is refused. If a
heartbeat has not succeeded within 80% of the session's TTL, every context from
that client is cancelled, so a paused worker cannot carry on with leases it has lost.

## What is guaranteed

SPEC.md §5 states nine invariants. The headline ones:

| | Invariant |
| --- | --- |
| I1, I2 | No descendant can consume more than remains in any ancestor's envelope. |
| I3, I4 | Leases held never exceed a limit anywhere on the chain. |
| I5 | Every lease ends exactly once. |
| I6 | Every live lease belongs to a live session and an active node. |
| I8 | No queued acquire is left waiting when it would fit. |
| I9 | A session never changes a node outside its scope. |

They are checked from scratch after every step of randomized operation sequences
(40 seeds of 1,500 steps), and each random history is also replayed through the
restart path and compared field by field with the live engine.

SPEC.md §12 lists what is not guaranteed, including that there is one `governord`
with no replication, and that traffic is not encrypted.

## Layout

| Path | Contents |
| --- | --- |
| `*.go` (root) | The Go SDK, package `governor` |
| `cmd/governord` | The daemon: restore, reconcile tenants, serve |
| `internal/core` | The in-memory engine: tree, quotas, leases, fair queue, restore |
| `internal/server` | The gRPC service: authentication, error mapping, idempotent requests |
| `internal/adaptive` | The controller that tunes a limit from reported latency and overload |
| `internal/store` | Postgres: events with group commit, session token hashes (goose, goqu) |
| `internal/config` | The YAML configuration |
| `proto/governor/v1` | The gRPC contract; generated code is in `internal/gen` |

## Development

```sh
go test ./...                    # everything that needs no database
go test -race ./...              # the same, with the race detector

# Tests that need Postgres empty their tables, so use a throwaway database
# and run packages one at a time.
GOVERNOR_TEST_DSN='postgres://postgres:governor@localhost:5432/governor?sslmode=disable' \
  go test -p 1 ./...

go generate ./internal/gen       # regenerate the gRPC code after editing the proto
```

Measured on an Apple M1 with `go test -bench . ./internal/core`:

| Operation | Time |
| --- | --- |
| Consume, 8 cores | 327 ns |
| Acquire and release, uncontended | 607 ns |
| Acquire and release, queued behind 2 slots | 1.3 µs |

## Status

Built and tested: the engine, the gRPC service, the Postgres store with restart,
`governord` with adaptive concurrency, and the SDK.

Not built yet:

- the `governor hook` command that governs Claude Code through its hooks (SPEC.md §10);
- the benchmark that compares a workload with and without Governor;
- snapshots, so restart time does not grow with history.

## License

MIT. See [LICENSE](LICENSE).
