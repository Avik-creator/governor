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

![governor ui listing the Claude Code and Codex sessions of one tenant with their budgets](docs/ui.png)

*`governor ui` showing the Claude Code and Codex sessions of one tenant: one has
spent its tool-call budget and is refused further tools, one is close to it.*

The exact semantics are in [SPEC.md](SPEC.md), which is the contract the tests check.

**Contents:** [How it works](#how-it-works) ·
[Quick start](#quick-start) ·
[Using the SDK](#using-the-sdk) ·
[Governing Claude Code and Codex](#governing-claude-code-and-codex) ·
[Watching and changing budgets](#watching-and-changing-budgets) ·
[Benchmark](#benchmark) ·
[What is guaranteed](#what-is-guaranteed) ·
[Layout](#layout) ·
[Development](#development) ·
[Status](#status)

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
- **Caps can be changed while work runs.** `governor ui` shows what every task has
  used and edits its caps, and the defaults new tasks start with.
- **Postgres or SQLite is the durable record.** Every change is committed before
  the caller gets its reply, in batches. A restart rebuilds the tree from the
  newest snapshot and the changes recorded after it, and a request retried across
  the restart is not applied twice.

## Quick start

You need Go 1.26 or later. A database is optional: without one `governord` keeps
everything in memory.

```sh
# 1. Settings the example configuration reads from the environment.
export GOVERNOR_DATABASE_URL="sqlite:$PWD/governor.db"
export GOVERNOR_ADMIN_KEY=admin-secret TEAM_A_KEY=team-a-secret TEAM_B_KEY=team-b-secret

# 2. Run the daemon.
go run ./cmd/governord -config governor.example.yaml

# 3. In another terminal, with the same variables set, watch the tree.
go run ./cmd/governor ui -config governor.example.yaml
```

`governor.example.yaml` declares the shared pools and one node per tenant.
`GOVERNOR_DATABASE_URL` chooses where the record is kept:

| Value | Record | Use it when |
| --- | --- | --- |
| `postgres://…` | Postgres | `governord` serves several machines |
| `sqlite:/path/to/governor.db` | One file, no other process needed | everything runs on one machine |
| empty | None; a restart forgets everything | trying it out, tests |

For Postgres, any database will do, for example:

```sh
docker run -d --name governor-pg -e POSTGRES_PASSWORD=governor -e POSTGRES_DB=governor \
  -p 5432:5432 postgres:17-alpine
export GOVERNOR_DATABASE_URL='postgres://postgres:governor@localhost:5432/governor?sslmode=disable'
```

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

## Governing Claude Code and Codex

`governor hook` caps what an agent CLI run may do: how many tool calls it makes,
how many subagents it uses, and for how long it may keep going. It works the same
way in both tools, as a `PreToolUse` command hook.

```sh
go install github.com/Avik-creator/governor/cmd/governor@latest
export GOVERNOR_API_KEY=team-a-secret      # GOVERNOR_ADDR defaults to 127.0.0.1:7600
```

Claude Code, in `~/.claude/settings.json`:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "*",
        "hooks": [
          {
            "type": "command",
            "command": "governor hook --source claude",
            "timeout": 10
          }
        ]
      }
    ]
  }
}
```

Codex, in `~/.codex/hooks.json`. Codex asks you to review and trust a new hook
before it runs it:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": ".*",
        "hooks": [
          {
            "type": "command",
            "command": "governor hook --source codex",
            "timeout": 10
          }
        ]
      }
    ]
  }
}
```

When a budget is spent the tool is refused and the model is told why:

```text
Governor blocked this tool: the tool_calls budget of "claude:4f2a" is spent (500 of 500 used).
Do not retry; stop and tell the user the budget is exhausted.
```

A run is one session of the CLI. What each run may use comes from the tenant's
`defaults` in `governor.yaml`: `quotas` is the budget of a run, and `children` is
the budget of each subagent.

```yaml
tenants:
  - name: team-a
    api_key: ${TEAM_A_KEY}
    defaults:
      quotas: {tool_calls: 500, agents: 10}
      children:
        quotas: {tool_calls: 100}
```

The tenant's own `quotas` cap all runs together. If `governord` is not running,
tools are blocked: the hook fails closed. The `matcher` decides which tools count.

| Flag | Meaning | Default |
| --- | --- | --- |
| `--ttl` | How long after its first tool call a run is refused everything | 24h |
| `--timeout` | How long to wait for `governord` before refusing | 5s |
| `--tool-calls`, `--agents`, `--agent-tool-calls` | Budgets used only where the tenant has no default | no cap |

Resuming a session continues its run, because both CLIs give a resumed session the
same id; forking one, or starting a new one, begins a new run. A session resumed
after its `--ttl` stays blocked, so set `--ttl` to the longest you expect to keep
resuming a session.

Checked with Claude Code 2.1.285 and Codex 0.151.0: in both, the first command ran
and the second was refused with the budget message when the budget was one tool call.

## Watching and changing budgets

`governor ui` is a terminal screen over the same tree. It lists the runs of a
tenant with what each has used, refreshed every second. Usage turns yellow from
80% of a cap and red once the cap is reached.

![The list of runs](docs/ui.png)

Opening a run with `enter` shows its subagents, each with its own budget:

![The subagents of one run](docs/ui-agents.png)

`e` opens the caps of the selected run. Here a run that has spent its 500 tool
calls is being given 800, which lets it carry on from its next tool call:

![Editing the caps of a run](docs/ui-edit.png)

| Key | What it does |
| --- | --- |
| `↑` `↓` | Move between runs |
| `enter` | Open a run to see its subagents; `esc` goes back |
| `e` | Edit the caps of the selected run: raise a spent budget, lower one, or empty the field to remove it |
| `d` | Edit the defaults: what every new run, and every subagent, starts with |
| `c` | Cancel a run, after asking; every later tool call of that run is refused |
| `r` | Read again now, without waiting for the next refresh |
| `q` | Quit |

```sh
GOVERNOR_API_KEY=team-a-secret governor ui     # one tenant's runs
governor ui -config governor.yaml              # on the machine governord runs on
```

With `-config` the screen takes the address and the admin key from governord's own
file, and then starts at the root, one level above the tenants.

A change takes effect at the next tool call and is part of the durable record. A
tenant's key can change everything under the tenant but not the tenant's own caps;
the admin key can change those too. Defaults set here take the place of the ones
in `governor.yaml`, which only seed a tenant that has none on record.

## Benchmark

`govbench` runs the same workload with and without Governor against a simulated
downstream service, then breaks things on purpose to show the recovery.

```sh
go run ./cmd/govbench                                   # five scenarios, no setup
go run ./cmd/govbench -database-url "$THROWAWAY_DSN"    # adds restart and Postgres overhead
```

Results from one run on an Apple M1 (the raw output is in [docs/benchmark.txt](docs/benchmark.txt)):

| Scenario | Metric | Without Governor | With Governor |
| --- | --- | --- | --- |
| **Runaway fan-out**: 121 tasks want 363 requests from a service that takes 10 at once | Requests sent | 363 | 150 (the budget) |
| | Requests shed by the service | 322 | 0 |
| | Peak concurrency at the service | 81 | 6 |
| **Noisy neighbour**: tenant A queues 400 jobs just before tenant B queues 10 | Tenant B's p95 wait | 550 ms | 29 ms |
| | Tenant A's finish time | 560 ms | 606 ms |
| **Retry storm**: 20 calls, three nested retry loops, service down | Requests per call | 27 | 2 |

| Scenario | Metric | Fixed limit | Adaptive limit |
| --- | --- | --- | --- |
| **Capacity drop**: the service's capacity falls from 20 to 5 mid-run | Requests shed afterwards | 4,781 | 296 |
| | Requests served afterwards | 208 | 595 |

| Failure | What happened |
| --- | --- |
| **Worker dies holding all 4 slots**, on a 1 s session | Another tenant got the pool after 1,009 ms. All 4 late releases by the dead worker were refused. |
| **`governord` stopped and restarted mid-run** | Down for 120 ms, during which 24 calls were refused. All 2,986 acknowledged charges were on record afterwards, and the worker's session and lease were still valid. |

| Overhead over loopback gRPC | In memory | With Postgres |
| --- | --- | --- |
| Consume, one caller | 70 µs | 1.3 ms |
| Acquire and release, one caller | 139 µs | 2.9 ms |
| Consume, 16 callers | 59,000 per second | 4,400 per second |

How to read these:

- **The downstream is simulated.** The numbers show how Governor behaves, not how a
  production system performs.
- **The adaptive limit still sheds some requests.** It keeps probing upward by one,
  and each probe past the real capacity costs a few rejections.
- **Postgres costs about a millisecond per call**, because every change is committed
  before its reply. Concurrent callers share commits, so throughput scales better
  than the single-caller time suggests.
- **Timings vary between machines and runs**, and a busy machine makes them several
  times worse; the counts do not. These were measured on an idle machine, before
  snapshots and durable request ids were added.
- **A call in flight when `governord` stops can be on record without having been
  acknowledged.** It is never the other way round: nothing a worker was told
  succeeded is lost.

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
| `cmd/governor` | The command-line client: `governor hook` and `governor ui` |
| `cmd/govbench` | The benchmark |
| `internal/daemon` | governord's startup, importable so the benchmark can restart it |
| `internal/bench` | The benchmark's scenarios and simulated downstream |
| `internal/hook` | The hook's logic for Claude Code and Codex |
| `internal/tui` | The terminal screen of `governor ui` (Bubble Tea) |
| `internal/core` | The in-memory engine: tree, quotas, leases, fair queue, restore |
| `internal/server` | The gRPC service: authentication, error mapping, idempotent requests |
| `internal/adaptive` | The controller that tunes a limit from reported latency and overload |
| `internal/store` | Postgres or SQLite: events with group commit, snapshots, session token hashes (goose, goqu) |
| `internal/config` | The YAML configuration |
| `proto/governor/v1` | The gRPC contract; generated code is in `internal/gen` |

## Development

```sh
go test ./...                    # everything that needs no Postgres; the store runs on SQLite
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

Built and tested: the engine, the gRPC service, the Postgres and SQLite store with
snapshots and restart, `governord` with adaptive concurrency, the SDK, the hook for
Claude Code and Codex, the terminal screen, and the benchmark.

Known limits:

- one `governord` is the authority; there is no replication;
- traffic is not encrypted, and API keys are kept in the configuration file;
- hooks budget tool calls and subagents, but cannot limit how many run at once;
- request ids sent directly with an API key, as hooks do, are not kept across a restart.

## License

MIT. See [LICENSE](LICENSE).
