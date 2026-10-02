# Governor: resource and lease semantics

Governor decides how much work a tree of tasks is allowed to create. It does not
run the work. This document is the contract the implementation is tested against.

## 1. Model

### 1.1 The tree

All state hangs off one tree owned by a single `governord` process:

```text
org (root)
 ├─ tenant A            depth 1: the fairness group
 │   ├─ task            a root execution
 │   │   ├─ subtask
 │   │   └─ subtask
 │   └─ task
 └─ tenant B
```

A node's **chain** is the node plus all of its ancestors up to the root. A node's
**tenant** is its ancestor at depth 1 (or itself at depth 0 or 1). Shared downstream
pools such as `db` and `http` are limits set on the root, so one rule covers both
"a task's budget" and "what Postgres can take".

Every node is in exactly one state:

```text
            ┌──────────► Done               Close()
Active ─────┼──────────► Cancelled          Cancel(), or an ancestor ended
            └──────────► DeadlineExceeded   deadline passed
```

Terminal states are final. Ending a node ends its whole subtree in the same step:
the node gets the requested state, and still-active descendants become `Cancelled`.

### 1.2 Limits are caps

A limit on a node is a **cap, not a reservation**. Creating a child sets nothing
aside. A child may carry its own tighter cap, and every operation is checked
against the whole chain.

## 2. Three primitives

They are deliberately not one generic counter.

| Primitive | Examples | Shape |
| --- | --- | --- |
| **Quota** | `http`, `sql`, `retry`, `tool_calls` | Monotonic count. Consumed, never returned. |
| **Lease** | concurrency classes `agents`, `db`, `http` | Held capacity. Returned on release, expiry or revocation. |
| **Deadline** | wall clock | Neither consumed nor returned. Inherited as a minimum. |

Dollar cost and tokens are not resources in this version.

## 3. Operations

All operations are linearizable: the engine applies them one at a time. "Atomic"
below means no other operation can observe an intermediate state.

### 3.1 Consume

`Consume(session, node, resource, n)`

1. `n ≥ 1`, the session is live, the node is in its scope (§4.1) and `Active`.
   Otherwise fail.
2. For every node `a` on the chain that has a limit for `resource`:
   if `used(a) + n > limit(a)`, fail with `Denied` and change nothing.
3. Otherwise add `n` to `used` on every node of the chain.

A consume is all-or-nothing and is never partially applied. A denial names the
node whose cap was hit, its usage and limit, and the largest single consumer
beneath it.

Quota is never refunded: not on cancel, not on close, not on worker death. A
request that was sent stays sent.

### 3.2 Acquire

`Acquire(ctx, session, node, class) → lease`

1. Session live, node in its scope and `Active`. Otherwise fail.
2. The request **fits** if every node on the chain with a limit for `class` has
   `held < limit`.
3. If it fits, grant immediately: `held += 1` on every node of the chain.
4. Otherwise wait in the class queue (§6) until granted, or until the context is
   cancelled, the node ends, or the session expires.

A lease may carry a **maximum hold time**. When it passes, the lease ends as
`expired` even if its session is live.

### 3.3 Release

`Release(session, lease)` ends the lease and decrements `held` along the chain.
Its result depends on how the lease ended first:

| Lease is… | Result | Effect |
| --- | --- | --- |
| held by this session | ok | released |
| held by another session | `NotOwner` | none |
| already released | ok | none (idempotent) |
| expired with its session or its hold time | `LeaseExpired` | none |
| revoked because its node ended | `LeaseRevoked` | none |
| never issued, or forgotten after retention | `UnknownLease` | none |

A lease ends exactly once. Nothing that happens afterwards can decrement `held`
again.

### 3.4 Cancel, close and deadlines

A node's effective deadline is `min(own deadline, parent's effective deadline)`.
When it passes, the topmost node whose deadline has passed becomes
`DeadlineExceeded` and its subtree ends with it.

Ending a node, for any terminal state, in one atomic step:

1. revokes all leases held in the subtree and frees their capacity,
2. fails all queued acquires in the subtree,
3. rejects every later `Consume`, `Acquire` and `CreateNode` on the subtree.

Ending a node that has already ended is a no-op. The root cannot be ended.

An ended node is remembered for the **node retention**, a day by default, and is
then removed together with its subtree. What it consumed stays in the totals of
its ancestors. A session confined to a removed node is ended. After the removal
the node's id is unknown, and its name can be given to a new node.

### 3.5 Changing caps and defaults

Caps can be changed while a node is active.

- `SetQuota(session, node, resource, cap)` and `SetLimit(session, node, class, cap)`
  replace one cap, or remove it. Nothing already consumed or held is taken back:
  a quota set below what is used only refuses further consumes, and a lowered
  limit revokes no lease (§7).
- A session may change every node in its scope **except the scope node itself**,
  so it cannot loosen the caps it is held to. The root has nothing above it, so a
  session scoped to the root may change the root too.
- `SetDefaults(session, node, defaults)` sets what each **new child** of the node
  starts with: a set of quotas, and optionally the defaults those children pass
  on to their own children. Defaults replace the quotas they name in the child's
  spec and leave the rest of the spec alone. Nodes that already exist are not
  changed. A session may set defaults on its own scope, since they only shape
  what is created beneath it.
- `Describe(session, node)` reports a node and its children: state, caps, usage,
  leases held and defaults.

## 4. Sessions and fencing

A worker process opens a **session** with a TTL. Every lease belongs to one
session. One `Heartbeat(session)` renews the session and therefore all its leases.

A session expires when `now ≥ expiresAt`. This is evaluated against the engine
clock on every operation that names the session, and by the reaper, so the outcome
does not depend on reaper timing. Expiry:

1. ends every lease of the session as `expired` and frees its capacity,
2. fails every queued acquire of the session,
3. deletes the session. Its id is never reused.

Session ids and lease ids come from one monotonic sequence and are never reissued.
A lease id is therefore its own **fencing token**: once a lease has ended, no
operation carrying that id can succeed, and a larger id is always a later grant.

### 4.1 Scope

A session is opened on one node, its **scope**, and may only act inside that
node's subtree. `CreateNode`, `Consume`, `Acquire`, `Cancel`, `Close`, the
operations of §3.5 and watching a node all take a session, and fail with `Forbidden` when the node they name is
outside its scope. A forbidden call changes nothing. The scope is fixed for the
life of the session.

A session scoped to a tenant therefore cannot spend, hold or cancel anything that
belongs to another tenant. A session scoped to the root can act anywhere.
`Release` needs no scope check, because a lease can only be released by the
session that holds it (§3.3).

Over the API a client never chooses its scope. `OpenSession` carries an API key,
`governord` maps the key to a node from its configuration, and the reply carries
a random 128-bit token that names the session on every later call. Numeric
session ids never leave the server, so they cannot be guessed.

## 5. Invariants

These hold between any two operations. The tests check them after every step of
randomized operation sequences.

- **I1 Quota bound.** No consume takes a node over a cap: after every granted
  consume, `used ≤ limit` on its whole chain. Usage can only sit above a cap
  because the cap was lowered afterwards (§3.5), and then every consume through
  that node is denied.
- **I2 Quota conservation.** `used(n) = self(n) + Σ used(children) + gone(n)`,
  where `gone` is the usage of children that have been removed. Usage never decreases.
- **I3 Lease conservation.** `held(n, class) = leases held directly at n + Σ held(children, class)`.
- **I4 Lease bound.** `held ≤ limit`, except transiently after a limit is lowered
  (§7). In that case no lease is granted through that node until `held < limit`.
- **I5 Single end.** Each lease ends exactly once.
- **I6 No orphans.** Every live lease belongs to a live session and an `Active` node.
- **I7 Terminal is final.** A terminal node never becomes active and never admits
  an operation.
- **I8 Work conserving.** After every operation, no queued acquire fits.
- **I9 Isolation.** A session never changes a node outside its scope: every live
  lease sits inside its session's scope, and a forbidden call leaves usage,
  leases and node states as they were.

I1 together with I2 is the headline property: *no descendant can consume more than
remains in any ancestor's envelope.*

## 6. Fair scheduling

Fairness only exists where there is a queue, so it lives in lease acquisition.
Per class there is one queue per tenant, ordered by priority (higher first) and
then arrival. Tenants are served by deficit round robin:

1. Starting at the cursor, find the first tenant that has a waiter that fits.
   Tenants skipped on the way forfeit their remaining deficit.
2. If that tenant's deficit is zero, set it to the tenant's weight.
3. Grant its first fitting waiter and decrement the deficit. When the deficit hits
   zero, move the cursor to the next tenant.
4. Repeat until no waiter fits.

A tenant with 10,000 waiters and a tenant with 2 at equal weight alternate grants.
A waiter that is blocked only by its own subtree's cap never blocks other tenants,
or other tasks of its own tenant.

Priority within a tenant is strict: a steady stream of high-priority work can
starve low-priority work of the same tenant.

## 7. Adaptive concurrency

A controller owns the limit of one class on one node, normally the root. Every
interval it looks at the latencies and overload signals reported with lease
releases in that interval:

```text
if samples < minSamples:              no change
else if p95 > target or overload rate > maxOverload:
    limit = max(minLimit, floor(limit × 0.7))
else:
    limit = min(maxLimit, limit + 1)
```

Every change is recorded with the inputs that caused it. Lowering a limit never
revokes leases: grants through that node stop until enough leases are returned.

## 8. Retry budget

`retry` is an ordinary quota. Every retry attempt, at any layer, consumes one unit
from the node in its context and therefore from the whole chain. The first attempt
is free. When the budget is exhausted the last error is returned immediately, so
nested retry loops are bounded by the budget rather than by the product of their
attempt counts.

## 9. Durability and crashes

`governord` keeps the tree in memory and uses a database as the durable record:
Postgres, or a SQLite file when everything runs on one machine. The guarantees
below are the same for both.

- **Write before reply.** Every state change is committed to the database before the
  caller receives its answer. Changes are batched into one transaction per flush.
- **Idempotent requests.** A request that creates a node, consumes or acquires
  may carry a request id. The id is recorded with the change it caused, so
  repeating it returns the original result and applies nothing twice, also after
  a restart. A session remembers its 1,024 most recent ids. Ids sent directly
  with an API key, as hooks do, are remembered only until the next restart.
- **Snapshots.** Once enough events have accumulated, the whole state is saved as
  a snapshot and the events it covers are deleted, in one transaction. The time a
  restart takes therefore depends on the size of the state, not on its history.
- **Restart.** Nodes, usage, live leases and sessions are rebuilt from the newest
  snapshot and the events recorded after it. Session tokens are stored as hashes, so a worker carries on
  with the token and leases it had. Each session gets one fresh TTL to reconnect;
  sessions that do not heartbeat expire. A session with no stored token could
  never be used again, so it is closed at start.
- **Fail closed.** While `governord` or its database is unreachable, clients deny new
  work. Work holding a lease stops when its session's local deadline passes.

| Crash happens… | Outcome |
| --- | --- |
| before the commit | The caller gets an error. Nothing was applied. |
| after the commit, before the reply | The change is durable. The caller sees an error and retries with the same request id. |
| while a release is uncommitted | The lease is still held after restart, until the release is retried or the session ends. |

## 10. Agent CLI hooks

Claude Code and Codex both run a command before every tool call and refuse the
tool if the command exits with code 2. `governor hook` is that command for both.
It handles one event, `PreToolUse`, and keeps no state between calls.

On each tool call it:

1. finds or creates the run's node, named `<cli>:<session_id>`, under the tenant;
   the node gets its quotas and a deadline when it is first created. The quotas
   are the tenant's defaults (§3.5); the hook's own flags only fill in what the
   defaults do not name;
2. inside a subagent, finds or creates the subagent's node under the run, which
   costs the run one `agents` unit, charged in the same step as the creation. The
   subagent's quotas are the run's defaults, which it got from the tenant's;
3. consumes one `tool_calls` unit from that node.

If any step is refused, the hook exits 2 and prints the reason for the model.

- **Find or create.** `EnsureNode` returns the newest child with a given name even
  if it has ended. A run that was cancelled or timed out therefore stays refused
  for as long as the node retention; asking again within it never replaces the
  run with a fresh budget.
- **Calls by API key.** A hook is a short-lived process, so `EnsureNode` and
  `Consume` accept the tenant's API key directly. The server keeps one standing
  session per key for these calls. Leases still need a session of their own.
- **Fail closed.** A malformed event, a wrong key or an unreachable `governord`
  blocks the tool. Neither CLI treats a crashed or timed-out hook as a refusal,
  so the hook bounds its own wait and answers explicitly.
- **No concurrency limit.** A lease taken before a tool call would have to be
  released by a different process after it, so hooks use quotas and deadlines only.
- **Flat subagents.** Hooks expose no parent link, so every subagent node sits
  directly under the run, whatever its real nesting depth.

## 11. Failure scenarios

| Scenario | Outcome |
| --- | --- |
| Two workers consume 80 each from a 100 budget | One succeeds, one is denied. Never both. |
| Worker releases a lease twice | Second call is a no-op. |
| Worker dies holding leases | Session TTL lapses, leases expire, capacity goes to waiters. |
| Worker pauses past its TTL, then wakes | Its session is gone. Heartbeat, consume, acquire and release all fail. The SDK cancels everything running under that session. |
| Node cancelled while workers hold leases | Leases revoked, capacity freed, later operations rejected. |
| Acquire context cancelled while queued | Waiter removed. If a grant raced with the cancel, the lease is released again. |
| Parent closed with children running | Children become `Cancelled`. |
| A lease holder never releases, such as an unclosed response body | The lease ends at its maximum hold time. |
| An agent CLI is killed mid-run | Nothing is held, so nothing leaks; the run's node ends at its deadline. |
| A subagent is over the run's `agents` budget | It gets no node, and every tool call it makes is refused. |

## 12. What is not guaranteed

- **Governor cannot stop a paused worker.** Between a session expiring and the
  worker noticing, the worker may still perform an operation it believed was
  covered by a lease. Three mitigations bound this: the server rejects every stale
  call; the SDK stops work at a local deadline derived from the last successful
  heartbeat on a monotonic clock, minus a safety margin; and a downstream that
  cares can call `Validate(lease)` before doing irreversible work.
- **Quota counts permission, not delivery.** A unit is charged when the consume is
  granted, before the request is sent. Work that is granted and then never
  performed stays charged, so a budget may be under-used but never exceeded.
- **One `governord` is the authority.** There is no replication. Availability comes
  from fast restart and durable state, not from consensus.
- **Isolation is only as strong as the keys.** Traffic is encrypted only when
  `governord` is given a certificate, and API keys sit in the configuration
  file. A process that can read another tenant's key or token can act as that
  tenant; keeping them apart is the operating system's job.
- **Hooks see tool calls only.** Model calls and tokens are not gated, a shell
  command that makes many requests counts as one tool call, and a tool that is
  already running cannot be interrupted. Codex does not run hooks for its hosted
  tools, such as web search.
- **Hooks guard against accidents, not a hostile agent.** The agent's process can
  read its own API key and can edit the hook configuration. With the key it can
  raise the budget of its own run, up to the tenant's caps, which the key cannot
  change; and it can remove the hook.
