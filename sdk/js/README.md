# @avik-creator/governor

A Node.js client for [Governor](https://github.com/Avik-creator/governor), a
distributed resource governor for autonomous and fan-out workloads.

Services that share a database, a payment gateway or an LLM API ask `governord`
before they use it. `governord` decides who may go now, who waits, and who has
spent their budget, so one busy service cannot starve the others.

```sh
npm install @avik-creator/governor
```

Node.js 18 or newer.

## Concepts

Governor keeps a tree: the root, then one **tenant** per service, then the
**tasks** each service creates. Everything a task uses counts against its tenant
and the root as well.

- A **quota** is a budget, such as 3 retries or 50,000 tokens. Each `consume`
  spends from it, and it never refills. Once it is spent, `consume` is refused.
- A **limit** caps how many **leases** of a class are held at once, such as 10
  database connections. `acquire` waits while the limit is full, and `release`
  frees the slot.
- An **adaptive** limit follows the health of what it guards: it is cut to 70%
  when releases report slow or overloaded calls, and raised by one while they
  are healthy.
- When a limit is full, tenants are served in proportion to their **weight**.

## Starting governord

Install it with Go, or build it from the repository:

```sh
go install github.com/Avik-creator/governor/cmd/governord@latest
```

Give it a configuration with one tenant per service:

```yaml
listen: 127.0.0.1:7600

root:
  limits:
    db: 40    # connections the database accepts
    psp: 20   # calls the payment gateway takes at once

adaptive:
  - class: psp
    target_p95: 300ms
    max_overload: 0.02
    min_limit: 1
    max_limit: 20

tenants:
  - name: payments
    api_key: ${PAYMENTS_KEY}
    weight: 4
    defaults:
      quotas:
        retry: 3   # every task under payments starts with 3 retries
  - name: reconciliation
    api_key: ${RECON_KEY}
    weight: 1
    limits:
      db: 5
      psp: 2
```

```sh
export PAYMENTS_KEY=change-me RECON_KEY=change-me-too
governord -config governor.yaml
```

`governor.example.yaml` in the repository describes every setting, including a
database for durability and TLS for serving other machines.

## Quick start

```js
import { connect } from '@avik-creator/governor';

// One session per process; it belongs to the tenant whose key it uses.
const session = await connect({ apiKey: process.env.PAYMENTS_KEY });

// One task per unit of work, such as a request or a job.
const task = await session.newTask({ name: 'checkout-42', deadlineMs: 3000 });
try {
  const lease = await task.acquire('psp');
  const start = performance.now();
  const res = await fetch('https://psp.example.com/capture', { method: 'POST' });
  await lease.release({ latencyMs: performance.now() - start, overloaded: res.status === 429 });
} finally {
  await task.close();
}

await session.close();
```

## Guides

### Guarding a call

Hold a lease only while the call runs, and always release it. Reporting the
latency and whether the other side was overloaded is what drives an adaptive
limit.

```js
// guarded runs fn while holding a lease of cls, and reports how it went.
async function guarded(task, cls, fn, isOverloaded = () => false) {
  const lease = await task.acquire(cls);
  const start = performance.now();
  let result;
  try {
    result = await fn();
    return result;
  } finally {
    await lease.release({ latencyMs: performance.now() - start, overloaded: isOverloaded(result) });
  }
}

const res = await guarded(task, 'psp', () => fetch(url, { method: 'POST' }), (r) => r?.status === 429);
```

### Guarding a database

Set the class's limit to what the database accepts, and run every query under a
lease. Each service's own pool can stay large; Governor keeps the total in check.

```js
import pg from 'pg';

const pool = new pg.Pool({ max: 20 });

// query runs one statement under a db lease.
async function query(task, text, values) {
  const lease = await task.acquire('db');
  try {
    return await pool.query(text, values);
  } finally {
    await lease.release();
  }
}
```

### Retrying within a budget

Charge one unit of a `retry` quota before every retry. Every retry under the
task draws from the same budget, so nested retries cannot multiply.

```js
// retry runs fn until it succeeds or the task's retry budget is spent.
async function retry(task, fn) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      // A refused charge ends the retries; the original error is the one to report.
      await task.consume('retry').catch(() => { throw err; });
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 100 * 2 ** attempt));
    }
  }
}
```

Give the quota to tasks through the tenant's `defaults`, as in the
configuration above. A quota set on the tenant itself is a budget for the
tenant's whole life.

### Deadlines

`deadlineMs` ends the task that long after it is created. From then on, its
calls are refused and its leases are freed, so work that has run too long
stops holding capacity.

```js
const task = await session.newTask({ name: `order-${id}`, deadlineMs: 5000 });
```

### Budgets for a task

A task can carry its own quotas and limits, within its tenant's:

```js
// An agent run may call the LLM 20 times, with at most 3 calls at once.
const run = await session.newTask({
  name: 'research-agent',
  quotas: { llm_calls: 20, tokens: 50_000 },
  limits: { llm: 3 },
});

await run.consume('llm_calls');
await run.consume('tokens', usage.total_tokens);
```

### A batch job

Make the whole batch one task and share it between the workers. They then
share its caps, and the tenant's limits keep the batch from taking a whole pool.

```js
const job = await session.newTask({ name: 'nightly-reconciliation' });
await Promise.all(workers.map((w) => w.run(job)));
await job.close();
```

### Fencing writes

A lease id only ever grows, so it can guard the write itself. Store it with the
row, and a worker that wakes up after a long pause cannot overwrite newer work:

```sql
UPDATE payments SET status = 'captured', fence = $1
WHERE id = $2 AND fence < $1;
```

To check that a lease is still held before a step that cannot be undone:

```js
await session.rpc('Validate', { leaseId: lease.id });   // throws if it expired or was revoked
```

### Shutting down

Close the session when the process stops. If the process dies instead,
`governord` frees its leases once the session's TTL passes without a heartbeat.

```js
process.on('SIGTERM', async () => {
  await session.close();
  process.exit(0);
});
```

## Errors

Every failure is a `GovernorError`, with the gRPC status code in `code` and the
reason in `message`.

| `code` | Status | Meaning |
| ---: | --- | --- |
| 8 | `RESOURCE_EXHAUSTED` | A quota is spent; `err.denied` is true |
| 9 | `FAILED_PRECONDITION` | The task has ended, or the lease expired or was revoked |
| 16 | `UNAUTHENTICATED` | The API key is wrong, or the session expired |
| 7 | `PERMISSION_DENIED` | The node or lease belongs to another tenant or session |
| 5 | `NOT_FOUND` | No such node or lease |
| 3 | `INVALID_ARGUMENT` | The request is malformed |
| 14 | `UNAVAILABLE` | `governord` cannot be reached |

```js
import { GovernorError } from '@avik-creator/governor';

try {
  await task.consume('retry');
} catch (err) {
  if (err instanceof GovernorError && err.denied) {
    // The budget is spent: give up rather than retry.
  }
}
```

## Connecting

| Option | Environment | Default |
| --- | --- | --- |
| `addr` | `GOVERNOR_ADDR` | `127.0.0.1:7600` |
| `tls` | `GOVERNOR_TLS` | off |
| `caFile` | `GOVERNOR_CA_FILE` | the system's roots |
| `ttlMs` | | `10000` |

These are the variables the Go SDK, the hook and the CLI read, so one setup
serves every client. `GOVERNOR_TLS` takes `true` or `false` (or `1`, `0`); any
other value is an error rather than plain text. `caFile` turns TLS on and trusts
only the certificates in that file.

With TLS, give `addr` as a host name the certificate names, such as
`localhost:7600`; Node does not allow an IP address as the server's name.

```js
const session = await connect({
  apiKey: process.env.PAYMENTS_KEY,
  addr: 'governor.internal:7600',
  caFile: '/etc/governor/ca.pem',
});
```

The session sends a heartbeat three times per `ttlMs`, so one lost heartbeat
does not end it.

## API

| | |
| --- | --- |
| `connect({ apiKey, addr, tls, caFile, ttlMs })` | Opens a session for the key's tenant |
| `session.newTask({ name, deadlineMs, quotas, limits })` | Creates a task under the tenant |
| `session.scopeId` | The tenant's node id |
| `session.rpc(method, request)` | Calls any method of the API as this session |
| `session.close()` | Ends the session and frees every lease it holds |
| `task.consume(resource, amount = 1)` | Charges a quota |
| `task.acquire(cls)` | Waits for a lease; one held for 60 s is freed |
| `task.close()` | Ends the task |
| `task.id` | The task's node id |
| `lease.release({ latencyMs, overloaded })` | Returns the lease and reports how the call went |
| `lease.id` | The lease id, which is also its fencing token |

`session.rpc` reaches the rest of the API, such as `GetNode`, `CancelNode` and
`SetQuota`. Requests use the field names of `governor.proto` in camelCase, and
64-bit ids travel as strings.

## Example

[`examples/checkout`](https://github.com/Avik-creator/governor/tree/main/examples/checkout)
runs a checkout service and a batch job against one database and one payment
gateway, with and without Governor, and prints what each side saw.
