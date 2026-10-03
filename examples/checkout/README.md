# Checkout demo (Node.js)

A payments service and a nightly reconciliation job share one Postgres and one
payment gateway. The job is heavy: it takes most of the database connections and
floods the gateway, so checkouts slow down and fail. This demo runs both for
8 seconds without Governor, then 8 seconds with it, and prints what each side saw.

It is written in JavaScript, with the Node.js client
[`@avik-creator/governor`](../../sdk/js), to show that `governord` is not tied
to Go: any language with gRPC can use it.

## What it shows

One run on a laptop:

|                           | without Governor | with Governor |
| ------------------------- | ---------------: | ------------: |
| checkouts paid within 3 s |              337 |           719 |
| checkouts failed          |               89 |            10 |
| checkout p50 (ms)         |              188 |           102 |
| checkout p95 (ms)         |              288 |           183 |
| checkout retries          |              638 |           214 |
| gateway calls             |            2,896 |         1,309 |
| gateway 429s              |            1,769 |           318 |
| reconciliation steps done |              790 |           272 |

- **Twice the checkouts get paid**, because the job can hold at most 3 of the
  10 database connections and 2 of the gateway's slots.
- **The gateway sees 82% fewer 429s.** Its limit starts at 8, above the
  gateway's real capacity of 6, and Governor lowers it as releases report 429s.
  `governord` logs each change, such as `limit changed class=psp old=8 new=5`.
- **The job does less**, and that is the trade: it is a batch, and it can wait.

The database and the gateway are simulated inside `demo.js`, so the numbers show
behaviour, not production performance.

## Running it

You need Node.js 18 or newer, and `governord`, either built from this repository
or installed:

```sh
go install github.com/Avik-creator/governor/cmd/governord@latest
```

In one terminal, from this folder, start `governord` with its configuration. The keys are
any strings you choose; `governord` refuses to start unless all three are set.

```sh
export PAYMENTS_KEY=pay-demo ORDERS_KEY=ord-demo RECON_KEY=rec-demo
governord -config governor.yaml
```

In another, run the demo with the same keys:

```sh
npm install
PAYMENTS_KEY=pay-demo RECON_KEY=rec-demo npm run demo
```

`governord` listens on `127.0.0.1:7600`. If that port is taken, change `listen`
in `governor.yaml` and set `GOVERNOR_ADDR` to the same address for the demo.

The folder needs nothing else from this repository: the client comes from npm.

## How it maps onto Governor

Each service is a tenant with its own API key. The root's limits are what the
real systems can take, and the tenants' limits and weights decide how they share.

```yaml
root:
  limits:
    db: 10   # connections to the shared Postgres
    psp: 8   # calls in flight to the payment gateway

tenants:
  - name: payments
    weight: 4
    defaults:
      quotas:
        retry: 3   # every checkout task starts with this retry budget
  - name: reconciliation
    weight: 1
    limits:
      db: 3
      psp: 2
```

The `psp` limit is adaptive: cut to 70% when the gateway is slow or answers 429,
raised by one while it is healthy. The full file is `governor.yaml`.

In code, every checkout is a task with a deadline. Each database query and each
gateway call holds a lease of its class, and each retry draws from the task's
retry quota:

```js
const task = await payments.newTask({ name: `checkout-${n}`, deadlineMs: 3000 });

const lease = await task.acquire('psp');
const res = await fetch(gateway, { method: 'POST' });
await lease.release({ latencyMs, overloaded: res.status === 429 });

await task.consume('retry');   // throws once the checkout's budget is spent
await task.close();
```

The run without Governor uses the same code with a stand-in that grants every
lease at once, so the only difference between the two columns is Governor.

## Taking it further

- **Fencing.** A lease id only grows, so it can guard the write itself. Store it
  with the payment, and a worker that wakes up after a long pause cannot capture
  twice:

  ```sql
  UPDATE payments SET status = 'captured', fence = $1
  WHERE id = $2 AND fence < $1;
  ```

- **Quotas never refill.** A quota on a tenant is a budget for its whole life.
  Give a long-running service `limits` only, and put quotas on its tasks through
  `defaults`, as `payments` does with `retry`.
- **Across machines,** `governord` needs a certificate. See "Encrypting the
  connection" in the main README; the demo then needs `GOVERNOR_CA_FILE` or
  `GOVERNOR_TLS=1`, as every client does.
