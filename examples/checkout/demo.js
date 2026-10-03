// Runs a checkout service and a reconciliation job against one database and one payment gateway, without Governor and then with it.
import http from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { connect } from '@avik-creator/governor';

const RUN_MS = 8_000;

// Checkouts must finish within this, or they count as failed.
const CHECKOUT_DEADLINE_MS = 3_000;
const CHECKOUT_WORKERS = 10;
const ATTEMPTS = 4;

// The reconciliation job is a batch: many workers, each a query and a gateway call per step.
const RECON_WORKERS = 30;

// The downstreams: a database with 10 connections and a gateway that takes 6 calls at once.
const DB_CONNECTIONS = 10;
const GATEWAY_CAPACITY = 6;
const GATEWAY_LATENCY_MS = 40;

// Pool stands in for Postgres: a fixed number of connections and a FIFO queue for the rest.
class Pool {
  constructor(size) {
    this.free = size;
    this.waiting = [];
  }

  async query(ms) {
    if (this.free > 0) {
      this.free--;
    } else {
      await new Promise((resolve) => this.waiting.push(resolve));
    }
    try {
      await sleep(ms);
    } finally {
      // The connection passes straight to the next in line, or goes back to the pool.
      const next = this.waiting.shift();
      if (next) {
        next();
      } else {
        this.free++;
      }
    }
  }
}

// startGateway serves a payment gateway that answers 429 once more than its capacity is in flight.
async function startGateway(stats) {
  let inFlight = 0;
  const server = http.createServer(async (req, res) => {
    stats.gatewayCalls++;
    if (inFlight >= GATEWAY_CAPACITY) {
      stats.gateway429++;
      res.writeHead(429).end();
      return;
    }
    inFlight++;
    await sleep(GATEWAY_LATENCY_MS);
    inFlight--;
    res.writeHead(200).end();
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { server, url: `http://127.0.0.1:${server.address().port}/capture` };
}

// capture calls the gateway under a psp lease and reports the outcome to Governor; it returns the status.
async function capture(task, gateway) {
  const lease = await task.acquire('psp');
  const start = performance.now();
  let status = 0;
  try {
    status = (await fetch(gateway, { method: 'POST' })).status;
    return status;
  } finally {
    await lease.release({ latencyMs: performance.now() - start, overloaded: status === 429 });
  }
}

// query runs one database query under a db lease.
async function query(task, pool, ms) {
  const lease = await task.acquire('db');
  try {
    await pool.query(ms);
  } finally {
    await lease.release();
  }
}

// checkout reserves the order in the database, then captures the payment, retrying a 429 within the task's budget.
async function checkout(payments, n, env, stats) {
  const start = performance.now();
  let ok = false;
  const task = await payments.newTask({ name: `checkout-${n}`, deadlineMs: CHECKOUT_DEADLINE_MS });
  try {
    await query(task, env.pool, 15);
    for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
      if (attempt > 1) {
        // Every retry draws from the checkout's retry quota, shared by everything under the task.
        await task.consume('retry');
        stats.retries++;
        await sleep(25 + Math.random() * 50);
      }
      if ((await capture(task, env.gateway)) === 200) {
        ok = true;
        break;
      }
    }
  } catch {
    // A denied retry, an ended task or a refused lease all leave the checkout unpaid.
  } finally {
    await task.close().catch(() => {});
  }
  const took = performance.now() - start;
  stats.checkoutTimes.push(took);
  if (ok && took <= CHECKOUT_DEADLINE_MS) {
    stats.checkoutsOk++;
  } else {
    stats.checkoutsFailed++;
  }
}

// reconcile is one step of the batch job: a heavy query, then a call to the gateway.
async function reconcile(job, env, stats) {
  await query(job, env.pool, 40);
  if ((await capture(job, env.gateway)) === 200) {
    stats.reconSteps++;
  } else {
    // A batch client backs off a little after a failure.
    await sleep(20);
  }
}

// ungoverned stands in for Governor when it is not used: every acquire is granted at once.
const ungoverned = {
  async newTask() {
    const lease = { release: async () => {} };
    return { acquire: async () => lease, consume: async () => {}, close: async () => {} };
  },
  async close() {},
};

// run drives both services for RUN_MS and returns what they saw.
async function run(payments, recon) {
  const stats = {
    checkoutsOk: 0, checkoutsFailed: 0, checkoutTimes: [], retries: 0,
    reconSteps: 0, gatewayCalls: 0, gateway429: 0,
  };
  const { server, url } = await startGateway(stats);
  const env = { pool: new Pool(DB_CONNECTIONS), gateway: url };
  const until = Date.now() + RUN_MS;
  const loop = async (step) => {
    while (Date.now() < until) {
      await step();
    }
  };

  // The whole batch is one task, so its workers share the tenant's caps.
  const job = await recon.newTask({ name: 'nightly-reconciliation' });
  let n = 0;
  await Promise.all([
    ...Array.from({ length: RECON_WORKERS }, () => loop(() => reconcile(job, env, stats).catch(() => {}))),
    ...Array.from({ length: CHECKOUT_WORKERS }, () => loop(() => checkout(payments, n++, env, stats))),
  ]);
  await job.close().catch(() => {});
  server.close();
  return stats;
}

// percentile returns the p-th percentile of the values, in milliseconds.
function percentile(values, p) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;
}

function report(without, withGov) {
  const rows = [
    ['checkouts paid within 3 s', (s) => s.checkoutsOk],
    ['checkouts failed', (s) => s.checkoutsFailed],
    ['checkout p50 (ms)', (s) => Math.round(percentile(s.checkoutTimes, 50))],
    ['checkout p95 (ms)', (s) => Math.round(percentile(s.checkoutTimes, 95))],
    ['checkout retries', (s) => s.retries],
    ['gateway calls', (s) => s.gatewayCalls],
    ['gateway 429s', (s) => s.gateway429],
    ['reconciliation steps done', (s) => s.reconSteps],
  ];
  const pad = (v, w) => String(v).padStart(w);
  console.log(`\n${''.padEnd(28)}${pad('without Governor', 18)}${pad('with Governor', 16)}`);
  for (const [label, get] of rows) {
    console.log(`${label.padEnd(28)}${pad(get(without), 18)}${pad(get(withGov), 16)}`);
  }
}

async function main() {
  const { PAYMENTS_KEY, RECON_KEY } = process.env;
  if (!PAYMENTS_KEY || !RECON_KEY) {
    throw new Error('set PAYMENTS_KEY and RECON_KEY to the keys in governor.yaml');
  }
  console.log(`Running ${RUN_MS / 1000} s without Governor...`);
  const without = await run(ungoverned, ungoverned);

  console.log(`Running ${RUN_MS / 1000} s with Governor...`);
  const payments = await connect({ apiKey: PAYMENTS_KEY });
  const recon = await connect({ apiKey: RECON_KEY });
  const withGov = await run(payments, recon);
  await Promise.all([payments.close(), recon.close()]);

  report(without, withGov);
}

main().catch((err) => {
  console.error(`demo: ${err.message}`);
  process.exitCode = 1;
});
