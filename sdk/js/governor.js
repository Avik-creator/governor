// A client for governord's gRPC API: sessions, tasks, quotas and leases.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

// A copy of proto/governor/v1/governor.proto, shipped in the package; CI keeps the two identical.
const PROTO = fileURLToPath(new URL('./governor.proto', import.meta.url));

// Node and lease ids are uint64, so they travel as strings to keep every digit.
const definition = protoLoader.loadSync(PROTO, { longs: String, enums: String, defaults: true });
const { GovernorService } = grpc.loadPackageDefinition(definition).governor.v1;

// GovernorError is a refusal or failure reported by governord.
export class GovernorError extends Error {
  constructor(err) {
    super(err.details || err.message);
    this.code = err.code;
  }

  // denied reports that a quota had no room left for the charge.
  get denied() {
    return this.code === grpc.status.RESOURCE_EXHAUSTED;
  }
}

// duration converts milliseconds to a google.protobuf.Duration or Timestamp.
function duration(ms) {
  const whole = Math.floor(ms);
  return { seconds: Math.floor(whole / 1000), nanos: (whole % 1000) * 1e6 };
}

// call makes one unary call with the secret as the bearer.
function call(client, method, request, secret) {
  const metadata = new grpc.Metadata();
  metadata.set('authorization', `Bearer ${secret}`);
  return new Promise((resolve, reject) => {
    client[method](request, metadata, (err, response) => (err ? reject(new GovernorError(err)) : resolve(response)));
  });
}

// parseBool reads a boolean the way Go's strconv.ParseBool does, so both clients accept the same values.
function parseBool(name, raw) {
  if (['1', 't', 'T', 'TRUE', 'true', 'True'].includes(raw)) {
    return true;
  }
  if (['0', 'f', 'F', 'FALSE', 'false', 'False'].includes(raw)) {
    return false;
  }
  // A value that cannot be read must not quietly mean plain text.
  throw new Error(`${name}: "${raw}" is neither true nor false`);
}

// credentials chooses how to reach governord, as the Go client does: TLS when asked for, plain text otherwise.
function credentials(tls, caFile) {
  if (tls === false && caFile) {
    throw new Error('TLS is turned off, but a CA file is set');
  }
  if (caFile) {
    return grpc.credentials.createSsl(readFileSync(caFile));
  }
  return tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
}

// connect trades a tenant's API key for a session that is kept alive until close.
export async function connect({
  apiKey,
  addr = process.env.GOVERNOR_ADDR || '127.0.0.1:7600',
  tls = process.env.GOVERNOR_TLS ? parseBool('GOVERNOR_TLS', process.env.GOVERNOR_TLS) : undefined,
  caFile = process.env.GOVERNOR_CA_FILE,
  ttlMs = 10_000,
}) {
  const client = new GovernorService(addr, credentials(tls, caFile));
  try {
    const opened = await call(client, 'OpenSession', { ttl: duration(ttlMs) }, apiKey);
    return new Session(client, opened.sessionToken, opened.scopeId, ttlMs);
  } catch (err) {
    client.close();
    throw err;
  }
}

// Session is one tenant's connection; closing it releases every lease it holds.
export class Session {
  #client;
  #token;
  #heartbeat;

  constructor(client, token, scopeId, ttlMs) {
    this.#client = client;
    this.#token = token;
    this.scopeId = scopeId;
    // Three heartbeats per TTL, so one lost heartbeat does not end the session.
    this.#heartbeat = setInterval(() => this.rpc('Heartbeat', {}).catch(() => {}), ttlMs / 3);
    this.#heartbeat.unref();
  }

  // rpc calls a method of governord as this session.
  rpc(method, request) {
    return call(this.#client, method, request, this.#token);
  }

  // newTask creates a task under the tenant; deadlineMs ends it that long from now.
  async newTask({ name, deadlineMs, quotas = {}, limits = {} }) {
    const spec = { name, quotas, limits };
    if (deadlineMs) {
      spec.deadline = duration(Date.now() + deadlineMs);
    }
    const { nodeId } = await this.rpc('CreateNode', { parentId: this.scopeId, spec });
    return new Task(this, nodeId);
  }

  // close ends the session and the connection.
  async close() {
    clearInterval(this.#heartbeat);
    await this.rpc('CloseSession', {}).catch(() => {});
    this.#client.close();
  }
}

// Task is one node of the tree; its charges and leases count against every ancestor.
export class Task {
  #session;

  constructor(session, id) {
    this.#session = session;
    this.id = id;
  }

  // consume charges amount of a quota, or throws a GovernorError that is denied.
  async consume(resource, amount = 1) {
    await this.#session.rpc('Consume', { nodeId: this.id, resource, amount });
  }

  // acquire waits for a lease of the class; the lease must be released.
  async acquire(cls) {
    const { leaseId } = await this.#session.rpc('Acquire', { nodeId: this.id, class: cls, maxHold: duration(60_000) });
    return new Lease(this.#session, leaseId);
  }

  // close ends the task as done.
  async close() {
    await this.#session.rpc('CloseNode', { nodeId: this.id });
  }
}

// Lease is held capacity of one class; its id is a fencing token.
export class Lease {
  #session;

  constructor(session, id) {
    this.#session = session;
    this.id = id;
  }

  // release returns the lease, reporting how long the work took and whether the downstream was overloaded.
  async release({ latencyMs, overloaded = false } = {}) {
    const request = { leaseId: this.id, overloaded };
    if (latencyMs !== undefined) {
      request.latency = duration(latencyMs);
    }
    await this.#session.rpc('Release', request);
  }
}
