import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import http from 'k6/http';
import {
  BASE,
  authHeaders,
  createTransfer,
  createTransferWithKey,
  newKey,
  pickPair,
  provisionUsers,
} from './setup.js';

const transferLatency = new Trend('transfer_latency', true);
const idempotentMismatch = new Counter('idempotent_mismatch');

const SCENARIO = __ENV.SCENARIO || 'steady';
const USERS = Number(__ENV.USERS || 100);

const scenarios = {
  steady: {
    executor: 'constant-arrival-rate',
    rate: 300,
    timeUnit: '1s',
    duration: '3m',
    preAllocatedVUs: 50,
    maxVUs: 200,
    exec: 'steady',
  },
  hot_account: {
    executor: 'constant-arrival-rate',
    rate: 150,
    timeUnit: '1s',
    duration: '1m',
    preAllocatedVUs: 40,
    maxVUs: 120,
    exec: 'hotAccount',
  },
  idempotent_retries: {
    executor: 'constant-arrival-rate',
    rate: 50,
    timeUnit: '1s',
    duration: '1m',
    preAllocatedVUs: 20,
    maxVUs: 60,
    exec: 'idempotentRetries',
  },
  read_mix: {
    executor: 'constant-arrival-rate',
    rate: 200,
    timeUnit: '1s',
    duration: '1m',
    preAllocatedVUs: 40,
    maxVUs: 120,
    exec: 'readMix',
  },
};

export const options = {
  scenarios: { [SCENARIO]: scenarios[SCENARIO] || scenarios.steady },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    'http_req_duration{name:POST /transfers}': ['p(99)<300'],
  },
};

export function setup() {
  console.log(`provisioning ${USERS} users against ${BASE}`);
  const users = provisionUsers(USERS);
  const hotDest = users[0].accounts[0];
  return { users, hotDest };
}

export function steady(data) {
  const pair = pickPair(data.users);
  const res = createTransfer(pair.token, pair.source, pair.dest, '100', 'RUB');
  transferLatency.add(res.timings.duration);
  check(res, {
    'transfer ok': (r) => r.status === 201 || r.status === 202,
  });
}

export function hotAccount(data) {
  const u = data.users[Math.floor(Math.random() * data.users.length)];
  let source = u.accounts[Math.floor(Math.random() * u.accounts.length)];
  let dest = data.hotDest;
  // 80% to hot dest, 20% random pair
  if (Math.random() >= 0.8) {
    const pair = pickPair(data.users);
    source = pair.source;
    dest = pair.dest;
    const res = createTransfer(pair.token, source, dest, '100', 'RUB');
    check(res, { 'transfer ok': (r) => r.status === 201 || r.status === 202 });
    return;
  }
  if (source === dest) {
    source = u.accounts[0] === dest ? u.accounts[1] : u.accounts[0];
  }
  const res = createTransfer(u.token, source, dest, '100', 'RUB');
  check(res, { 'hot transfer ok': (r) => r.status === 201 || r.status === 202 });
}

export function idempotentRetries(data) {
  const pair = pickPair(data.users);
  const key = newKey();
  const first = createTransferWithKey(pair.token, pair.source, pair.dest, '100', 'RUB', key);
  check(first, { 'first transfer': (r) => r.status === 201 || r.status === 202 });
  if (__ITER % 5 === 0) {
    const replay = createTransferWithKey(pair.token, pair.source, pair.dest, '100', 'RUB', key);
    const same =
      replay.status === first.status &&
      replay.json('id') === first.json('id');
    if (!same) {
      idempotentMismatch.add(1);
    }
    check(replay, {
      'idempotent replay': (r) =>
        r.status === first.status &&
        r.json('id') === first.json('id'),
    });
  }
}

export function readMix(data) {
  const u = data.users[Math.floor(Math.random() * data.users.length)];
  if (Math.random() < 0.7) {
    if (Math.random() < 0.5) {
      const res = http.get(`${BASE}/accounts`, {
        headers: authHeaders(u.token),
        tags: { name: 'GET /accounts' },
      });
      check(res, { 'list accounts': (r) => r.status === 200 });
    } else {
      const id = u.accounts[Math.floor(Math.random() * u.accounts.length)];
      const res = http.get(`${BASE}/accounts/${id}/statement?limit=20`, {
        headers: authHeaders(u.token),
        tags: { name: 'GET /statement' },
      });
      check(res, { statement: (r) => r.status === 200 });
    }
    return;
  }
  const pair = pickPair(data.users);
  const res = createTransfer(pair.token, pair.source, pair.dest, '50', 'RUB');
  check(res, { 'mix transfer': (r) => r.status === 201 || r.status === 202 });
  sleep(0.001);
}
