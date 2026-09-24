import http from 'k6/http';
import { check, fail } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const BASE = __ENV.BASE_URL || 'http://localhost:8080/v1';

export function authHeaders(token, key) {
  const h = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  };
  if (key) h['Idempotency-Key'] = key;
  return h;
}

export function newKey() {
  return uuidv4();
}

/**
 * Creates N users with two RUB accounts each and a large deposit.
 * Result is written to a SharedArray-friendly JSON shape.
 */
export function provisionUsers(n) {
  const users = [];
  for (let i = 0; i < n; i++) {
    const tokRes = http.post(`${BASE}/dev/token`, JSON.stringify({}), {
      headers: { 'Content-Type': 'application/json' },
    });
    if (tokRes.status !== 200) {
      fail(`dev token failed: ${tokRes.status} ${tokRes.body}`);
    }
    const token = tokRes.json('token');
    const a = createAccount(token, 'RUB');
    const b = createAccount(token, 'RUB');
    deposit(token, a, '100000000', 'RUB');
    deposit(token, b, '100000000', 'RUB');
    users.push({ token, accounts: [a, b] });
  }
  return users;
}

export function createAccount(token, currency) {
  const res = http.post(`${BASE}/accounts`, JSON.stringify({ currency }), {
    headers: authHeaders(token, newKey()),
  });
  check(res, { 'create account': (r) => r.status === 201 });
  if (res.status !== 201) {
    fail(`create account: ${res.status} ${res.body}`);
  }
  return res.json('id');
}

export function deposit(token, accountID, amount, currency) {
  const res = http.post(
    `${BASE}/accounts/${accountID}/deposits`,
    JSON.stringify({ amount, currency }),
    { headers: authHeaders(token, newKey()) },
  );
  check(res, { deposit: (r) => r.status === 201 });
  if (res.status !== 201) {
    fail(`deposit: ${res.status} ${res.body}`);
  }
}

export function createTransfer(token, source, dest, amount, currency) {
  return http.post(
    `${BASE}/transfers`,
    JSON.stringify({
      source_account_id: source,
      dest_account_id: dest,
      amount,
      currency,
    }),
    { headers: authHeaders(token, newKey()), tags: { name: 'POST /transfers' } },
  );
}

export function createTransferWithKey(token, source, dest, amount, currency, key) {
  return http.post(
    `${BASE}/transfers`,
    JSON.stringify({
      source_account_id: source,
      dest_account_id: dest,
      amount,
      currency,
    }),
    { headers: authHeaders(token, key), tags: { name: 'POST /transfers' } },
  );
}

export function pickPair(users) {
  const u = users[Math.floor(Math.random() * users.length)];
  let source = u.accounts[0];
  let dest = u.accounts[1];
  if (Math.random() < 0.5) {
    source = u.accounts[1];
    dest = u.accounts[0];
  }
  // Prefer cross-user transfers when possible for more contention.
  if (users.length > 1 && Math.random() < 0.7) {
    const other = users[Math.floor(Math.random() * users.length)];
    if (other !== u) {
      dest = other.accounts[Math.floor(Math.random() * other.accounts.length)];
      if (dest === source) {
        dest = other.accounts[0] === dest ? other.accounts[1] : other.accounts[0];
      }
    }
  }
  return { token: u.token, source, dest };
}
