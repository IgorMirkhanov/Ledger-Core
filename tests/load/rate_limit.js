import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

/**
 * Proves the gateway rate limiter still returns 429 under default limits.
 * Run against a stack WITHOUT docker-compose.load.yml.
 *
 *   k6 run tests/load/rate_limit.js
 */
const BASE = __ENV.BASE_URL || 'http://localhost:8080/v1';
const limited = new Counter('rate_limited');

export const options = {
  scenarios: {
    rate_limit: {
      executor: 'constant-arrival-rate',
      rate: 200,
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 50,
      maxVUs: 100,
    },
  },
  thresholds: {
    rate_limited: ['count>0'],
  },
};

export default function () {
  const res = http.get(`${BASE}/accounts`, {
    headers: { Authorization: 'Bearer not-a-real-token' },
  });
  const is429 = res.status === 429;
  if (is429) {
    limited.add(1);
  }
  check(res, {
    '401 or 429': (r) => r.status === 401 || r.status === 429,
    '429 has Retry-After': (r) => r.status !== 429 || !!r.headers['Retry-After'],
  });
}
