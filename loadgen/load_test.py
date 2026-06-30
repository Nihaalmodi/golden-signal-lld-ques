"""
Continuous dummy load generator for the single shop API.

  - TRAFFIC varies over time (sine-wave base rate) so request-rate graphs
    show actual shape instead of a flat line.
  - Every few minutes there's a short concurrency BURST of order creation,
    enough to exhaust the API's small DB pool (max_size=6 by default) and
    produce a visible SATURATION / queueing effect.
  - ERRORS and LATENCY spikes are baked into the API itself
    (/products/{id}/availability, /payments/process, and order validation)
    - the loadgen just needs to hit those endpoints regularly.

This script intentionally does NOT touch Prometheus/Grafana - instrumenting
the API and building the dashboard is the candidate's job.
"""

import asyncio
import math
import os
import random
import time

import httpx

API_URL = os.environ.get("API_URL", "http://localhost:8000")

NUM_USERS = 20
NUM_PRODUCTS = 15

# base traffic oscillates between MIN_RPS and MAX_RPS on a ~6 minute cycle
MIN_RPS = 3
MAX_RPS = 18
CYCLE_SECONDS = 360

# every BURST_INTERVAL seconds, fire a short burst of concurrent order
# creations to stress the API's small connection pool
BURST_INTERVAL = 180
BURST_DURATION = 20
BURST_CONCURRENCY = 40

START_TIME = time.time()


async def hit(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    try:
        resp = await client.request(method, url, **kwargs)
        return resp.status_code
    except httpx.RequestError as exc:
        return f"request_error:{exc.__class__.__name__}"


async def random_action(client: httpx.AsyncClient):
    """Pick one weighted action representing realistic user behaviour."""
    roll = random.random()

    if roll < 0.20:
        uid = random.randint(1, NUM_USERS)
        return await hit(client, "GET", f"{API_URL}/users/{uid}")

    elif roll < 0.30:
        return await hit(client, "GET", f"{API_URL}/users")

    elif roll < 0.45:
        pid = random.randint(1, NUM_PRODUCTS)
        return await hit(client, "GET", f"{API_URL}/products/{pid}")

    elif roll < 0.55:
        return await hit(client, "GET", f"{API_URL}/products")

    elif roll < 0.68:
        pid = random.randint(1, NUM_PRODUCTS)
        return await hit(client, "GET", f"{API_URL}/products/{pid}/availability")

    elif roll < 0.85:
        body = {
            "user_id": random.randint(1, NUM_USERS),
            "product_id": random.randint(1, NUM_PRODUCTS),
            "quantity": random.randint(1, 3),
        }
        # occasionally reference a nonexistent user to generate 404s
        if random.random() < 0.05:
            body["user_id"] = 9999
        return await hit(client, "POST", f"{API_URL}/orders", json=body)

    elif roll < 0.95:
        body = {"order_id": random.randint(1, 500), "amount_cents": random.randint(500, 15000)}
        return await hit(client, "POST", f"{API_URL}/payments/process", json=body)

    else:
        return await hit(client, "GET", f"{API_URL}/orders")


def current_target_rps() -> float:
    elapsed = time.time() - START_TIME
    phase = (elapsed % CYCLE_SECONDS) / CYCLE_SECONDS
    wave = (math.sin(phase * 2 * math.pi - math.pi / 2) + 1) / 2  # 0..1
    return MIN_RPS + wave * (MAX_RPS - MIN_RPS)


async def steady_traffic_loop():
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            target_rps = current_target_rps()
            interval = 1.0 / max(target_rps, 0.5)
            asyncio.create_task(random_action(client))
            await asyncio.sleep(interval)


async def burst_loop():
    """Periodically slam the API with concurrent order creations."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            await asyncio.sleep(BURST_INTERVAL)
            print(f"[loadgen] starting burst: {BURST_CONCURRENCY} concurrent order requests for {BURST_DURATION}s")
            end = time.time() + BURST_DURATION

            async def burst_worker():
                while time.time() < end:
                    body = {
                        "user_id": random.randint(1, NUM_USERS),
                        "product_id": random.randint(1, NUM_PRODUCTS),
                        "quantity": 1,
                    }
                    await hit(client, "POST", f"{API_URL}/orders", json=body)

            await asyncio.gather(*(burst_worker() for _ in range(BURST_CONCURRENCY)))
            print("[loadgen] burst finished")


async def main():
    print("[loadgen] starting continuous load against the shop API")
    await asyncio.gather(steady_traffic_loop(), burst_loop())


if __name__ == "__main__":
    asyncio.run(main())
