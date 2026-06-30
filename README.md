# Mini Shop API — Observability Exercise

This repo contains a small e-commerce-style system: one PostgreSQL
database and a single Python (FastAPI) API, plus a load generator that
continuously sends realistic traffic against it.

```
db (Postgres)
└── api (FastAPI, port 8000)
    /health
    /users, /users/{id}
    /products, /products/{id}, /products/{id}/availability
    /orders, /orders/{id}
    /payments/process, /payments/{id}
loadgen   -- generates continuous traffic + periodic bursts
```

## Running it

```bash
docker compose up --build
```

This brings up Postgres (seeded with some users/products), the API, and
the load generator. Traffic starts flowing immediately — you don't need
to send any requests yourself, though you're welcome to (e.g.
`curl http://localhost:8000/products`).

Give it a minute or two after startup for traffic to ramp up; the load
generator varies its rate over a ~6 minute cycle and also fires periodic
bursts of concurrent order-creation requests.

## Your task

Add observability to this API using **Prometheus** and **Grafana**, and
build a dashboard that surfaces the **four golden signals**:

1. **Latency** — how long requests take (consider percentiles, not just
   averages)
2. **Traffic** — how much demand the API is receiving
3. **Errors** — rate of failed requests
4. **Saturation** — how "full" the system is (think about what's actually
   constrained here)

You have full freedom in how you implement this. At minimum we'd expect:

- The API instrumented to expose a `/metrics` endpoint (e.g. via
  `prometheus_client` or `prometheus_fastapi_instrumentator`)
- A Prometheus instance configured to scrape it
- A Grafana instance with a dashboard showing the four golden signals,
  broken down by endpoint where it makes sense
- Be ready to explain your choices: what you picked as your saturation
  metric and why, how you defined "error" per endpoint, and what
  queries/PromQL you used

You can add Prometheus, Grafana, and any instrumentation libraries to
`docker-compose.yml`, or run them however you prefer — there's no fixed
structure you need to follow. Feel free to modify `app/main.py` to add
custom metrics, as long as the existing endpoints keep working (the load
generator depends on them).

## Notes on the system (useful context, not spoilers)

- The API has a **small Postgres connection pool** (`DB_POOL_MAX=6` by
  default).
- `/products/{id}/availability` simulates an occasionally slow/failing
  downstream dependency (e.g. a warehouse/inventory system).
- `/payments/process` simulates a flaky third-party payment gateway —
  most calls are fast, some are slow, some fail outright.
- The load generator varies its request rate over time and periodically
  bursts concurrent traffic at `/orders`, which is enough to exhaust the
  small connection pool — a real, observable saturation event.

None of this is hidden from you — feel free to read `app/main.py` and
`loadgen/load_test.py` if it helps you decide what to measure.
