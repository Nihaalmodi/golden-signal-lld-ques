"""
Single mini e-commerce API (FastAPI) backed by Postgres.

Endpoints, grouped by resource, all living in one service/process:
  - /health
  - /users, /users/{id}                         (CRUD-ish, fast)
  - /products, /products/{id}                   (fast)
  - /products/{id}/availability                 (occasionally slow/fails -
                                                   simulates a flaky downstream
                                                   inventory check)
  - /orders, /orders/{id}, POST /orders          (validates user/product
                                                   in-process, occasionally slow)
  - /payments/process, /payments/{id}            (simulates a flaky payment
                                                   gateway: mix of fast/slow/
                                                   failing calls)

The DB connection pool is deliberately small (see DB_POOL_MAX) so that
under the load generator's periodic concurrency bursts, requests queue up
waiting for a free connection - a real, observable saturation signal for
the candidate to find.
"""

import os
import random
import asyncio
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@db:5432/golden")
DB_POOL_MIN = int(os.environ.get("DB_POOL_MIN", 2))
DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", 6))

pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DB_DSN, min_size=DB_POOL_MIN, max_size=DB_POOL_MAX)
    yield
    await pool.close()


app = FastAPI(title="shop-api", lifespan=lifespan)


class UserIn(BaseModel):
    name: str
    email: str


class ProductIn(BaseModel):
    name: str
    price_cents: int
    stock: int = 100


class OrderIn(BaseModel):
    user_id: int
    product_id: int
    quantity: int = 1


class PaymentIn(BaseModel):
    order_id: int
    amount_cents: int


@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------- users ----

@app.get("/users")
async def list_users(limit: int = 20):
    await asyncio.sleep(random.uniform(0.005, 0.03))
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, email FROM users ORDER BY id LIMIT $1", limit)
    return [dict(r) for r in rows]


@app.get("/users/{user_id}")
async def get_user(user_id: int):
    await asyncio.sleep(random.uniform(0.005, 0.04))
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, name, email FROM users WHERE id = $1", user_id)
    if not row:
        raise HTTPException(status_code=404, detail="user not found")
    return dict(row)


@app.post("/users", status_code=201)
async def create_user(user: UserIn):
    await asyncio.sleep(random.uniform(0.01, 0.05))
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email",
                user.name,
                user.email,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail="email already exists")
    return dict(row)


# ------------------------------------------------------------- products ----

@app.get("/products")
async def list_products(limit: int = 20):
    await asyncio.sleep(random.uniform(0.005, 0.03))
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, price_cents, stock FROM products ORDER BY id LIMIT $1", limit)
    return [dict(r) for r in rows]


@app.get("/products/{product_id}")
async def get_product(product_id: int):
    await asyncio.sleep(random.uniform(0.005, 0.03))
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, name, price_cents, stock FROM products WHERE id = $1", product_id)
    if not row:
        raise HTTPException(status_code=404, detail="product not found")
    return dict(row)


@app.get("/products/{product_id}/availability")
async def check_availability(product_id: int):
    """
    Simulates a call out to a (fictional) warehouse/inventory system.
    ~3% time out entirely, ~12% are noticeably slow, the rest are fast.
    """
    roll = random.random()
    if roll < 0.03:
        await asyncio.sleep(random.uniform(1.5, 3.0))
        raise HTTPException(status_code=504, detail="warehouse system timeout")
    elif roll < 0.15:
        await asyncio.sleep(random.uniform(0.4, 1.2))
    else:
        await asyncio.sleep(random.uniform(0.01, 0.08))

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT stock FROM products WHERE id = $1", product_id)
    if not row:
        raise HTTPException(status_code=404, detail="product not found")
    return {"product_id": product_id, "in_stock": row["stock"] > 0, "stock": row["stock"]}


@app.post("/products", status_code=201)
async def create_product(product: ProductIn):
    await asyncio.sleep(random.uniform(0.01, 0.05))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO products (name, price_cents, stock) VALUES ($1, $2, $3) RETURNING id, name, price_cents, stock",
            product.name,
            product.price_cents,
            product.stock,
        )
    return dict(row)


# --------------------------------------------------------------- orders ----

@app.get("/orders")
async def list_orders(limit: int = 20):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, user_id, product_id, quantity, status FROM orders ORDER BY id DESC LIMIT $1", limit
        )
    return [dict(r) for r in rows]


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, user_id, product_id, quantity, status FROM orders WHERE id = $1", order_id
        )
    if not row:
        raise HTTPException(status_code=404, detail="order not found")
    return dict(row)


@app.post("/orders", status_code=201)
async def create_order(order: OrderIn):
    # validate user + product exist (in-process, same DB)
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow("SELECT id FROM users WHERE id = $1", order.user_id)
        product_row = await conn.fetchrow("SELECT id FROM products WHERE id = $1", order.product_id)

    if not user_row:
        raise HTTPException(status_code=404, detail="user not found")
    if not product_row:
        raise HTTPException(status_code=404, detail="product not found")

    # small artificial processing delay, occasionally a slow checkout
    if random.random() < 0.07:
        await asyncio.sleep(random.uniform(0.5, 1.5))
    else:
        await asyncio.sleep(random.uniform(0.01, 0.08))

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO orders (user_id, product_id, quantity, status)
            VALUES ($1, $2, $3, 'created')
            RETURNING id, user_id, product_id, quantity, status
            """,
            order.user_id,
            order.product_id,
            order.quantity,
        )
    return dict(row)


# ------------------------------------------------------------- payments ----

@app.get("/payments/{payment_id}")
async def get_payment(payment_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, order_id, amount_cents, status FROM payments WHERE id = $1", payment_id
        )
    if not row:
        raise HTTPException(status_code=404, detail="payment not found")
    return dict(row)


@app.post("/payments/process", status_code=201)
async def process_payment(payment: PaymentIn):
    """
    Simulates calling out to a third-party payment gateway:
      - ~70% fast & successful
      - ~20% noticeably slow (gateway under load) but still succeeds
      - ~7% slow AND fails (gateway timeout)
      - ~3% fast failure (card declined style error)
    """
    roll = random.random()
    status = "completed"
    http_error = None

    if roll < 0.70:
        await asyncio.sleep(random.uniform(0.02, 0.12))
    elif roll < 0.90:
        await asyncio.sleep(random.uniform(0.3, 1.0))
    elif roll < 0.97:
        await asyncio.sleep(random.uniform(1.5, 3.5))
        status = "failed"
        http_error = (504, "payment gateway timeout")
    else:
        await asyncio.sleep(random.uniform(0.02, 0.1))
        status = "failed"
        http_error = (402, "payment declined")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO payments (order_id, amount_cents, status)
            VALUES ($1, $2, $3)
            RETURNING id, order_id, amount_cents, status
            """,
            payment.order_id,
            payment.amount_cents,
            status,
        )

    if http_error:
        raise HTTPException(status_code=http_error[0], detail=http_error[1])

    return dict(row)
