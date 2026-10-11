"""Demo data through the real API (RFC 0004 §3.1.1.4 *Seed*, RFC 0005 O16, RFC 0006 #8).

Runs after the identity and catalog seeds, against the gateway:

1. each demo merchant's products, with variants and URL media;
2. every product published **through ProductPublishingWorkflow** — nothing is
   written as `published` directly, so the seed exercises the publish path;
3. stock set for every variant through inventory's merchant API;
4. sample orders through OrderWorkflow, in several end states.

Idempotent: products are found by SKU, stock is set absolutely, and orders
use fixed Idempotency-Keys, so a re-run replays them instead of ordering again.

    make seed      (runs this in the gateway image, on the Compose network)
"""

import os
import sys
import time
from typing import Any

import httpx

BASE_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8000")
ADMIN = (
    os.environ.get("SEED_ADMIN_EMAIL", "admin@smartretail.local"),
    os.environ.get("SEED_ADMIN_PASSWORD", "change-me-admin-password"),
)
DEMO_PASSWORD = os.environ.get("SEED_DEMO_PASSWORD", "change-me-demo-password")
WAIT_SECONDS = 90

# (merchant, sku, name, category slug, price, variants [(suffix, options, override)])
PRODUCTS: list[tuple[str, str, str, str, str, list[tuple[str, dict[str, str], str | None]]]] = [
    (
        "merchant1",
        "NW-TEE",
        "Trail Tee",
        "apparel-men",
        "24.00",
        [("S", {"size": "S"}, None), ("M", {"size": "M"}, None), ("L", {"size": "L"}, "26.00")],
    ),
    (
        "merchant1",
        "NW-JKT",
        "Storm Jacket",
        "apparel-women",
        "149.00",
        [("M", {"size": "M"}, None), ("L", {"size": "L"}, None)],
    ),
    ("merchant1", "NW-CAP", "Summit Cap", "apparel-men", "18.50", [("OS", {"size": "one"}, None)]),
    (
        "merchant2",
        "CH-PAN",
        "Cast Iron Pan",
        "home-kitchen-cookware",
        "39.90",
        [("26", {"diameter_cm": "26"}, None), ("30", {"diameter_cm": "30"}, "49.90")],
    ),
    (
        "merchant2",
        "CH-EAR",
        "Studio Earbuds",
        "electronics-audio",
        "79.00",
        [("BLK", {"colour": "black"}, None), ("WHT", {"colour": "white"}, None)],
    ),
    (
        "merchant2",
        "CH-PHN",
        "Pocket Phone",
        "electronics-phones",
        "299.00",
        [("128", {"storage_gb": "128"}, None)],
    ),
]

# (key, customer, sku of first variant to buy, quantity, payment token, postcode)
ORDERS = [
    ("seed-order-confirmed", "customer1", "NW-TEE-M", 2, "tok_ok", "10001"),
    ("seed-order-shipped", "customer1", "CH-PAN-26", 1, "tok_ok", "10001"),
    ("seed-order-declined", "customer2", "CH-EAR-BLK", 1, "tok_decline", "94107"),
    ("seed-order-undeliverable", "customer2", "NW-CAP-OS", 1, "tok_ok", "00000"),
]


def log(message: str) -> None:
    print(f"seed-demo: {message}", flush=True)


def login(client: httpx.Client, email: str, password: str) -> dict[str, str]:
    r = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def ok(r: httpx.Response) -> Any:
    if r.status_code >= 400:
        raise SystemExit(f"{r.request.method} {r.request.url.path} → {r.status_code}: {r.text}")
    return r.json() if r.content else None


def ensure_product(
    client: httpx.Client, headers: dict[str, str], categories: dict[str, str], spec: tuple
) -> dict[str, Any]:
    _, sku, name, category, price, variants = spec
    found = ok(client.get("/api/v1/products/me", params={"q": sku}, headers=headers))["items"]
    product = next((p for p in found if p["sku"] == sku), None)
    if product is None:
        product = ok(
            client.post(
                "/api/v1/products",
                json={
                    "sku": sku,
                    "name": name,
                    "description": f"{name}: demo product, seeded through the API.",
                    "category_id": categories[category],
                    "base_price": price,
                    "attributes": {"seeded": True},
                },
                headers=headers,
            )
        )
        log(f"created {sku}")
    detail = ok(client.get(f"/api/v1/products/{product['id']}", headers=headers))
    if detail["status"] != "draft":
        return detail
    version = detail["version"]
    have = {v["sku"] for v in detail["variants"]}
    for suffix, options, override in variants:
        if f"{sku}-{suffix}" in have:
            continue
        body: dict[str, Any] = {"sku": f"{sku}-{suffix}", "options": options, "version": version}
        if override:
            body["price_override"] = override
        version = ok(
            client.post(f"/api/v1/products/{product['id']}/variants", json=body, headers=headers)
        )["version"]
    if not detail["media"]:
        version = ok(
            client.post(
                f"/api/v1/products/{product['id']}/media",
                json={
                    "url": f"https://images.smartretail.local/{sku.lower()}.jpg",
                    "alt_text": name,
                    "version": version,
                },
                headers=headers,
            )
        )["version"]
    return ok(client.get(f"/api/v1/products/{product['id']}", headers=headers))


def publish(client: httpx.Client, headers: dict[str, str], product: dict[str, Any]) -> None:
    """Publishes through the workflow. A failed run (perhaps an earlier seed's) is
    retried once: the failure stays recorded, and the product is back in draft."""
    url = f"/api/v1/products/{product['id']}"
    for _ in range(2):
        detail = ok(client.get(url, headers=headers))
        if detail["status"] == "published":
            return
        if detail["status"] != "publishing":
            r = client.post(
                f"{url}/status",
                json={"status": "published", "version": detail["version"]},
                headers=headers,
            )
            if r.status_code != 202:
                raise SystemExit(f"publish {detail['sku']} → {r.status_code}: {r.text}")
        status = wait_for_publish(client, headers, url)
        if status["state"] == "published":
            log(f"published {detail['sku']} through {status['workflow_id']}")
            return
        log(f"publish {detail['sku']} failed at {status['failed_step']}; retrying once")
    raise SystemExit(f"publish {product['sku']} failed twice: {status}")


def wait_for_publish(client: httpx.Client, headers: dict[str, str], url: str) -> dict[str, Any]:
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        status: dict[str, Any] = ok(client.get(f"{url}/publish-status", headers=headers))
        if status["state"] in ("published", "failed"):
            return status
        time.sleep(0.5)
    raise SystemExit(f"publish of {url} did not finish in {WAIT_SECONDS}s")


def stock(client: httpx.Client, headers: dict[str, str], product: dict[str, Any]) -> None:
    for i, variant in enumerate(product["variants"]):
        current = ok(client.get(f"/api/v1/inventory/{variant['id']}"))
        if current["on_hand"] == 0:
            # One low-stock variant per product, so the low_stock filter has something.
            on_hand = 4 if i == len(product["variants"]) - 1 else 40
            ok(
                client.put(
                    f"/api/v1/inventory/{variant['id']}", json={"on_hand": on_hand}, headers=headers
                )
            )


def place_order(
    client: httpx.Client, headers: dict[str, str], variants: dict[str, tuple[str, str]], spec: tuple
) -> dict[str, Any]:
    key, _, variant_sku, quantity, token, postcode = spec
    product_id, variant_id = variants[variant_sku]
    body = {
        "items": [{"product_id": product_id, "variant_id": variant_id, "quantity": quantity}],
        "shipping_address": {
            "name": "Demo Customer",
            "line1": "1 Seed Street",
            "city": "Springfield",
            "postal_code": postcode,
            "country": "US",
        },
        "payment_method": token,
    }
    accepted = ok(
        client.post("/api/v1/orders", json=body, headers={**headers, "Idempotency-Key": key})
    )
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        order = ok(client.get(f"/api/v1/orders/{accepted['order_id']}", headers=headers))
        if order["status"] not in ("pending", "awaiting_payment", "refunding"):
            log(f"order {order['order_number']} ({key}) is {order['status']}")
            return order
        time.sleep(0.5)
    raise SystemExit(f"order {key} did not settle in {WAIT_SECONDS}s")


def main() -> None:
    with httpx.Client(base_url=BASE_URL, timeout=15) as client:
        admin = login(client, *ADMIN)
        users = {
            name: login(client, f"{name}@smartretail.local", DEMO_PASSWORD)
            for name in ("merchant1", "merchant2", "customer1", "customer2")
        }
        categories = {
            c["slug"]: c["id"]
            for c in ok(client.get("/api/v1/categories", params={"view": "flat", "limit": 100}))[
                "items"
            ]
        }
        variants: dict[str, tuple[str, str]] = {}
        for spec in PRODUCTS:
            merchant = users[spec[0]]
            product = ensure_product(client, merchant, categories, spec)
            publish(client, merchant, product)
            product = ok(client.get(f"/api/v1/products/{product['id']}", headers=merchant))
            stock(client, merchant, product)
            for v in product["variants"]:
                variants[v["sku"]] = (product["id"], v["id"])

        orders = {spec[0]: place_order(client, users[spec[1]], variants, spec) for spec in ORDERS}
        shipped = orders["seed-order-shipped"]
        for status in ("shipped", "in_transit", "delivered"):
            r = client.post(
                f"/api/v1/orders/{shipped['order_id']}/shipment/status",
                json={"status": status},
                headers=admin,
            )
            if r.status_code not in (200, 409):  # 409: already further along on a re-run
                raise SystemExit(f"carrier update {status} → {r.status_code}: {r.text}")
        log("done")


if __name__ == "__main__":
    try:
        main()
    except httpx.HTTPError as exc:
        sys.exit(f"seed-demo: gateway unreachable at {BASE_URL}: {exc}")
