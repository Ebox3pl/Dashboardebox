#!/usr/bin/env python3
"""
velocity_ventas_30d.py

Calcula, para un negocio y bodega de Velocity, las unidades DESPACHADAS
(entregadas) por SKU en los ultimos N dias. Este numero es el insumo pesado
que no se puede calcular via el conector MCP en el chat (por volumen de
ordenes), asi que este script corre aparte y deja el resultado en un JSON.

USO
---
    export VELOCITY_API_KEY="tu-clave-aqui"
    python3 velocity_ventas_30d.py --business-id 670955107d7c5 --warehouse-id 344

Salida: ventas_30d_<warehouse_id>.json con la forma:
    {
      "warehouse_id": 344,
      "business_id": "670955107d7c5",
      "period_days": 30,
      "generated_at": "2026-09-22T ...",
      "units_sold": {
        "<sku>": 123,
        ...
      }
    }

NOTA DE SEGURIDAD
------------------
La clave se lee SIEMPRE desde la variable de entorno VELOCITY_API_KEY.
Nunca la escribas en este archivo ni la subas a un repositorio.

CRITERIO DE "VENDIDO"
----------------------
Se cuentan solo ordenes en estado "Entregado" (order_status_id = 6). Es el
estado mas limpio para medir demanda real: excluye canceladas, devueltas y
ordenes aun en transito. Si prefieren contar unidades ya comprometidas
(incluyendo "En Camino", "Asignado a Piloto", etc.) porque esas unidades ya
salieron fisicamente de la bodega, agreguen sus IDs a DISPATCHED_STATUS_IDS
mas abajo.
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

BASE_URL = "https://api.velocity-x.co"
PAGE_SIZE = 200  # maximo permitido por la API
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# Estados que cuentan como "vendido/despachado" para este calculo.
# 6 = Entregado. Ver docstring arriba si quieren ampliar el criterio.
DISPATCHED_STATUS_IDS = [6]


def get_api_key() -> str:
    key = os.environ.get("VELOCITY_API_KEY")
    if not key:
        sys.exit(
            "ERROR: falta la variable de entorno VELOCITY_API_KEY.\n"
            "       export VELOCITY_API_KEY='tu-clave-aqui'"
        )
    return key


def fetch_orders_page(
    session: requests.Session,
    warehouse_id: int,
    date_from: str,
    date_to: str,
    page: int,
) -> dict:
    """Trae una pagina de /orders filtrada por bodega, estado y rango de fechas."""
    filters = [
        ["warehouse_id", "=", warehouse_id],
        ["and"],
        ["order_status_id", "IN", DISPATCHED_STATUS_IDS],
        ["and"],
        ["created_at", ">=", date_from],
        ["and"],
        ["created_at", "<=", date_to],
    ]
    params = {
        "page": page,
        "size": PAGE_SIZE,
        "filters": json.dumps(filters),
        "skip_total": "true",  # mejora performance en listas grandes
        "sort_by": "created_at",
        "sort_dir": "ASC",
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(f"{BASE_URL}/orders", params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except requests.RequestException as exc:
            last_error = str(exc)

        print(
            f"  [pagina {page}] intento {attempt}/{MAX_RETRIES} fallo: {last_error}",
            file=sys.stderr,
        )
        time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"No se pudo traer la pagina {page} tras {MAX_RETRIES} intentos: {last_error}")


def compute_units_sold(
    api_key: str, business_id: str, warehouse_id: int, period_days: int
) -> dict:
    """Recorre todas las paginas de ordenes del periodo y suma unidades por SKU."""
    session = requests.Session()
    session.headers.update({"X-Velocity-Access-Token": api_key})

    date_to = datetime.now(timezone.utc)
    date_from = date_to - timedelta(days=period_days)
    date_from_str = date_from.strftime("%Y-%m-%dT00:00:00Z")
    date_to_str = date_to.strftime("%Y-%m-%dT23:59:59Z")

    units_sold = defaultdict(int)
    order_count = 0
    page = 1

    while True:
        data = fetch_orders_page(session, warehouse_id, date_from_str, date_to_str, page)
        orders = data.get("items") or data.get("orders") or []
        if not orders:
            break

        for order in orders:
            # Filtrar por negocio aca (por si el token es de un operador con
            # varios negocios hijos y el filtro DSL no discrimina por business_id
            # en /orders).
            if business_id and order.get("business_id") not in (None, business_id):
                continue
            order_count += 1
            for item in order.get("items", []) or []:
                sku = (item.get("product") or {}).get("sku") or item.get("sku")
                qty = item.get("quantity") or 0
                if sku:
                    units_sold[sku] += qty

        print(f"  pagina {page}: {len(orders)} ordenes (acumulado: {order_count})", file=sys.stderr)

        total_pages = data.get("total_pages")
        if total_pages and page >= total_pages:
            break
        if len(orders) < PAGE_SIZE:
            break
        page += 1

    print(f"Total ordenes procesadas: {order_count}", file=sys.stderr)
    print(f"Total SKUs con ventas: {len(units_sold)}", file=sys.stderr)

    return {
        "warehouse_id": warehouse_id,
        "business_id": business_id,
        "period_days": period_days,
        "date_from": date_from_str,
        "date_to": date_to_str,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "order_count": order_count,
        "units_sold": dict(units_sold),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--business-id", required=True, help="business_id de Velocity (ej. 670955107d7c5)")
    parser.add_argument("--warehouse-id", required=True, type=int, help="warehouse_id (ej. 344)")
    parser.add_argument("--period-days", type=int, default=30, help="Ventana de dias hacia atras (default 30)")
    parser.add_argument("--output", default=None, help="Path de salida (default: ventas_30d_<warehouse_id>.json)")
    args = parser.parse_args()

    api_key = get_api_key()

    print(
        f"Calculando ventas de los ultimos {args.period_days} dias "
        f"para business_id={args.business_id} warehouse_id={args.warehouse_id} ...",
        file=sys.stderr,
    )

    result = compute_units_sold(api_key, args.business_id, args.warehouse_id, args.period_days)

    output_path = args.output or f"ventas_30d_{args.warehouse_id}.json"
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Listo. Resultado guardado en: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
