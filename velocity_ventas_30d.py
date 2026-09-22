#!/usr/bin/env python3
"""
velocity_ventas_30d.py

Descubre automaticamente todos los negocios (sellers) activos de la cuenta
de Velocity, identifica en que bodega(s) fisica(s) tiene stock cada uno, y
calcula las unidades DESPACHADAS (entregadas) por SKU en los ultimos N dias
para cada combinacion negocio+bodega encontrada.

No hace falta editar este script ni el workflow cuando entra o sale un
cliente: mientras el cliente este activo en Velocity y tenga inventario en
alguna de las bodegas fisicas rastreadas (ver PHYSICAL_WAREHOUSE_IDS), el
script lo procesa solo.

USO
---
    export VELOCITY_API_KEY="tu-clave-aqui"
    python3 velocity_ventas_30d.py --output-dir data

Salida (por cada negocio+bodega encontrado):
    data/ventas_30d_<business_id>_<warehouse_id>.json
Ademas escribe:
    data/manifest.json  -- lista de todos los negocios+bodegas procesados,
                            con nombre, business_id, warehouse_id y el path
                            del archivo de ventas correspondiente. Sirve para
                            que cualquier proceso downstream (como la pagina
                            de controles) descubra los clientes sin tener que
                            conocerlos de antemano.

NOTA DE SEGURIDAD
------------------
La clave se lee SIEMPRE desde la variable de entorno VELOCITY_API_KEY.
Nunca la escribas en este archivo ni la subas a un repositorio.

CRITERIO DE "VENDIDO"
----------------------
Se cuentan solo ordenes en estado "Entregado" (order_status_id = 6). Ver
DISPATCHED_STATUS_IDS mas abajo para ampliar el criterio si hace falta.

BODEGAS FISICAS RASTREADAS
----------------------------
Solo las bodegas con layout de ubicaciones reales (has_layout=true en
GET /warehouses) tienen sentido para un proceso de conteo fisico. Hoy son
la 344 (ebox layout) y la 521 (Ebox lo Echevers). Si Ebox abre una bodega
fisica nueva, agregar su id a PHYSICAL_WAREHOUSE_IDS mas abajo -- eso es
lo unico que requiere editar este archivo; los clientes nuevos no.
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
PAGE_SIZE = 200  # maximo permitido por /orders
BUSINESS_PAGE_SIZE = 100
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# Estados que cuentan como "vendido/despachado" para este calculo.
# 6 = Entregado.
DISPATCHED_STATUS_IDS = [6]

# Unicas bodegas con layout de ubicaciones fisicas reales. Editar solo si
# Ebox abre/cierra una bodega fisica -- nunca por un cliente nuevo.
PHYSICAL_WAREHOUSE_IDS = {344, 521}


def get_api_key() -> str:
    key = os.environ.get("VELOCITY_API_KEY")
    if not key:
        sys.exit(
            "ERROR: falta la variable de entorno VELOCITY_API_KEY.\n"
            "       export VELOCITY_API_KEY='tu-clave-aqui'"
        )
    return key


def request_with_retries(session, method, url, **kwargs):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(method, url, timeout=30, **kwargs)
            if resp.status_code == 200:
                return resp.json()
            last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except requests.RequestException as exc:
            last_error = str(exc)
        print(f"  intento {attempt}/{MAX_RETRIES} fallo ({url}): {last_error}", file=sys.stderr)
        time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"No se pudo completar {url} tras {MAX_RETRIES} intentos: {last_error}")


def list_active_businesses(session):
    """Recorre GET /businesses (paginado) y devuelve los negocios activos."""
    businesses = []
    page = 1
    while True:
        data = request_with_retries(
            session, "GET", f"{BASE_URL}/businesses",
            params={"page": page, "size": BUSINESS_PAGE_SIZE},
        )
        items = data.get("items", [])
        print(f"  GET /businesses pagina {page}: {len(items)} items", file=sys.stderr)
        if not items:
            break

        for biz in items:
            if biz.get("active") is False:
                continue
            business_id = biz.get("id")
            business_name = biz.get("name") or business_id
            if business_id:
                businesses.append({"business_id": business_id, "business_name": business_name})

        total_pages = data.get("total_pages")
        if total_pages and page >= total_pages:
            break
        if len(items) < BUSINESS_PAGE_SIZE:
            break
        page += 1

    return businesses


def has_activity(session, business_id, warehouse_id, period_days):
    """
    Revisa rapido (1 pagina, 1 resultado) si el negocio tuvo alguna orden
    entregada en esta bodega dentro del periodo. Se usa para detectar en que
    bodega(s) opera cada negocio, ya que la asociacion negocio->bodega no
    viene expuesta de forma confiable en GET /businesses para cuentas de
    operador.
    """
    date_to = datetime.now(timezone.utc)
    date_from = date_to - timedelta(days=period_days)
    filters = [
        ["warehouse_id", "=", warehouse_id],
        ["and"],
        ["order_status_id", "IN", DISPATCHED_STATUS_IDS],
        ["and"],
        ["created_at", ">=", date_from.strftime("%Y-%m-%dT00:00:00Z")],
        ["and"],
        ["created_at", "<=", date_to.strftime("%Y-%m-%dT23:59:59Z")],
        ["and"],
        ["business_id", "=", business_id],
    ]
    params = {
        "page": 1,
        "size": 1,
        "filters": json.dumps(filters),
        "skip_total": "true",
    }
    data = request_with_retries(session, "GET", f"{BASE_URL}/orders", params=params)
    orders = data.get("items") or data.get("orders") or []
    return len(orders) > 0


def discover_business_warehouse_pairs(session, period_days):
    """
    Para cada negocio activo, revisa cada bodega fisica rastreada y arma el
    par negocio+bodega solo si hubo actividad real (ordenes entregadas) en
    el periodo. Un negocio nuevo aparece solo la primera semana que tenga
    despachos registrados.
    """
    businesses = list_active_businesses(session)
    print(f"Negocios activos encontrados: {len(businesses)}", file=sys.stderr)

    pairs = []
    for biz in businesses:
        for wh_id in sorted(PHYSICAL_WAREHOUSE_IDS):
            try:
                if has_activity(session, biz["business_id"], wh_id, period_days):
                    pairs.append({
                        "business_id": biz["business_id"],
                        "business_name": biz["business_name"],
                        "warehouse_id": wh_id,
                    })
            except Exception as exc:
                print(f"  aviso: no se pudo revisar {biz['business_name']} / bodega {wh_id}: {exc}", file=sys.stderr)

    return pairs


def compute_units_sold(session, api_key, business_id, warehouse_id, period_days):
    date_to = datetime.now(timezone.utc)
    date_from = date_to - timedelta(days=period_days)
    date_from_str = date_from.strftime("%Y-%m-%dT00:00:00Z")
    date_to_str = date_to.strftime("%Y-%m-%dT23:59:59Z")

    units_sold = defaultdict(int)
    order_count = 0
    page = 1

    while True:
        filters = [
            ["warehouse_id", "=", warehouse_id],
            ["and"],
            ["order_status_id", "IN", DISPATCHED_STATUS_IDS],
            ["and"],
            ["created_at", ">=", date_from_str],
            ["and"],
            ["created_at", "<=", date_to_str],
        ]
        params = {
            "page": page,
            "size": PAGE_SIZE,
            "filters": json.dumps(filters),
            "skip_total": "true",
            "sort_by": "created_at",
            "sort_dir": "ASC",
        }
        data = request_with_retries(session, "GET", f"{BASE_URL}/orders", params=params)
        orders = data.get("items") or data.get("orders") or []
        if not orders:
            break

        for order in orders:
            if business_id and order.get("business_id") not in (None, business_id):
                continue
            order_count += 1
            for item in order.get("items", []) or []:
                sku = (item.get("product") or {}).get("sku") or item.get("sku")
                qty = item.get("quantity") or 0
                if sku:
                    units_sold[sku] += qty

        total_pages = data.get("total_pages")
        if total_pages and page >= total_pages:
            break
        if len(orders) < PAGE_SIZE:
            break
        page += 1

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
    parser.add_argument("--output-dir", default="data", help="Carpeta de salida (default: data)")
    parser.add_argument("--period-days", type=int, default=30, help="Ventana de dias hacia atras (default 30)")
    parser.add_argument(
        "--business-id", default=None,
        help="Opcional: si se pasa junto con --warehouse-id, procesa SOLO ese par en vez de descubrir todos.",
    )
    parser.add_argument("--warehouse-id", type=int, default=None, help="Ver --business-id.")
    args = parser.parse_args()

    api_key = get_api_key()
    session = requests.Session()
    session.headers.update({"X-Velocity-Access-Token": api_key})

    os.makedirs(args.output_dir, exist_ok=True)

    if args.business_id and args.warehouse_id:
        pairs = [{"business_id": args.business_id, "business_name": args.business_id, "warehouse_id": args.warehouse_id}]
        print("Modo manual: procesando solo el negocio/bodega indicado.", file=sys.stderr)
    else:
        print("Descubriendo negocios activos y sus bodegas fisicas...", file=sys.stderr)
        pairs = discover_business_warehouse_pairs(session, args.period_days)
        print(f"Encontrados {len(pairs)} pares negocio+bodega.", file=sys.stderr)

    manifest = []
    for pair in pairs:
        business_id = pair["business_id"]
        business_name = pair["business_name"]
        warehouse_id = pair["warehouse_id"]
        print(f"-> {business_name} ({business_id}) / bodega {warehouse_id}", file=sys.stderr)

        try:
            result = compute_units_sold(session, api_key, business_id, warehouse_id, args.period_days)
        except Exception as exc:
            print(f"   ERROR procesando {business_name}: {exc}", file=sys.stderr)
            continue

        result["business_name"] = business_name
        filename = f"ventas_30d_{business_id}_{warehouse_id}.json"
        output_path = os.path.join(args.output_dir, filename)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"   {result['order_count']} ordenes, {len(result['units_sold'])} SKUs vendidos -> {filename}", file=sys.stderr)

        manifest.append({
            "business_id": business_id,
            "business_name": business_name,
            "warehouse_id": warehouse_id,
            "file": filename,
            "order_count": result["order_count"],
            "sku_count": len(result["units_sold"]),
            "generated_at": result["generated_at"],
        })

    manifest_path = os.path.join(args.output_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "clients": manifest}, f, ensure_ascii=False, indent=2)

    print(f"\nListo. {len(manifest)} archivos generados. Manifest: {manifest_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
