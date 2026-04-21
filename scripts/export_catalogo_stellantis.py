#!/usr/bin/env python3
"""
Export catalogo-stellantis.json desde Airtable (tabla "Inventario Stellantis").

Lee la tabla `tblDjSN19anut5mQF` de la base "Catalogos Plasencia" (`appDrSPzp5214PFj1`)
y genera el JSON consumible por la landing https://stellantis.grupoplasencia.com.

Filtra por `ACTIVO=true`. Transforma los campos de Airtable al formato `vehicles[]`
que espera el HTML (id, name, brand, year, version, type, cat, engine, trans, fuel,
price, priceType, badge, img, color, km, photos).

Credenciales:
    AIRTABLE_PAT       — Personal Access Token con permiso de lectura
    AIRTABLE_BASE_ID   — appDrSPzp5214PFj1 (Catalogos Plasencia)

Lectura desde .env local (dev) o env vars (GitHub Actions).

Output: ../docs/catalogo-stellantis.json
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime

# ── Config ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", ".env")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "docs", "catalogo-stellantis.json")
TABLE_ID = "tblDjSN19anut5mQF"  # Inventario Stellantis
TABLE_NAME = "Inventario Stellantis"


def load_env():
    env = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    return {
        "pat": env.get("AIRTABLE_PAT", os.environ.get("AIRTABLE_PAT", "")),
        "base_id": env.get("AIRTABLE_BASE_ID", os.environ.get("AIRTABLE_BASE_ID", "")),
    }


def fetch_active_records(pat, base_id):
    """Fetch all active records (ACTIVO=true) from Inventario Stellantis."""
    records = []
    offset = None
    url_base = f"https://api.airtable.com/v0/{base_id}/{TABLE_ID}"

    while True:
        params = {
            "filterByFormula": "{ACTIVO}=TRUE()",
            "pageSize": "100",
            # Sortear por ID_SKU para mantener orden estable.
            "sort[0][field]": "ID_SKU",
            "sort[0][direction]": "asc",
        }
        if offset:
            params["offset"] = offset
        url = url_base + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
        })
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f"ERROR HTTP {e.code} fetching from Airtable: {e.read().decode()}", file=sys.stderr)
            sys.exit(1)

        for rec in data.get("records", []):
            records.append(rec["fields"])

        offset = data.get("offset")
        if not offset:
            break

    return records


def brand_to_cat(brand):
    """Map MARCA to cat (lowercase usado en filtros)."""
    return (brand or "").lower()


def transform(fields):
    """Convierte un record de Airtable al formato vehicles[] del HTML.
    Incluye campos de oferta para que la calculadora use tasa/CxA por SKU."""
    return {
        "id": int(fields.get("ID_SKU", 0)),
        "name": fields.get("MODELO", ""),
        "brand": fields.get("MARCA", ""),
        "year": int(fields.get("ANIO", 0)),
        "version": fields.get("VERSION", ""),
        "type": fields.get("TIPO", "sedan"),
        "cat": brand_to_cat(fields.get("MARCA", "")),
        "engine": fields.get("MOTOR", ""),
        "trans": fields.get("TRANSMISION", ""),
        "fuel": fields.get("COMBUSTIBLE", "Gasolina"),
        "price": int(fields.get("MSRP", 0)),
        "priceType": "msrp",
        "badge": fields.get("BONO_BADGE", ""),
        "img": fields.get("IMG_URL", ""),
        "color": "",
        "km": 0,
        "photos": 0,
        # Campos de oferta (nuevos, usados por calculadora y popup):
        "tasa": float(fields.get("TASA_ANUAL", 0) or 0),  # decimal: 0.0799 = 7.99%
        "cxa": float(fields.get("COMISION_APERTURA", 0) or 0),  # decimal: 0.015 = 1.5%
        "descContado": int(fields.get("DESCUENTO_CONTADO", 0) or 0),
        "descFinanc": int(fields.get("DESCUENTO_FINANCIAMIENTO", 0) or 0),
        "msi": int(fields.get("MSI_MESES", 0) or 0),
        "mantAnios": int(fields.get("MANTENIMIENTOS_ANIOS", 0) or 0),
        "programas": fields.get("PROGRAMAS_APLICABLES", []) or [],
        "vigDesde": fields.get("VIGENCIA_DESDE", "2026-04-03"),
        "vigHasta": fields.get("VIGENCIA_HASTA", "2026-04-30"),
    }


def main():
    cfg = load_env()
    if not cfg["pat"] or not cfg["base_id"]:
        print("ERROR: faltan AIRTABLE_PAT y/o AIRTABLE_BASE_ID en .env o env vars", file=sys.stderr)
        sys.exit(1)

    print(f"[{datetime.now().isoformat()}] Fetching Inventario Stellantis...")
    raw = fetch_active_records(cfg["pat"], cfg["base_id"])
    print(f"  → {len(raw)} records ACTIVOS recibidos")

    transformed = [transform(r) for r in raw]
    # Sort estable por id (Airtable ya devuelve sorted, defensivo).
    transformed.sort(key=lambda v: v["id"])

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)

    # Resumen
    by_brand = {}
    for v in transformed:
        by_brand[v["brand"]] = by_brand.get(v["brand"], 0) + 1
    print(f"  → {OUTPUT_PATH} escrito ({len(transformed)} SKUs)")
    print(f"  Distribución: {by_brand}")
    print(f"  Tamaño: {os.path.getsize(OUTPUT_PATH)} bytes")


if __name__ == "__main__":
    main()
