"""EXTRACT & LOAD: clima diario de ciudades del Perú desde la API pública Open-Meteo.

- Carga **incremental**: por cada ciudad solo descarga los días posteriores al último
  día ya cargado (o desde START_DATE en la primera ejecución).
- **Idempotente**: hace upsert por (city_id, date); re-ejecutar no duplica filas.
- Registra cada ejecución en raw.ingestion_log (observabilidad).

Uso:
    python -m pipeline.extract
"""

from __future__ import annotations

import csv
import logging
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("DUCKDB_PATH", ROOT / "warehouse" / "weather.duckdb"))
CITIES_CSV = ROOT / "seeds" / "cities.csv"
START_DATE = date.fromisoformat(os.getenv("START_DATE", "2024-01-01"))
# El archivo histórico de Open-Meteo (ERA5) se publica con unos días de retraso
ARCHIVE_DELAY_DAYS = int(os.getenv("ARCHIVE_DELAY_DAYS", "5"))
API_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARS = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "shortwave_radiation_sum",
    "sunshine_duration",
]

log = logging.getLogger("extract")

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.weather_daily (
    city_id                  INTEGER NOT NULL,
    date                     DATE    NOT NULL,
    weather_code             INTEGER,
    temperature_2m_max       DOUBLE,
    temperature_2m_min       DOUBLE,
    temperature_2m_mean      DOUBLE,
    precipitation_sum        DOUBLE,
    rain_sum                 DOUBLE,
    precipitation_hours      DOUBLE,
    wind_speed_10m_max       DOUBLE,
    wind_gusts_10m_max       DOUBLE,
    shortwave_radiation_sum  DOUBLE,
    sunshine_duration        DOUBLE,
    _ingested_at             TIMESTAMP NOT NULL,
    _run_id                  VARCHAR NOT NULL,
    PRIMARY KEY (city_id, date)
);
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    run_id       VARCHAR,
    city_id      INTEGER,
    start_date   DATE,
    end_date     DATE,
    rows_loaded  INTEGER,
    status       VARCHAR,
    message      VARCHAR,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP
);
"""


def load_cities() -> list[dict]:
    with open(CITIES_CSV, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def fetch_city(lat: float, lon: float, start: date, end: date, retries: int = 5) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARS),
        "timezone": "America/Lima",
    }
    for intento in range(1, retries + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            if r.status_code == 429 and intento < retries:
                espera = 15 * intento
                log.warning("Rate limit 429 (intento %d/%d), esperando %ds...", intento, retries, espera)
                time.sleep(espera)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            if intento == retries:
                raise
            espera = 3 * (2 ** (intento - 1))
            log.warning("Error %s (intento %d/%d), reintentando en %ds", exc, intento, retries, espera)
            time.sleep(espera)
    raise RuntimeError("unreachable")


def to_dataframe(payload: dict, city_id: int, run_id: str) -> pd.DataFrame:
    """Transforma la respuesta JSON de Open-Meteo a filas tabulares."""
    daily = payload.get("daily") or {}
    if not daily.get("time"):
        return pd.DataFrame()
    df = pd.DataFrame({"date": pd.to_datetime(daily["time"]).date})
    for var in DAILY_VARS:
        df[var] = daily.get(var, [None] * len(df))
    df.insert(0, "city_id", city_id)
    df["_ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df["_run_id"] = run_id
    # Días sin datos aún publicados (todo nulo) no se cargan
    return df.dropna(subset=["temperature_2m_max", "temperature_2m_min"], how="all")


def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    con.register("nuevos", df)
    try:
        cols = ", ".join(["city_id", "date", *DAILY_VARS, "_ingested_at", "_run_id"])
        con.execute(f"INSERT OR REPLACE INTO raw.weather_daily ({cols}) SELECT {cols} FROM nuevos").fetchall()
    finally:
        con.unregister("nuevos")
    return len(df)


def last_loaded(con: duckdb.DuckDBPyConnection, city_id: int) -> date | None:
    rows = con.execute("SELECT max(date) FROM raw.weather_daily WHERE city_id = ?", [city_id]).fetchall()
    return rows[0][0] if rows else None


def run(db_path: Path = DB_PATH, end: date | None = None, pause_seconds: float = 0.0) -> dict:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    end = end or (date.today() - timedelta(days=ARCHIVE_DELAY_DAYS))
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    resumen = {"run_id": run_id, "rows": 0, "cities_ok": 0, "cities_failed": 0}

    con = duckdb.connect(str(db_path))
    con.execute(DDL).fetchall()
    try:
        for idx, city in enumerate(load_cities()):
            cid = int(city["city_id"])
            ultimo = last_loaded(con, cid)
            start = (ultimo + timedelta(days=1)) if ultimo else START_DATE
            iniciado = datetime.now(timezone.utc).replace(tzinfo=None)
            if start > end:
                log.info("%-10s al día (último %s)", city["city"], ultimo)
                continue
            if idx > 0 and pause_seconds > 0:
                time.sleep(pause_seconds)
            try:
                payload = fetch_city(float(city["latitude"]), float(city["longitude"]), start, end)
                n = upsert(con, to_dataframe(payload, cid, run_id))
                estado, msg = "success", ""
                resumen["rows"] += n
                resumen["cities_ok"] += 1
                log.info("%-10s %s → %s : %d filas", city["city"], start, end, n)
            except Exception as exc:  # noqa: BLE001 - se registra y se continúa con otras ciudades
                n, estado, msg = 0, "failed", str(exc)[:500]
                resumen["cities_failed"] += 1
                log.error("%-10s FALLÓ: %s", city["city"], exc)
            con.execute(
                "INSERT INTO raw.ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [run_id, cid, start, end, n, estado, msg, iniciado,
                 datetime.now(timezone.utc).replace(tzinfo=None)],
            ).fetchall()
        resumen["total_rows_raw"] = con.execute("SELECT count(*) FROM raw.weather_daily").fetchall()[0][0]
    finally:
        con.close()
    return resumen


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    res = run(pause_seconds=1.2)
    print(res)
    if res["cities_ok"] == 0 and res["cities_failed"] > 0:
        raise SystemExit(1)
