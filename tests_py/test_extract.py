"""Pruebas unitarias de la ingesta (sin red: la API se simula)."""

from datetime import date

import duckdb
import pytest

from pipeline import extract


def payload(dias, tmax=25.0):
    n = len(dias)
    daily = {"time": dias}
    for v in extract.DAILY_VARS:
        daily[v] = [1.0] * n
    daily["temperature_2m_max"] = [tmax] * n
    daily["temperature_2m_min"] = [tmax - 8] * n
    daily["weather_code"] = [3] * n
    return {"daily": daily}


def test_to_dataframe_columnas_y_filtrado():
    p = payload(["2025-01-01", "2025-01-02", "2025-01-03"])
    p["daily"]["temperature_2m_max"][2] = None
    p["daily"]["temperature_2m_min"][2] = None  # día aún no publicado
    df = extract.to_dataframe(p, city_id=7, run_id="r1")
    assert len(df) == 2
    assert set(["city_id", "date", "_ingested_at", "_run_id", *extract.DAILY_VARS]) <= set(df.columns)
    assert (df.city_id == 7).all()


def test_payload_vacio():
    assert extract.to_dataframe({"daily": {}}, 1, "r").empty


def test_upsert_idempotente(tmp_path):
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    con.execute(extract.DDL).fetchall()
    df = extract.to_dataframe(payload(["2025-01-01", "2025-01-02"]), 1, "r1")
    extract.upsert(con, df)
    extract.upsert(con, df)  # re-ejecución: no duplica
    assert con.execute("SELECT count(*) FROM raw.weather_daily").fetchall()[0][0] == 2
    # una corrección actualiza el valor existente
    extract.upsert(con, extract.to_dataframe(payload(["2025-01-02"], tmax=30.0), 1, "r2"))
    assert con.execute("SELECT temperature_2m_max FROM raw.weather_daily WHERE date='2025-01-02'").fetchall()[0][0] == 30.0
    assert extract.last_loaded(con, 1) == date(2025, 1, 2)
    con.close()


def test_run_incremental(tmp_path, monkeypatch):
    llamadas = []

    def fake_fetch(lat, lon, start, end, retries=4):
        llamadas.append((start, end))
        dias = [str(date.fromordinal(d)) for d in range(start.toordinal(), end.toordinal() + 1)]
        return payload(dias)

    monkeypatch.setattr(extract, "fetch_city", fake_fetch)
    monkeypatch.setattr(extract, "START_DATE", date(2025, 1, 1))
    db = tmp_path / "w.duckdb"

    r1 = extract.run(db, end=date(2025, 1, 10))
    assert r1["cities_ok"] == 12 and r1["rows"] == 120
    llamadas.clear()

    r2 = extract.run(db, end=date(2025, 1, 12))  # solo descarga 2 días nuevos por ciudad
    assert all(s == date(2025, 1, 11) for s, _ in llamadas)
    assert r2["rows"] == 24 and r2["total_rows_raw"] == 144

    r3 = extract.run(db, end=date(2025, 1, 12))  # nada nuevo
    assert r3["rows"] == 0


def test_run_tolera_fallos(tmp_path, monkeypatch):
    def falla(*a, **k):
        raise extract.requests.ConnectionError("sin red")

    monkeypatch.setattr(extract, "fetch_city", falla)
    r = extract.run(tmp_path / "f.duckdb", end=date(2025, 1, 5))
    assert r["cities_failed"] == 12
    with duckdb.connect(str(tmp_path / "f.duckdb")) as con:
        assert con.execute("SELECT count(*) FROM raw.ingestion_log WHERE status='failed'").fetchone()[0] == 12


@pytest.mark.parametrize("var", ["temperature_2m_max", "precipitation_sum", "weather_code"])
def test_variables_requeridas(var):
    assert var in extract.DAILY_VARS
