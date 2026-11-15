"""Orquestador del pipeline: EXTRACT/LOAD → TRANSFORM (dbt) → TEST → DOCS → PUBLISH.

Uso:
    python -m pipeline.run              # pipeline completo
    python -m pipeline.run --skip-extract
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from pipeline import extract, publish

ROOT = Path(__file__).resolve().parent.parent
log = logging.getLogger("pipeline")


def dbt(*args: str) -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT)}
    cmd = [sys.executable, "-m", "dbt.cli.main", *args] if os.name == "nt" else ["dbt", *args]
    log.info("$ dbt %s", " ".join(args))
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def step(nombre: str, fn, *a, **kw):
    log.info("▶ %s", nombre)
    t0 = time.perf_counter()
    res = fn(*a, **kw)
    log.info("✔ %s (%.1fs) %s", nombre, time.perf_counter() - t0, res if res is not None else "")
    return res


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--skip-extract", action="store_true")
    args = p.parse_args()

    if not args.skip_extract:
        res = step("1. Extract & Load (Open-Meteo → raw)", extract.run, pause_seconds=1.2)
        if res["cities_ok"] == 0 and res["cities_failed"] > 0:
            log.error("La ingesta falló para todas las ciudades")
            return 1
    step("2. dbt seed", dbt, "seed")
    step("3. dbt build (run + test)", dbt, "build", "--exclude", "resource_type:seed")
    step("4. dbt source freshness", dbt, "source", "freshness")
    step("5. dbt docs generate", dbt, "docs", "generate", "--static")
    step("6. Publish (site estático)", publish.build)
    return 0


if __name__ == "__main__":
    sys.exit(main())
