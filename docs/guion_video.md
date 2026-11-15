# Guion del video (≤ 5 min) – Data Engineering con Python + dbt + DuckDB

| Tiempo | Escena | Qué decir / mostrar |
|---|---|---|
| 0:00–0:35 | Introducción | Presentación del equipo y objetivo: construir un pipeline ELT moderno de Ingeniería de Datos con **dbt + DuckDB + Python**, sin AWS, que extrae datos climáticos reales del Perú y los publica automáticamente. |
| 0:35–1:25 | Ingesta incremental (`pipeline/extract.py`) | Mostrar cómo se consulta la API de Open-Meteo para 12 ciudades del Perú, el control de última fecha cargada (`last_loaded`), el upsert idempotente en `raw.weather_daily` y la tabla de auditoría `raw.ingestion_log`. Ejecutar `pytest tests_py -v`. |
| 1:25–2:40 | Transformación y calidad con **dbt** | Mostrar `dbt_project.yml`, capas `seeds/`, `models/staging/` y `models/marts/` (destacar el modelo incremental `fct_weather_daily.sql` y la macro `heat_category`). Mostrar los tests en `_marts.yml` y `tests/assert_temp_max_ge_min.sql`. Ejecutar `dbt build`. |
| 2:40–3:45 | Portal publicado y linaje de dbt | Abrir el sitio desplegado en GitHub Pages (`index.html`): KPIs, serie mensual de temperaturas, precipitaciones por ciudad, anomalía térmica de 30 días, eventos extremos y bitácora de corridas. Abrir `/docs/` para mostrar el grafo de linaje (DAG) de dbt y los enlaces a CSV/Parquet. |
| 3:45–4:35 | Automatización CI/CD (`.github/workflows/pipeline.yml`) | Mostrar el workflow con trigger `push`, `workflow_dispatch` y `cron` diario, el caché de `weather.duckdb` y el despliegue automático a GitHub Pages. Mostrar la corrida en verde en la pestaña **Actions**. |
| 4:35–5:00 | Conclusiones | Resumen: ELT reproducible, incremental, con 27 pruebas de calidad de datos, documentación viva y despliegue 100% automatizado sin servidores. |
