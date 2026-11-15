**Título:** Construyendo un Pipeline de Data Engineering moderno con Python, dbt y DuckDB (sin AWS)

**Tags (4):** `dataengineering` `dbt` `duckdb` `python`

---

## ¿Qué hace un pipeline moderno de Data Engineering?

Un pipeline de **Ingeniería de Datos (Data Engineering)** confiable no consiste solo en mover datos de un punto A a un punto B. En producción necesitamos:

1. **Ingesta idempotente e incremental** con manejo de reintentos y registro de auditoría (*observability*).
2. **Transformación modular y declarativa** separada en capas (*staging* → *marts*).
3. **Pruebas de calidad de datos** automáticas (unicidad, integridad referencial, rangos válidos, frescura).
4. **Linaje y documentación** autogenerada.
5. **Orquestación y despliegue automatizado (CI/CD)** hacia la nube pública.

En este artículo construyo una solución **ELT (Extract, Load, Transform)** de punta a punta, **100% libre de AWS**, que monitorea el clima diario e identifica eventos extremos en **12 ciudades de la Costa, Sierra y Selva del Perú**, usando herramientas open source y gratuitas.

## Stack tecnológico

- 🐍 **Python (`requests` + `pandas`)** – Extractor incremental desde la API histórica de **Open-Meteo** con *exponential backoff* y bitácora de corridas.
- 🦆 **DuckDB** – Data warehouse columnar embebido donde conviven los esquemas `raw`, `seeds`, `staging` y `marts`.
- 🧱 **dbt (`dbt-duckdb`)** – Transformaciones SQL modulares, modelos incrementales, macros Jinja, 27 tests de calidad de datos y documentación con grafo de linaje (DAG).
- ⚙️ **GitHub Actions** – Orquestador *serverless* con ejecución programada (`cron` diario) + caché del warehouse entre corridas.
- 🌐 **GitHub Pages** – Publicación automática del dashboard interactivo (Plotly.js), la documentación estática de dbt y los *data products* en formato **CSV y Parquet**.

## Arquitectura ELT

```
Open-Meteo API (12 ciudades del Perú)
        │
        ▼  1. Extract & Load (pipeline/extract.py)
┌───────────────────────────────────────────────────┐
│ DuckDB Warehouse (weather.duckdb)                 │
│  ├─ raw.weather_daily + raw.ingestion_log         │
│  ├─ seeds (cities, weather_codes)                 │
│  ├─ staging (stg_weather_daily, stg_cities)       │
│  └─ marts (dim_city, dim_date, fct_weather_daily, │
│            mart_city_monthly, mart_extreme_events,│
│            mart_city_kpis)                        │
└───────────────────────────────────────────────────┘
        │
        ▼  2. dbt test + source freshness + docs generate
        │  3. pipeline/publish.py (CSV + Parquet + HTML)
        ▼
GitHub Pages (Dashboard + dbt Docs + Data Products)
```

## 1. Extract & Load incremental e idempotente

El módulo `pipeline/extract.py` consulta para cada ciudad cuál fue la última fecha cargada en `raw.weather_daily` y solicita a la API de Open-Meteo **únicamente los días faltantes**:

```python
def last_loaded(con: duckdb.DuckDBPyConnection, city_id: int) -> date | None:
    rows = con.execute(
        "SELECT max(date) FROM raw.weather_daily WHERE city_id = ?", [city_id]
    ).fetchall()
    return rows[0][0] if rows else None
```

La carga usa `INSERT OR REPLACE INTO raw.weather_daily` sobre la clave primaria `(city_id, date)`, garantizando **idempotencia**: si el pipeline se re-ejecuta para el mismo rango de fechas, no duplica registros. Además, cada ciudad procesada deja un registro en `raw.ingestion_log` con tiempos de inicio/fin, filas cargadas y estado (`success` / `failed`).

## 2. Transformación por capas con dbt

Organizamos el proyecto dbt en tres capas:

### Seeds (`seeds/`)
Catálogos versionados en Git:
- `cities.csv`: 12 ciudades del Perú con latitud, longitud y región natural (`Costa`, `Sierra`, `Selva`).
- `weather_codes.csv`: códigos meteorológicos WMO traducidos a categorías legibles.

### Staging (`models/staging/`)
Vistas ligeras que limpian tipos de datos, estandarizan nombres y convierten unidades (por ejemplo, duración de sol de segundos a horas):

```sql
select
    city_id,
    date as weather_date,
    weather_code,
    round(temperature_2m_max, 1) as temp_max_c,
    round(temperature_2m_min, 1) as temp_min_c,
    coalesce(precipitation_sum, 0) as precipitation_mm,
    round(sunshine_duration / 3600.0, 2) as sunshine_hours,
    _ingested_at as ingested_at
from {{ source('raw', 'weather_daily') }}
where temperature_2m_max is not null
```

### Marts (`models/marts/`)
Tablas listas para consumo analítico:
- `dim_city` y `dim_date` (incluye estación del hemisferio sur).
- `fct_weather_daily`: **modelo incremental** con estrategia `delete+insert` y ventana móvil de 7 días para absorber correcciones tardías de reanálisis climático:

```sql
{{
    config(
        materialized='incremental',
        unique_key=['city_id', 'weather_date'],
        incremental_strategy='delete+insert'
    )
}}

with weather as (
    select * from {{ ref('stg_weather_daily') }}
    {% if is_incremental() %}
    where weather_date > (select max(weather_date) - interval 7 day from {{ this }})
    {% endif %}
)
select
    w.*,
    {{ heat_category('w.temp_max_c') }} as heat_category,
    w.precipitation_mm >= 20 as is_heavy_rain,
    w.temp_max_c >= 30 as is_hot_day,
    w.temp_min_c <= 0 as is_frost_day
from weather w
```

- `mart_city_monthly`: agregaciones mensuales por ciudad y región natural.
- `mart_extreme_events`: detección de lluvias intensas (≥20 mm), calor (≥30 °C), heladas (≤0 °C) y ráfagas fuertes (≥60 km/h).
- `mart_city_kpis`: anomalía térmica de los últimos 30 días frente al promedio histórico del mismo periodo del año.

## 3. Data Quality Tests y Freshness

En dbt los datos se prueban igual que el código. Configuramos **27 pruebas automáticas** entre:

- **Tests genéricos** (`unique`, `not_null`, `accepted_values`, `relationships`) y tests propios en Jinja (`unique_combination`, `non_negative`).
- **Tests singulares de reglas de negocio** (`tests/assert_temp_max_ge_min.sql` y `tests/assert_plausible_values.sql`):

```sql
-- La temperatura máxima nunca debe ser menor que la mínima
select city_id, weather_date, temp_max_c, temp_min_c
from {{ ref('fct_weather_daily') }}
where temp_max_c < temp_min_c
```

Además, `dbt source freshness` verifica que `raw.weather_daily` haya recibido datos recientemente.

## 4. Orquestación y CI/CD con GitHub Actions

El workflow `.github/workflows/pipeline.yml` se dispara en cada `push` a `main`, manualmente (`workflow_dispatch`) o todos los días a las 06:00 UTC mediante `schedule`:

```yaml
on:
  push:
    branches: [main]
  schedule:
    - cron: "0 6 * * *"
  workflow_dispatch:
```

En cada corrida:
1. Ejecuta `pytest` para validar la lógica de extracción e idempotencia.
2. Restaura el archivo `warehouse/weather.duckdb` desde el caché de GitHub Actions para que la ingesta y el modelo incremental de dbt procesen solo el delta.
3. Ejecuta `python -m pipeline.run`, que encadena `extract` → `dbt seed` → `dbt build` → `dbt source freshness` → `dbt docs generate --static` → `publish`.
4. Publica el portal resultante en **GitHub Pages**.

## 5. Resultados

- ✅ Más de **12 000 registros climáticos diarios** reales de 12 ciudades del Perú.
- ✅ **37 ejecuciones exitosas en `dbt build`** (modelos + seeds + 27 pruebas de calidad).
- ✅ Portal público con gráficos interactivos, catálogo de linaje de dbt y descarga directa de tablas en **CSV y Parquet**.
- ✅ Costo de infraestructura: **$0** (sin AWS).

## Enlaces

- 📦 Repositorio: https://github.com/VictorPlatero/Data-Enginnering
- 🌐 Portal y Dashboard en vivo: https://victorplatero.github.io/Data-Enginnering/
- 📚 Documentación y DAG de dbt: https://victorplatero.github.io/Data-Enginnering/docs/
- 🎥 Video: (enlace al video)

*Actividad Grupal 01-U2 – Inteligencia de Negocios – Universidad Privada de Tacna.*
