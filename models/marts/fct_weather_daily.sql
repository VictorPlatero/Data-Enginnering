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
    -- Solo procesa lo nuevo (con 7 días de solapamiento por si hubo correcciones)
    where weather_date > (select max(weather_date) - interval 7 day from {{ this }})
    {% endif %}
)

select
    w.city_id,
    w.weather_date,
    w.weather_code,
    coalesce(wc.description, 'Desconocido')        as weather_description,
    coalesce(wc.category, 'Desconocido')           as weather_category,
    w.temp_max_c,
    w.temp_min_c,
    w.temp_mean_c,
    w.temp_range_c,
    w.precipitation_mm,
    w.rain_mm,
    w.precipitation_hours,
    w.wind_max_kmh,
    w.wind_gusts_kmh,
    w.radiation_mj_m2,
    w.sunshine_hours,
    {{ heat_category('w.temp_max_c') }}            as heat_category,
    w.precipitation_mm >= 1                        as is_rainy_day,
    w.precipitation_mm >= 20                       as is_heavy_rain,
    w.temp_max_c >= 30                             as is_hot_day,
    w.temp_min_c <= 0                              as is_frost_day,
    w.ingested_at
from weather w
left join {{ ref('weather_codes') }} wc using (weather_code)
