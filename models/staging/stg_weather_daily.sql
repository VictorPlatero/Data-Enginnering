with source as (
    select * from {{ source('raw', 'weather_daily') }}
)

select
    city_id,
    date                                         as weather_date,
    weather_code,
    round(temperature_2m_max, 1)                 as temp_max_c,
    round(temperature_2m_min, 1)                 as temp_min_c,
    round(coalesce(temperature_2m_mean,
                   (temperature_2m_max + temperature_2m_min) / 2), 1) as temp_mean_c,
    round(temperature_2m_max - temperature_2m_min, 1) as temp_range_c,
    coalesce(precipitation_sum, 0)               as precipitation_mm,
    coalesce(rain_sum, 0)                        as rain_mm,
    coalesce(precipitation_hours, 0)             as precipitation_hours,
    wind_speed_10m_max                           as wind_max_kmh,
    wind_gusts_10m_max                           as wind_gusts_kmh,
    shortwave_radiation_sum                      as radiation_mj_m2,
    round(sunshine_duration / 3600.0, 2)         as sunshine_hours,
    _ingested_at                                 as ingested_at,
    _run_id                                      as run_id
from source
where temperature_2m_max is not null
