-- Eventos climáticos extremos: lluvia intensa, calor extremo, heladas y ráfagas fuertes
with eventos as (
    select city_id, weather_date, 'Lluvia intensa' as event_type, precipitation_mm as value, 'mm' as unit
    from {{ ref('fct_weather_daily') }} where is_heavy_rain
    union all
    select city_id, weather_date, 'Calor (≥30 °C)', temp_max_c, '°C'
    from {{ ref('fct_weather_daily') }} where is_hot_day
    union all
    select city_id, weather_date, 'Helada (≤0 °C)', temp_min_c, '°C'
    from {{ ref('fct_weather_daily') }} where is_frost_day
    union all
    select city_id, weather_date, 'Ráfagas (≥60 km/h)', wind_gusts_kmh, 'km/h'
    from {{ ref('fct_weather_daily') }} where wind_gusts_kmh >= 60
)
select
    c.city,
    c.natural_region,
    e.weather_date,
    e.event_type,
    round(e.value, 1) as value,
    e.unit
from eventos e
join {{ ref('dim_city') }} c using (city_id)
