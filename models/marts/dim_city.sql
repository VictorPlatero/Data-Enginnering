select
    c.city_id,
    c.city,
    c.department,
    c.natural_region,
    c.latitude,
    c.longitude,
    count(w.weather_date)   as days_observed,
    min(w.weather_date)     as first_date,
    max(w.weather_date)     as last_date
from {{ ref('stg_cities') }} c
left join {{ ref('stg_weather_daily') }} w using (city_id)
group by all
