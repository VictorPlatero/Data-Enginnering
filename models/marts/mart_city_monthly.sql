select
    c.city,
    c.natural_region,
    d.year,
    d.month,
    d.year_month,
    d.month_name,
    d.season,
    count(*)                                  as days,
    round(avg(f.temp_mean_c), 2)              as avg_temp_c,
    max(f.temp_max_c)                         as max_temp_c,
    min(f.temp_min_c)                         as min_temp_c,
    round(sum(f.precipitation_mm), 1)         as total_precip_mm,
    count(*) filter (where f.is_rainy_day)    as rainy_days,
    count(*) filter (where f.is_hot_day)      as hot_days,
    count(*) filter (where f.is_frost_day)    as frost_days,
    round(avg(f.sunshine_hours), 2)           as avg_sunshine_h,
    round(max(f.wind_gusts_kmh), 1)           as max_gust_kmh
from {{ ref('fct_weather_daily') }} f
join {{ ref('dim_city') }} c using (city_id)
join {{ ref('dim_date') }} d on d.date_day = f.weather_date
group by all
