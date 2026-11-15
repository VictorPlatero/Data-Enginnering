-- KPIs por ciudad: últimos 30 días vs. promedio histórico del mismo periodo del año
with base as (
    select f.*, max(f.weather_date) over () as max_date
    from {{ ref('fct_weather_daily') }} f
),
ultimos as (
    select city_id,
           round(avg(temp_mean_c), 2)      as avg_temp_30d,
           round(sum(precipitation_mm), 1) as precip_30d,
           max(temp_max_c)                 as max_temp_30d,
           min(temp_min_c)                 as min_temp_30d
    from base
    where weather_date > max_date - interval 30 day
    group by city_id
),
historico as (
    select city_id,
           round(avg(temp_mean_c), 2) as avg_temp_hist_same_period
    from base
    where weather_date <= max_date - interval 30 day
      and dayofyear(weather_date) between dayofyear(max_date) - 30 and dayofyear(max_date)
    group by city_id
)
select
    c.city,
    c.department,
    c.natural_region,
    c.latitude,
    c.longitude,
    c.last_date,
    u.avg_temp_30d,
    h.avg_temp_hist_same_period,
    round(u.avg_temp_30d - h.avg_temp_hist_same_period, 2) as temp_anomaly_c,
    u.precip_30d,
    u.max_temp_30d,
    u.min_temp_30d
from {{ ref('dim_city') }} c
left join ultimos u using (city_id)
left join historico h using (city_id)
