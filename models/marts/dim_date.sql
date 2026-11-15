with bounds as (
    select min(weather_date) as d0, max(weather_date) as d1 from {{ ref('stg_weather_daily') }}
),
dates as (
    select cast(d as date) as date_day
    from bounds, range(bounds.d0, bounds.d1 + interval 1 day, interval 1 day) t(d)
)
select
    date_day,
    year(date_day)                                      as year,
    quarter(date_day)                                   as quarter,
    month(date_day)                                     as month,
    strftime(date_day, '%Y-%m')                         as year_month,
    ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'][month(date_day)] as month_name,
    isodow(date_day)                                    as day_of_week,
    -- Estación del año en el hemisferio sur
    case
        when month(date_day) in (12, 1, 2) then 'Verano'
        when month(date_day) in (3, 4, 5)  then 'Otoño'
        when month(date_day) in (6, 7, 8)  then 'Invierno'
        else 'Primavera'
    end                                                 as season
from dates
