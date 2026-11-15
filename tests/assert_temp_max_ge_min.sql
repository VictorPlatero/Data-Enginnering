-- La temperatura máxima nunca debe ser menor que la mínima
select city_id, weather_date, temp_max_c, temp_min_c
from {{ ref('fct_weather_daily') }}
where temp_max_c < temp_min_c
