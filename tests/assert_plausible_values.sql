-- Rangos físicamente plausibles para el Perú
select city_id, weather_date, temp_max_c, temp_min_c, precipitation_mm
from {{ ref('fct_weather_daily') }}
where temp_max_c > 45 or temp_min_c < -25 or precipitation_mm > 400
   or weather_date > current_date
