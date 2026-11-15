select
    city_id,
    city,
    department,
    natural_region,
    latitude,
    longitude
from {{ ref('cities') }}
