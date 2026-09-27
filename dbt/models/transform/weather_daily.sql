-- Cleaned pass-through of the raw table, plus a composite surrogate key
-- (city_date) used for uniqueness testing and snapshot keying downstream,
-- since Snowflake does not enforce the (city, date) primary key declared
-- on the raw table.

select
    city,
    latitude,
    longitude,
    date as weather_date,
    temp_max,
    temp_min,
    precipitation,
    weather_code,
    city || '_' || to_varchar(date, 'YYYY-MM-DD') as city_date
from {{ source('raw', 'city_weather_daily') }}
where city is not null
  and date is not null
