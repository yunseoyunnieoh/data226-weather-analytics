-- Intermediate layer: adds the daily average temperature and a
-- "dry spell length" counter (consecutive days ending on this date with
-- under 0.1mm of precipitation), both partitioned by city so Seoul and
-- Toronto are never mixed together in the same window.
--
-- Dry spell length uses a classic gaps-and-islands trick: every wet day
-- (is_dry_day = 0) starts a new group; running SUM() of "not dry" flags,
-- partitioned by city, assigns the same group number to every day in one
-- unbroken dry streak. ROW_NUMBER() within that group is the streak length.

with base as (
    select * from {{ ref('stg_weather') }}
),

flagged as (
    select
        *,
        (temp_max + temp_min) / 2 as daily_avg_temp,
        case when coalesce(precipitation, 0) < 0.1 then 1 else 0 end as is_dry_day
    from base
),

dry_groups as (
    select
        *,
        sum(case when is_dry_day = 0 then 1 else 0 end)
            over (partition by city order by weather_date rows unbounded preceding) as wet_day_group
    from flagged
)

select
    city,
    latitude,
    longitude,
    weather_date,
    city_date,
    temp_max,
    temp_min,
    precipitation,
    weather_code,
    daily_avg_temp,
    is_dry_day,
    case
        when is_dry_day = 1
            then row_number() over (partition by city, wet_day_group order by weather_date)
        else 0
    end as dry_spell_length
from dry_groups
