-- Final analytics table read by the BI dashboard.
--
-- Every window function is PARTITION BY city so Seoul and Toronto values
-- never blend together:
--   - moving_avg_temp_7d : trailing 7-day average of daily_avg_temp
--   - temp_anomaly       : today's daily_avg_temp minus that city's overall
--                          average over the loaded window (a simple baseline
--                          anomaly, not a multi-year climatological one)
--   - rolling_precip_7d  : trailing 7-day sum of precipitation
--   - dry_spell_length   : carried through from int_weather_metrics

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
    dry_spell_length,
    round(avg(daily_avg_temp) over (
        partition by city
        order by weather_date
        rows between 6 preceding and current row
    ), 2) as moving_avg_temp_7d,
    -- ROUND avoids floating-point noise (e.g. 2.8e-15 instead of 0) showing
    -- up as a non-zero value on the dashboard during a true dry spell.
    round(daily_avg_temp - avg(daily_avg_temp) over (partition by city), 2) as temp_anomaly,
    round(sum(coalesce(precipitation, 0)) over (
        partition by city
        order by weather_date
        rows between 6 preceding and current row
    ), 2) as rolling_precip_7d
from {{ ref('int_weather_metrics') }}
