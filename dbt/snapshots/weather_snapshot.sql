{% snapshot weather_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='city_date',
    strategy='check',
    check_cols=[
        'temp_max', 'temp_min', 'precipitation', 'weather_code',
        'moving_avg_temp_7d', 'temp_anomaly', 'rolling_precip_7d'
    ],
    invalidate_hard_deletes=True
  )
}}

-- 'check' strategy (not 'timestamp'): Open-Meteo revises very recent days as
-- more observations come in, and there is no natural updated_at column on
-- the analytics table. Snapshotting weather_analytics lets us see exactly
-- how a given (city, date) row's metrics changed between dbt runs.
select * from {{ ref('weather_analytics') }}

{% endsnapshot %}
