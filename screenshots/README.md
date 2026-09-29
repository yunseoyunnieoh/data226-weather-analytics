# Screenshot checklist

All 10 required screenshots are captured and present in this folder,
verified by opening each file directly (not assumed from what was reported
as done).

| # | Filename | Shows |
| - | --- | --- |
| 1 | `01_airflow_variables.png` | Airflow Admin > Variables: the `CITY_CONFIG` row with the Seoul/Toronto JSON |
| 2 | `02_snowflake_connection.png` | Airflow Admin > Connections: `snowflake_conn`, type `snowflake`. No host, port, or secret values are shown |
| 3 | `03_etl_dag_graph.png` | `weather_etl_dag` Graph view: `extract -> transform -> load -> trigger_dbt_dag`, all green |
| 4 | `04_etl_load_log.png` | Snowflake query on `RAW.CITY_WEATHER_DAILY` grouped by city: 61 rows each for Seoul (2026-07-31 to 2026-09-29) and Toronto (2026-07-30 to 2026-09-28), with `last_loaded` matching the scheduled 02:30 UTC load |
| 5 | `05_dbt_dag_graph.png` | `weather_dbt_dag` Graph view: `dbt_run -> dbt_test -> dbt_snapshot`, all green |
| 6 | `06_dbt_run_log.png` | `dbt_run` task log: `1 of 1 OK created sql table model analytics.weather_analytics`, `Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1` |
| 7 | `07_dbt_test_log.png` | `dbt_test` task log: all 5 tests listed individually as `PASS`, `Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5` |
| 8 | `08_dbt_snapshot_log.png` | `dbt_snapshot` task log: `1 of 1 OK snapshotted snapshots.weather_snapshot [SUCCESS 16]`, `Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1` |
| 9 | `09_dashboard_overview.png` | Preset "Weather Analytics Dashboard", default state (no filters): all four charts showing both Seoul and Toronto |
| 10 | `10_dashboard_filtered.png` | Same dashboard filtered to City = Seoul and Date Range 2026-09-01 to 2026-09-20: all four charts update to Seoul over the narrower window |

## Environment / evidence notes

- Screenshots 1-3 and 5-8 were taken from a local Airflow instance at
  `http://localhost:8082` (see repo root `README.md` -> "How to run Airflow"
  for why port 8082, not the default 8081, was used).
- Screenshots 4-8 come from the same pipeline run on 2026-09-29, triggered automatically by that run's
  `weather_etl_dag` execution -- consistent with the ETL-then-dbt dependency
  described in `docs/report.md` Section 14.
- Screenshots 9-10 are from the Preset dashboard described in the repo root
  `README.md` -> "Dashboard", connected to Snowflake via key-pair
  authentication through Preset's Secure Extra field (see that section and
  `docs/report.md` Section 15 for why, and for the security note on that
  field never being committed anywhere in this repo).
- No screenshot shows a password, private key, or Secure Extra
  configuration screen.
