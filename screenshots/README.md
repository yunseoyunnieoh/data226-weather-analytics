# Screenshot checklist

All 10 required screenshots are captured and present in this folder,
verified by opening each file directly (not assumed from what was reported
as done).

| # | Filename | Status | Content verified |
| - | --- | --- | --- |
| 1 | `01_airflow_variables.png` | **COMPLETE AND PRESENT IN REPO** | Airflow Admin > Variables, `CITY_CONFIG` row with Seoul/Toronto JSON |
| 2 | `02_snowflake_connection.png` | **COMPLETE AND PRESENT IN REPO** | Airflow Admin > Connections list: `snowflake_conn`, type `snowflake`, Is Encrypted/Is Extra Encrypted both `False` -- no secret values shown |
| 3 | `03_etl_dag_graph.png` | **COMPLETE AND PRESENT IN REPO** | `weather_etl_dag` Graph view: extract, transform, load, trigger_dbt_dag all green |
| 4 | `04_etl_load_log.png` | **COMPLETE AND PRESENT IN REPO** | `load` task log: `BEGIN: upserting 122 rows...`, `COMMIT succeeded`, `Duplicate (city, date) keys: 0`, `Seoul: 61 rows`, `Toronto: 61 rows` |
| 5 | `05_dbt_dag_graph.png` | **COMPLETE AND PRESENT IN REPO** | `weather_dbt_dag` Graph view: dbt_run, dbt_snapshot, dbt_test all green |
| 6 | `06_dbt_run_log.png` | **COMPLETE AND PRESENT IN REPO** | `dbt_run` task log: 2 models built (`stg_weather` view, `weather_analytics` table), `Completed successfully`, `PASS=2 WARN=0 ERROR=0` |
| 7 | `07_dbt_test_log.png` | **COMPLETE AND PRESENT IN REPO** | `dbt_test` task log: all 10 tests listed individually as `PASS`, `Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10` |
| 8 | `08_dbt_snapshot_log.png` | **COMPLETE AND PRESENT IN REPO** | `dbt_snapshot` task log: `1 of 1 OK snapshotted snapshots.weather_snapshot ... [SUCCESS 244 in 6.47s]`, `Done. PASS=1 WARN=0 ERROR=0` |
| 9 | `09_dashboard_overview.png` | **COMPLETE AND PRESENT IN REPO** | Preset "Weather Analytics Dashboard", default state (no city/date filter applied), all four charts visible showing both Seoul and Toronto |
| 10 | `10_dashboard_filtered.png` | **COMPLETE AND PRESENT IN REPO** | Same dashboard filtered to City = Seoul and Date Range 2026-09-01 to 2026-09-20 -- all four charts visibly update to show only Seoul over the narrower window |

## Environment / evidence notes

- Screenshots 1-8 were captured from a real local Airflow instance at
  `http://localhost:8082` (see repo root `README.md` -> "How to run Airflow"
  for why port 8082, not the default 8081, was used).
- Screenshots 6-8 are all from the same `weather_dbt_dag` run
  (`2026-09-25, 18:44:08 UTC`), triggered automatically by that run's
  `weather_etl_dag` execution -- consistent with the ETL-then-dbt dependency
  described in `docs/report.md` Section 14.
- Screenshots 9-10 are from the Preset dashboard described in the repo root
  `README.md` -> "Dashboard", connected to Snowflake via key-pair
  authentication through Preset's Secure Extra field (see that section and
  `docs/report.md` Section 15 for why, and for the security note on that
  field never being committed anywhere in this repo).
- No screenshot shows a password, private key, or Secure Extra
  configuration screen.
