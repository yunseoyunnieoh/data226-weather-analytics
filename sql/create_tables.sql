-- DATA 226 Team Lab: Weather Prediction Analytics (Seoul + Toronto)
--
-- This DDL is executed automatically by dags/weather_etl_dag.py on every run
-- (CREATE TABLE IF NOT EXISTS). It is checked in separately so the table
-- design is easy to review without reading the DAG code.
--
-- We use a NEW table name (CITY_WEATHER_DAILY) instead of reusing the HW2/HW3
-- table (DEV.RAW.WEATHER_DAILY), which already holds single-city (San Jose)
-- data with a different column set. Reusing that name would either collide
-- with graded homework data or silently fail to add the CITY column via
-- CREATE TABLE IF NOT EXISTS (it only runs when the table does not exist).

CREATE DATABASE IF NOT EXISTS DEV;
CREATE SCHEMA IF NOT EXISTS DEV.RAW;

CREATE TABLE IF NOT EXISTS DEV.RAW.CITY_WEATHER_DAILY (
    CITY          VARCHAR(64)     NOT NULL,  -- e.g. 'Seoul', 'Toronto'; drives partitioning in dbt
    LATITUDE      FLOAT           NOT NULL,
    LONGITUDE     FLOAT           NOT NULL,
    DATE          DATE            NOT NULL,
    TEMP_MAX      FLOAT,                      -- degrees Celsius
    TEMP_MIN      FLOAT,                      -- degrees Celsius
    PRECIPITATION FLOAT,                      -- millimeters
    WEATHER_CODE  INTEGER,                    -- WMO weather interpretation code
    LOADED_AT     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CITY, DATE)
);

-- NOTE: Snowflake declares but does NOT enforce PRIMARY KEY / UNIQUE
-- constraints on standard tables. Idempotency (no duplicate CITY+DATE rows
-- on rerun) is enforced in application logic in weather_etl_dag.py via a
-- MERGE statement keyed on (CITY, DATE), executed inside an explicit
-- BEGIN/COMMIT transaction with a duplicate-key verification query before
-- COMMIT. See dags/weather_etl_dag.py::load().
