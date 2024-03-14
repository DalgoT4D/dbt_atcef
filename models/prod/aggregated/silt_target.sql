{{ config(
  materialized='table'
) }}


-- models/silt_differences.sql

WITH silt_calculations AS (
    SELECT
        dam,
        date,
        district,
        state,
        taluka,
        village,
        silt_to_be_excavated,
        silt_target,
        (silt_to_be_excavated - silt_target) * 1000 AS silt_difference_litres,
        (silt_to_be_excavated * 1000) / 10000 AS equivalent_water_tankers
    FROM
        {{ ref('work_order') }}
)

SELECT
    dam,
    date,
    district,
    state,
    taluka,
    village,
    silt_to_be_excavated,
    silt_target,
    silt_difference_litres,
    equivalent_water_tankers
FROM
    silt_calculations
