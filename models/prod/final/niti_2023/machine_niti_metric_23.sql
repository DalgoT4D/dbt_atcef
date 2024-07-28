{{ config(
  materialized='table'
) }}

WITH excavated_data AS (
  SELECT
    e.state,
    e.district,
    e.village,
    e.taluka,
    e.dam,
    w.ngo_name,
    e.type_of_machine,
    e.date_time,
    ROUND(SUM(w.total_silt_carted / CASE WHEN COALESCE(e.total_working_hours, 0) = 0 THEN 1 ELSE e.total_working_hours END)::numeric, 2) AS avg_silt_excavated_per_hour
  FROM {{ref('machine_niti_2023_agg')}} e
  INNER JOIN {{ref('farmer_silt_calc_niti_2023')}} w ON e.district = w.district AND e.dam = w.dam
  GROUP BY
    e.state, e.district, e.village, e.taluka, e.dam, e.type_of_machine, e.date_time, w.ngo_name
),

cte as (
  SELECT
    date_time,
    state,
    district,
    village,
    taluka,
    dam,
    ngo_name,
    type_of_machine,
    avg_silt_excavated_per_hour,
    CASE
      WHEN type_of_machine = 'JCB' AND avg_silt_excavated_per_hour < 39.2 THEN 'Below Benchmark'
      WHEN type_of_machine = 'JCB' AND avg_silt_excavated_per_hour >= 39.2 THEN 'Above Benchmark'
      WHEN type_of_machine = 'Poclain' AND avg_silt_excavated_per_hour < 89.6 THEN 'Below Benchmark'
      WHEN type_of_machine = 'Poclain' AND avg_silt_excavated_per_hour >= 89.6 THEN 'Above Benchmark'
    END as benchmark_classification
  FROM excavated_data
)

SELECT * 
FROM cte 
WHERE benchmark_classification is not null
