{{ config(
  materialized='table',
  tags=["final","final_niti_2023"]
) }}

WITH excavated_data AS (
  SELECT
    e.state,
    e.district,
    e.village,
    e.taluka,
    e.dam,
    e.type_of_machine,
    e.date_time,
    SUM(w.total_silt_carted) as total_silt_carted,
    SUM(e.total_working_hours) as total_working_hours,
    ROUND(
      CAST(SUM(w.total_silt_carted) AS NUMERIC) / NULLIF(CAST(SUM(e.total_working_hours) AS NUMERIC), 0), 
      2
    ) AS avg_silt_excavated_per_hour
  FROM {{ref('machine_niti_2023_agg')}} e
  INNER JOIN {{ref('farmer_silt_calc_niti_2023')}} w 
  ON e.state = w.state AND e.taluka=w.taluka AND e.village=w.village AND e.district=w.district AND e.dam = w.dam 
  WHERE w.total_silt_carted > 0 AND e.total_working_hours > 0 
  GROUP BY
    e.state, e.district, e.village, e.taluka, e.dam, e.type_of_machine, e.date_time
),

cte as (
  SELECT
    date_time,
    state,
    district,
    village,
    taluka,
    dam,
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
