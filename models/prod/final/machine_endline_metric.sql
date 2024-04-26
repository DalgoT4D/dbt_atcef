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
    e.type_of_machine,
    e.date_time,
    ROUND(SUM(w.silt_achieved / CASE WHEN COALESCE(e.total_working_hours_of_machine, 0) = 0 THEN 1 ELSE e.total_working_hours_of_machine END), 2) AS avg_silt_excavated_per_hour
  FROM {{ref('machine_endline_aggregated')}} e
  INNER JOIN {{ref('work_order_metric')}} w ON e.district = w.district AND e.dam = w.dam
  WHERE total_working_hours_of_machine != 0
  GROUP BY
    e.state, e.district, e.village, e.taluka, e.dam, e.type_of_machine, e.date_time
),

cte as (SELECT
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

select * from cte Where benchmark_classification is not null