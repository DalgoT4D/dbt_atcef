{{ config(
  materialized='table'
) }}


WITH excavated_data AS (
  SELECT
    max(e.date_time) as date_time,
    e.state,
    e.district,
    e.village,
    e.taluka,
    e.waterbodies,
    e.type_of_machine,
    ROUND(SUM(w.silt_achieved / CASE WHEN COALESCE(e.total_working_hours_of_machine, 0) = 0 THEN 1 ELSE e.total_working_hours_of_machine END), 2) AS avg_silt_excavated_per_hour
  FROM
    {{ ref('machine_endline_aggregated24') }} AS e
    left JOIN {{ ref('work_order_metric24') }} w ON e.district = w.district AND e.waterbodies = w.dam
  GROUP BY
    e.state, e.district, e.village, e.taluka, e.waterbodies, e.type_of_machine, e.date_time
),

benchmark_cte AS (
  SELECT
    date_time,
    state,
    district,
    village,
    taluka,
    waterbodies,
    type_of_machine,
    avg_silt_excavated_per_hour,
    CASE
      WHEN type_of_machine = 'Jcb' AND avg_silt_excavated_per_hour < 39.2 THEN 'Below Benchmark'
      WHEN type_of_machine = 'Jcb' AND avg_silt_excavated_per_hour >= 39.2 THEN 'Above Benchmark'
      WHEN type_of_machine = 'Poclain' AND avg_silt_excavated_per_hour < 89.6 THEN 'Below Benchmark'
      WHEN type_of_machine = 'Poclain' AND avg_silt_excavated_per_hour >= 89.6 THEN 'Above Benchmark'
    END AS benchmark_classification
  FROM excavated_data
)

SELECT *
FROM benchmark_cte
WHERE benchmark_classification IS NOT NULL
