{{ config(
  materialized='table'
) }}


WITH excavated_data AS (
  SELECT
    max(e.latest_date_time) as date_time,
    e.state,
    e.district,
    e.village,
    e.taluka,
    e.dam,
    e.type_of_machine,
    ROUND(SUM(w.silt_achieved / CASE WHEN COALESCE(e.total_working_hours, 0) = 0 THEN 1 ELSE e.total_working_hours END), 2) AS avg_silt_excavated_per_hour
  FROM
    {{ ref('machine_endline_gdgs_un') }} AS e
    left JOIN {{ ref('work_order_metric23-24') }} w ON e.district = w.district AND e.dam = w.dam
  GROUP BY
    e.state, e.district, e.village, e.taluka, e.dam, e.type_of_machine, e.latest_date_time
),

benchmark_cte AS (
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
    END AS benchmark_classification
  FROM excavated_data
)

SELECT *
FROM benchmark_cte
WHERE benchmark_classification IS NOT NULL
