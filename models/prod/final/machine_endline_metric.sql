{{ config(
  materialized='table'
) }}

WITH excavated_data AS (
  SELECT
    e.district,
    e.type_of_machine,
    w.silt_achieved, 
    CASE
      WHEN CAST(COALESCE(e.total_working_hours_of_machine, '0') AS NUMERIC) = 0 THEN NULL
      ELSE w.silt_achieved / CAST(COALESCE(e.total_working_hours_of_machine, '1') AS NUMERIC)
    END as silt_excavated_per_hour
  FROM {{ref('machine_endline_aggregated')}} e
  INNER JOIN {{ref('work_order_metric')}} w ON e.district = w.district AND e.dam = w.dam
)

SELECT
  district,
  type_of_machine,
  AVG(silt_excavated_per_hour) as avg_silt_excavated_per_hour,
  CASE
    WHEN type_of_machine = 'JCB' AND AVG(silt_excavated_per_hour) < 39.2 THEN 'Below Benchmark'
    WHEN type_of_machine = 'JCB' AND AVG(silt_excavated_per_hour) >= 39.2 THEN 'Above Benchmark'
    WHEN type_of_machine = 'Poclain' AND AVG(silt_excavated_per_hour) < 89.6 THEN 'Below Benchmark'
    WHEN type_of_machine = 'Poclain' AND AVG(silt_excavated_per_hour) >= 89.6 THEN 'Above Benchmark'
    ELSE 'Benchmark Not Applicable'
  END as benchmark_classification
FROM excavated_data
GROUP BY district, type_of_machine