
{{ config(
  materialized='table'
) }}


WITH max_dates AS (
    SELECT
        dam,
        MAX(date_time) AS max_date_time
    FROM {{ ref('work_order_union') }}
    GROUP BY dam
)

SELECT DISTINCT ON (w.dam) 
    w.dam AS waterbodies,
    w.date_time,
    w.state,
    w.village,
    w.taluka,
    w.district,
    w.ngo_name,
    w.encounter_type,
    w.project_ongoing,
    w.project_not_started,
    w.project_completed
FROM {{ ref('work_order_union') }} w
JOIN max_dates md ON w.dam = md.dam AND w.date_time = md.max_date_time
ORDER BY w.dam, w.date_time
