{{ config(
  materialized='table'
) }}

WITH max_dates AS (
    SELECT
        dam,
        MAX(date_time) AS max_date_time
    FROM {{ ref('work_order_gdgs_union') }}
    WHERE dam IS NOT NULL
    GROUP BY dam
)

SELECT DISTINCT ON (w.dam)
    w.dam AS waterbodies,
    w.date_time,
    w.state,
    w.village,
    w.taluka,
    w.district,
    w.encounter_type,
    CONCAT(
        CASE WHEN w.project_ongoing IS NOT NULL THEN 'Ongoing' ELSE '' END,
        CASE WHEN w.project_not_started IS NOT NULL THEN 'Not Started' ELSE '' END,
        CASE WHEN w.project_completed IS NOT NULL THEN 'Completed' ELSE '' END
    ) AS project_status
FROM {{ ref('work_order_gdgs_union') }} w
JOIN max_dates md ON w.dam = md.dam AND w.date_time = md.max_date_time
WHERE w.dam IS NOT NULL
ORDER BY w.dam, w.date_time
