{{ config(
  materialized='table'
) }}

SELECT
    distinct dam,
    district,
    date_time,
    state,
    taluka,
    village,
    project_status,
    project_status_value
FROM
    (SELECT
        distinct dam,
        district,
        date_time,
        state,
        taluka,
        village,
        project_status,
        CASE 
            WHEN project_status = 'Ongoing' THEN 1 -- Adjust based on actual 'Ongoing' value
            ELSE 0 -- Assuming any other value means not started
        END as project_status_value
     FROM {{ ref('work_order_union') }} ) AS original_table
CROSS JOIN LATERAL
    (VALUES
        ('project_status', project_status::text)
    ) AS unpivoted(attribute, value)
