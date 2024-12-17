{{ config(
  materialized='table'
) }}


WITH mycte AS (
    SELECT
        "ID" AS work_order_id,
        INITCAP(TRIM(COALESCE(observations->>'First name', ''))) AS work_order_name,
        INITCAP(COALESCE(location->>'Dam', '')) AS dam,
        INITCAP(COALESCE(location->>'District', '')) AS district,
        CASE  -- Standardize state names
            WHEN LOWER(location->>'State') LIKE '%maharashtra%' THEN 'Maharashtra'
            WHEN LOWER(location->>'State') LIKE '%maharshatra%' THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location->>'State', ''))
        END AS state,
        INITCAP(COALESCE(location->>'Taluka', '')) AS taluka,
        INITCAP(COALESCE(location->>'GP/Village', '')) AS village,
        COALESCE(NULLIF(rwb."Project/NGO", ''), 'Unknown') AS ngo_name,
        CAST(COALESCE(NULLIF(rwb."Estimated quantity of Silt", ''), '0') AS numeric) AS silt_target,
        "Voided" AS work_order_voided
    FROM 
        {{ source('source_atecf_surveyss', 'subjects_2022') }}
    RIGHT JOIN rwb_niti_2022.rwbniti22 AS rwb ON location->>'Dam' = rwb."Dam"
    WHERE 
        NOT (LOWER(location->>'Dam') ~ 'voided') 
        AND "Subject_type" = 'Work Order' 
        and "Voided" = False
),

approval_work_orders AS (
    SELECT 
        d.*, 
        a.approval_status AS work_order_approval_status
    FROM 
        mycte d
    JOIN 
        {{ ref('approval_statuses_niti_2022') }} a 
    ON 
        d.work_order_id = a.entity_id
    WHERE 
        a.entity_type = 'Subject' 
        AND a.approval_status = 'Approved'
)

{{ dbt_utils.deduplicate(
    relation='approval_work_orders',
    partition_by='work_order_id',
    order_by='work_order_id DESC'
) }}
