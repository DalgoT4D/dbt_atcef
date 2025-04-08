{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2022", "niti_2022", "niti"]
) }}

WITH mycte AS (
    SELECT
        s."ID" AS work_order_id,
        s."Voided" AS work_order_voided,
        INITCAP(
            TRIM(COALESCE(s.observations ->> 'First name', ''))
        ) AS work_order_name,
        INITCAP(COALESCE(s.location ->> 'Dam', '')) AS dam,
        INITCAP(COALESCE(s.location ->> 'District', '')) AS district,
        CASE
            WHEN
                LOWER(s.location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(s.location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(s.location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(s.location ->> 'Taluka', '')) AS taluka,
        INITCAP(COALESCE(s.location ->> 'GP/Village', '')) AS village,
        COALESCE(NULLIF(rwb."Project/NGO", ''), 'Unknown') AS ngo_name,
        CAST(
            COALESCE(
                NULLIF(rwb."Estimated quantity of Silt", ''), '0'
            ) AS numeric
        ) AS silt_target
    FROM
        rwb_niti_2022.rwbniti22 AS rwb
    LEFT JOIN
        {{ source('source_atecf_surveyss', 'subjects_2022') }} AS s
        ON rwb."Dam" = s.location ->> 'Dam'
    WHERE
        NOT (LOWER(s.location ->> 'Dam') ~ 'voided')
        AND s."Subject_type" = 'Work Order'
        AND s."Voided" = False
),

approval_work_orders AS (
    SELECT
        d.*,
        a.approval_status AS work_order_approval_status
    FROM
        mycte AS d
    INNER JOIN
        {{ ref('approval_statuses_niti_2022') }} AS a
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
