{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2023", "gdgs_2023", "gdgs"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS work_order_id,
        "Voided" AS work_order_voided,
        INITCAP(
            TRIM(COALESCE(observations ->> 'First name', ''))
        ) AS work_order_name,
        INITCAP(COALESCE(location ->> 'Dam', '')) AS dam,
        INITCAP(COALESCE(location ->> 'District', '')) AS district,
        CASE  -- Standardize state names
            WHEN
                LOWER(location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(location ->> 'Taluka', '')) AS taluka,
        INITCAP(COALESCE(location ->> 'GP/Village', '')) AS village,
        COALESCE(
            NULLIF(rwb.stakeholder_responsible, ''), 'Unknown'
        ) AS ngo_name,
        ROUND(CAST(CAST(
            COALESCE(
                NULLIF(TRIM(CAST(rwb.silt_target AS text)), ''),
                '0'
            ) AS float
        ) AS numeric), 2) AS silt_target
    FROM
        {{ source('source_gdgsom_surveys_2023', 'subjects_2023') }}
    LEFT JOIN
        {{ ref('address_gdgs_23') }} AS rwb
        ON
            location ->> 'Dam' = rwb.dam
    WHERE
        NOT (LOWER(location ->> 'Dam') ~ 'voided')
        AND "Subject_type" = 'Work Order'
        AND "Voided" = False
),

approval_work_orders AS (
    SELECT
        d.*,
        a.approval_status AS work_order_approval_status
    FROM
        mycte AS d
    INNER JOIN
        {{ ref('approval_statuses_gdgs_2023') }} AS a
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
