{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2025", "niti_2025", "niti"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS work_order_id,
        last_modified_at AS date_time,
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
        {{ source('rwb_niti_2025', 'subjects_niti_2025') }}
    LEFT JOIN
        {{ ref('address_niti_2025') }} AS rwb
        ON
            location ->> 'Dam' = rwb.dam
    WHERE
        NOT (LOWER(location ->> 'Dam') ~ 'voided')
        AND "Subject_type" = 'Work Order'
        AND "Voided" = False
)

{{ dbt_utils.deduplicate(
    relation='mycte',
    partition_by='work_order_id',
    order_by='work_order_id DESC'
) }}
