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
        INITCAP(COALESCE(rwb.dam, '')) AS dam,
        INITCAP(COALESCE(rwb.district, '')) AS district,
        CASE  -- Standardize state names
            WHEN
                LOWER(rwb.state) LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(rwb.state) LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(rwb.state, ''))
        END AS state,
        INITCAP(COALESCE(rwb.taluka, '')) AS taluka,
        INITCAP(COALESCE(rwb.village, '')) AS village,
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
            location ->> 'Nalla' = rwb.dam
    WHERE
        "Subject_type" = 'Work Order'
        AND "Voided" = False
)

{{ dbt_utils.deduplicate(
    relation='mycte',
    partition_by='work_order_id',
    order_by='work_order_id DESC'
) }}
