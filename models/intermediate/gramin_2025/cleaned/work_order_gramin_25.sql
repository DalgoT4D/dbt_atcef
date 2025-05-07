{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin_niti", "gramin_niti", "gramin_25"]
) }}


WITH work_orders AS (
    SELECT
        subjects."ID" AS work_order_id,
        subjects."Location_ID" AS address_id,
        subjects."Voided" AS work_order_voided,
        INITCAP(
            TRIM(COALESCE(subjects.observations ->> 'First name', ''))
        ) AS work_order_name,
        rwb.dam,
        INITCAP(COALESCE(subjects.location ->> 'District', '')) AS district,
        CASE  -- Standardize state names
            WHEN
                LOWER(subjects.location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(subjects.location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(subjects.location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(subjects.location ->> 'Taluka', '')) AS taluka,
        INITCAP(COALESCE(subjects.location ->> 'GP/Village', '')) AS village,
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
        {{ source('source_gramin_25', 'subjects_gramin_25') }} AS subjects
    LEFT JOIN
        {{ ref('address_gramin_25') }} AS rwb
        ON
            subjects."Location_ID" = rwb.address_id
    WHERE
        "Subject_type" = 'Work Order'
)

{{ dbt_utils.deduplicate(
    relation='work_orders',
    partition_by='work_order_id',
    order_by='work_order_id DESC'
) }}
