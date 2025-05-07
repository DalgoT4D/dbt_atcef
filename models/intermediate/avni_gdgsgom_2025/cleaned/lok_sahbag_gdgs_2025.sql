{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2025", "gdgs_2025", "gdgs"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS lok_sahbhag_id,
        "Voided" AS lok_sahbhag_voided,
        INITCAP(
            TRIM(COALESCE(observations ->> 'First name', ''))
        ) AS lok_sahbhag_name,
        INITCAP(COALESCE(rwb.district, '')) AS district,
        CASE  -- Standardize state names
            WHEN
                LOWER(location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location ->> 'State', ''))
        END AS state,
        observations ->> 'End Date' AS end_date,
        observations ->> 'Image of site after work' AS image_after_work,
        observations ->> 'Image of site before work' AS image_before_work,
        observations ->> 'Image of site during work' AS image_during_work,
        rwb.taluka,
        rwb.village,
        rwb.dam,
        observations ->> 'Start Date' AS start_date,
        CAST(
            observations ->> 'Total Silt Excavated' AS numeric
        ) AS total_silt_excavated,
        observations ->> 'Type of Lok Sahbhag' AS lok_sahbhag_type,
        observations ->> 'Village Code' AS village_code,
        CAST("Registration_date" AS timestamp) AS date_time,
        COALESCE(NULLIF(rwb.stakeholder_responsible, ''), 'Unknown') AS ngo_name
    FROM
        {{ source('gdgs_25_surveys', 'subjects_gdgs_2025') }}
    LEFT JOIN
        {{ ref('address_gdgs_2025') }} AS rwb
        ON
            observations ->> 'Name of WB' = rwb.dam
    WHERE
        "Subject_type" = 'Lok-sahbhag'
        AND "Voided" = False
)

SELECT * FROM mycte
