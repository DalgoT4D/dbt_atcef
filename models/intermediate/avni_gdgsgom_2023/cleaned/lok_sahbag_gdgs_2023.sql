{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2023"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS lok_sahbhag_id,
        INITCAP(TRIM(COALESCE(observations->>'First name', ''))) AS lok_sahbhag_name,
        INITCAP(COALESCE(location->>'District', '')) AS district,
        CASE  -- Standardize state names
            WHEN LOWER(location->>'State') LIKE '%maharashtra%' THEN 'Maharashtra'
            WHEN LOWER(location->>'State') LIKE '%maharshatra%' THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location->>'State', ''))
        END AS state,
        "Voided" AS lok_sahbhag_voided,
        observations->>'End Date' AS end_date,
        observations->>'Image of site after work' AS image_after_work,
        observations->>'Image of site before work' AS image_before_work,
        observations->>'Image of site during work' AS image_during_work,
        observations->>'Name of Taluka' AS taluka,
        observations->>'Name of Village' AS village,
        observations->>'Name of WB' AS dam,
        observations->>'Start Date' AS start_date,
        cast(observations->>'Total Silt Excavated' as numeric) AS total_silt_excavated,
        observations->>'Type of Lok Sahbhag' AS lok_sahbhag_type,
        observations->>'Village Code' AS village_code,
        "Registration_date"::timestamp AS date_time
    FROM 
        {{ source('source_gdgsom_surveys_2023', 'subjects_2023') }}
    WHERE 
       "Subject_type" = 'Lok-sahbhag' and 
        "Voided" = False
)

select * from mycte