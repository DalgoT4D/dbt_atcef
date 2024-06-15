{{ config(
  materialized='table'
) }}

WITH mycte AS (
    SELECT
        "ID" AS gram_id,
        INITCAP(TRIM(COALESCE(observations->>'First name'))) AS gram_panchyat_name,  
        INITCAP(COALESCE(location->>'Dam')) AS dam,  
        INITCAP(COALESCE(location->>'District')) AS district,  
        "Subject_type" AS subject_type,
        CASE  -- Standardize state names
            WHEN LOWER(location->>'State') LIKE '%maharashtra%' THEN 'Maharashtra'
            WHEN LOWER(location->>'State') LIKE '%maharshatra%' THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location->>'State', ''))
        END AS state,
        INITCAP(COALESCE(location->>'Taluka')) AS taluka, 
        INITCAP(COALESCE(location->>'GP/Village')) AS village,
        "Voided" AS gram_panchayat_voided
    FROM 
        {{ source('source_atecf_surveys', 'subjects_2023') }}
    WHERE "Subject_type" = 'Gram panchayat' AND "Voided" = 'False'
), 

approval_gram_panchyat AS (
    SELECT d.*, a.approval_status AS farmer_approval_status
    FROM mycte d
    JOIN {{ ref('approval_statuses_niti_2023') }} a ON d.gram_id = a.entity_id
    WHERE a.entity_type = 'Subject' 
    AND a.approval_status = 'Approved'
)

SELECT s.*,
       e.total_silt_excavated_by_GP_for_non_farm_purpose
FROM approval_gram_panchyat AS s
LEFT JOIN {{ ref('encounter_2023') }} AS e 
     ON e.subject_id = s.gram_id  -- Corrected the join column here
