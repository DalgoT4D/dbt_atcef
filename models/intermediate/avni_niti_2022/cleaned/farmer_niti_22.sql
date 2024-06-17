{{ config(
  materialized='table'
) }}

with mycte as (SELECT
        "ID" AS farmer_id,
        INITCAP(TRIM(COALESCE(observations->>'First name'))) AS farmer_name,  
        INITCAP(COALESCE(location->>'Dam')) AS dam,  
        INITCAP(COALESCE(location->>'District')) AS district,  
        "Subject_type" as subject_type,
        CASE  -- Standardize state names
        WHEN LOWER(location->>'State') LIKE '%maharashtra%' THEN 'Maharashtra'
        WHEN LOWER(location->>'State') LIKE '%maharshatra%' THEN 'Maharashtra'
        ELSE INITCAP(COALESCE(location->>'State', ''))
        END AS state,
        INITCAP(COALESCE(location->>'Taluka')) AS taluka, 
        INITCAP(COALESCE(location->>'GP/Village')) AS village, 
        'farmer_niti_2022' AS category_of_farmer,
        observations -> 'Mobile Number' ->> 'phoneNumber' AS mobile_number,
        (observations -> 'Mobile Number' ->> 'verified')::boolean AS mobile_verified,
        "Voided" as farmer_voided
 FROM 
        {{ source('source_atecf_surveyss', 'subjects_2022') }}
 Where "Subject_type" = 'Farmer' and "Voided" = 'False' and NOT (LOWER(location->>'Dam') ~ 'voided') ), 

 approval_farmers AS (
SELECT d.*, a.approval_status as farmer_approval_status
FROM mycte d
JOIN {{ ref('approval_statuses_niti_2022') }} a ON d.farmer_id = a.entity_id
WHERE a.entity_type = 'Subject' 
and a.approval_status = 'Approved'
)

{{ dbt_utils.deduplicate(
      relation='approval_farmers',
      partition_by='farmer_id',
      order_by='farmer_id desc'
)}}