{{ config(
  materialized='table'
) }}

SELECT
        "ID" AS farmer_id,
        INITCAP(TRIM(COALESCE(observations->>'First name'))) AS work_order_name,  
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
        observations ->> 'Category of farmer' AS category_of_farmer,
        observations -> 'Mobile Number' ->> 'phoneNumber' AS mobile_number,
        (observations -> 'Mobile Number' ->> 'verified')::boolean AS mobile_verified,
        "Voided" as subject_voided
 FROM 
        {{ source('source_gramin', 'subjects_gramin') }}
 Where "Subject_type" = 'Farmer'
