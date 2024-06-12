{{ config(
  materialized='table'
) }}


    SELECT
        "ID" AS work_order_id,
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
        CAST(COALESCE(observations ->> 'Silt to be excavated as per plan', '0') AS NUMERIC) AS silt_to_be_excavated_as_per_plan,
        "Voided" as subject_voided
    FROM 
        {{ source('source_gramin', 'subjects_gramin') }}
    Where "Subject_type" = 'Work Order'
