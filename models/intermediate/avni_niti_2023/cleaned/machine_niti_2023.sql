{{ config(
  materialized='table'
) }}


with mycte as (SELECT
        "ID" AS machine_id,
        INITCAP(TRIM(COALESCE(observations->>'First name'))) AS machine_name,  
        observations->>'Type of Machine' AS type_of_machine,
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
        "Voided" as machine_voided
 FROM 
        {{ source('source_atecf_surveys', 'subjects_2023') }}
 Where "Subject_type" = 'Excavating Machine' and "Voided" = 'False'), 

 approval_machines AS (
SELECT d.*, a.approval_status as farmer_approval_status
FROM mycte d
JOIN {{ ref('approval_statuses_niti_2023') }} a ON d.machine_id = a.entity_id
WHERE a.entity_type = 'Subject' 
and a.approval_status = 'Approved'
)

{{ dbt_utils.deduplicate(
      relation='approval_machines',
      partition_by='machine_id',
      order_by='machine_id desc'
)}}