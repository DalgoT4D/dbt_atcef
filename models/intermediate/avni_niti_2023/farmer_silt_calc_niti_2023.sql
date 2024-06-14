{{ config(
  materialized='table'
) }}

SELECT 
    e.eid,
    s.farmer_id,
    w.work_order_id,
    s.state,
    s.village,
    s.district,
    s.taluka,
    s.dam,
    s.farmer_name,
    s.mobile_number,
    s.mobile_verified,
    s.category_of_farmer,
    w.work_order_name,
    w.silt_target,
    e.total_silt_carted
FROM {{ ref('encounter_2023') }} AS e
LEFT JOIN {{ ref('farmer_niti_2023') }} AS s 
    ON e.farmer_sub_id = s.farmer_id
LEFT JOIN {{ ref('work_order_niti_2023') }} AS w 
    ON e.subject_id = w.work_order_id
WHERE w.work_order_voided != TRUE and s.farmer_voided != TRUE



        
