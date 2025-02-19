{{ config(
  materialized='table',
  tags=["aggregated","aggregated_gdgs_2024", "gdgs_2024", "gdgs"]
) }}

SELECT 
    e.eid,
    s.farmer_id,
    w.work_order_id,
    w.state,
    w.village,
    e.machine_sub_id,
    w.district,
    w.taluka,
    w.dam,
    w.ngo_name,
    s.farmer_name,
    s.mobile_number,
    s.mobile_verified,
    s.category_of_farmer,
    w.work_order_name,
    w.silt_target,
    e.total_silt_carted,
    e.date_time,
    s.farmer_voided
FROM {{ ref('encounters_2024') }} AS e
INNER JOIN {{ ref('farmer_gdgs_2024') }} AS s 
    ON e.farmer_sub_id = s.farmer_id
INNER JOIN {{ ref('work_order_gdgs_2024') }} AS w 
    ON e.subject_id = w.work_order_id



        
