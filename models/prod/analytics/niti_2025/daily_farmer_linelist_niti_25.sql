{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

SELECT
    w.work_order_id,
    w.work_order_name,
    s.farmer_name,
    w.state,
    w.district,
    w.village,
    w.taluka,
    w.dam,
    w.ngo_name,
    e.purpose_of_carting_silt, 
    (e.total_silt_carted::numeric) AS silt_carted,
    (e.number_of_hyvasdumper_carted::numeric) AS hyvasdumper_carted,
    (e.number_of_trolleys_carted::numeric) AS trolleys_carted,
    e.silt_used_for_non_farm_purpose,
    e.amt_used_for_non_farm_purpose
 

FROM {{ ref('encounters_niti_2025') }} AS e
LEFT JOIN {{ ref('farmer_niti_2025') }} AS s
    ON e.farmer_sub_id = s.farmer_id
LEFT JOIN {{ ref('work_order_niti_2025') }} AS w
    ON e.subject_id = w.work_order_id

WHERE w.work_order_voided != TRUE AND s.farmer_voided != TRUE 
AND e.encounter_type = 'Work order daily Recording - Farmer'

