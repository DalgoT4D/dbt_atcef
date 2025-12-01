{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti"]
) }}

with farmer_totals as (

  SELECT
      w.farmer_work_order_sub_id as subject_id,
      w.farmer_beneficiary_id,
      SUM(COALESCE(w.trolleys_carted, 0)) AS total_trolleys_carted,
      SUM(COALESCE(w.hyvas_carted, 0)) AS total_hyvas_carted,
      SUM(COALESCE(w.silt_carted, 0)) AS total_silt_carted

  FROM {{ ref('work_order_farmer_niti_25') }} AS w
  WHERE w.farmer_beneficiary_id IS NOT NULL
  GROUP BY
      w.farmer_work_order_sub_id,
      w.farmer_beneficiary_id
)

SELECT
  s.farmer_first_name as farmer_name,
  s.state,
  s.district,
  s.taluka,
  s.gp_village as village,
  s.dam as dam,
  s.mobile_number,
  s.land_holding_acres as land_holding,
  s.farmer_category,
  
  fe.area_silt_spread,

  w.total_trolleys_carted,
  w.total_hyvas_carted,
  w.total_silt_carted,  
  a_farmer.approval_status AS farmer_approval_status,
  a_workorder.approval_status AS workorder_approval_status
  
    FROM farmer_totals AS w
    LEFT JOIN {{ ref('dim_subjects_farmer_niti_25') }} AS s
      ON w.farmer_beneficiary_id = s.subject_id

    LEFT JOIN {{ ref('approval_status_niti_25') }} AS a_farmer
    ON w.farmer_beneficiary_id = a_farmer.entity_id

    LEFT JOIN {{ ref('approval_status_niti_25') }} AS a_workorder
    ON w.subject_id = a_workorder.entity_id

    LEFT JOIN {{ ref('farmer_endline_niti_25') }} AS fe
    ON w.farmer_beneficiary_id = fe.endline_farmer_sub_id