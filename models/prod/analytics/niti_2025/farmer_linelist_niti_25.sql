{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models"]
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
  s.farmer_name,
  s.state,
  s.district,
  s.taluka,
  s.village,
  s.dam,
  s.mobile_number,
  s.land_holding,
  s.farmer_category,
  s.approval_status,

  fe.area_silt_spread,

  w.total_trolleys_carted,
  w.total_hyvas_carted,
  w.total_silt_carted
  
    FROM farmer_totals AS w
    LEFT JOIN {{ ref('farmer_regn_niti_25') }} AS s
      ON w.farmer_beneficiary_id = s.subject_id

    LEFT JOIN {{ ref('farmer_endline_niti_25') }} AS fe
    ON w.farmer_beneficiary_id = fe.endline_farmer_sub_id

    WHERE approval_status = 'Approved'

