{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

with farmer_totals as (

  SELECT
      -- w.farmer_work_order_sub_id as subject_id,
      w.farmer_beneficiary_id,
      SUM(COALESCE(w.trolleys_carted, 0)) AS total_trolleys_carted,
      SUM(COALESCE(w.hyvas_carted, 0)) AS total_hyvas_carted,
      SUM(COALESCE(w.silt_carted, 0)) AS total_silt_carted

  FROM {{ ref('work_order_farmer_niti_25') }} AS w
  WHERE w.farmer_beneficiary_id IS NOT NULL
  GROUP BY
      -- w.farmer_work_order_sub_id,
      w.farmer_beneficiary_id
),


latest_endline AS (
    SELECT
        *,
        -- Rank records by submission date/time descending, partitioned by farmer ID
        ROW_NUMBER() OVER (
            PARTITION BY endline_farmer_sub_id
            -- *** REPLACE 'endline_submission_datetime' with the actual timestamp column ***
            ORDER BY encounter_date_time DESC
        ) AS rn
    FROM {{ ref('farmer_endline_niti_25') }}
    WHERE
        voided != TRUE -- Only consider non-voided records
)


SELECT
  -- w.farmer_beneficiary_id, -- added temp
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

    FROM {{ ref('farmer_regn_niti_25') }} AS s
    LEFT JOIN  farmer_totals AS w
    ON w.farmer_beneficiary_id = s.subject_id

    LEFT JOIN latest_endline AS fe
    ON s.subject_id = fe.endline_farmer_sub_id
    AND fe.rn = 1

