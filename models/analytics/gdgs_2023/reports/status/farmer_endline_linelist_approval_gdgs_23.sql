-- Keeps the latest farmer_endline_gdgs_23 per farmer, adds registration fields, 
-- and unfiltered approval status.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2023", "analytical_models", "reports_gdgs_2023", "gdgs_23_approval_status"]
) }}


WITH farmer_latest_records AS (
    SELECT
        t1.*, -- Select all columns from the main table
        ROW_NUMBER() OVER (
            PARTITION BY t1.endline_farmer_sub_id
            ORDER BY
                t1.encounter_date_time::timestamp DESC
        ) AS rn
    FROM {{ ref('farmer_endline_analytics_gdgs_23') }} AS t1 
    WHERE t1.voided != TRUE
)


SELECT
  fr.farmer_name,
  fe.endline_farmer_sub_id as farmer_beneficiary_id,
  fe.encounter_date_time,
  fr.state,
  fr.district,
  fr.taluka,
  fr.village,
  fr.dam,
  fr.gp,
  fr.stakeholder_responsible,
  fe.total_silt_excavated,
  fe.area_silt_spread,
  fe.type_of_land_silt_is_spread_on,
  fe.major_crops_grown,
  fe.major_to_be_grown,
  fe.farmer_subsidy,
  -- farmers' village is to be added, but it isnt the same as other village?
  asn.approval_status

from farmer_latest_records as fe
LEFT JOIN {{ ref('farmer_regn_gdgs_23') }} AS fr
    ON fe.endline_farmer_sub_id = fr.subject_id

LEFT JOIN {{ ref('approval_status_gdgs_23') }} AS asn
    ON fe.eid = asn.entity_id

WHERE asn.approval_status IS NOT NULL
AND fr.subject_id IS NOT NULL



