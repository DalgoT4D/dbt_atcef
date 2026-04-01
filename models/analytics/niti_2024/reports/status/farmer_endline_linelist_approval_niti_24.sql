-- Keeps the latest farmer_endline_niti_24 per farmer, adds registration fields, 
-- and unfiltered approval status.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2024", "analytical_models", "reports_niti_2024"]
) }}


WITH farmer_latest_records AS (
    SELECT
        t1.*, -- Select all columns from the main table
        ROW_NUMBER() OVER (
            PARTITION BY t1.endline_farmer_sub_id
            ORDER BY
                t1.encounter_date_time::timestamp DESC
        ) AS rn
    FROM {{ ref('farmer_endline_niti_24') }} AS t1 
    WHERE t1.voided != TRUE
),

farmer_with_status AS (
    SELECT
        flr.*,
        asn.approval_status
    FROM farmer_latest_records AS flr
    LEFT JOIN {{ ref('approval_status_niti_24') }} AS asn
        ON flr.eid = asn.entity_id
    WHERE flr.rn = 1 -- Filter to include only the latest record for each farmer
)

SELECT
  fr.farmer_name,
  fr.subject_id as farmer_beneficiary_id,
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
  fe.distance_from_waterbody, -- new
  -- farmers' village is to be added, but it isnt the same as other village?
  fe.approval_status

from farmer_with_status as fe
LEFT JOIN {{ ref('farmer_regn_niti_24') }} AS fr
    ON fe.endline_farmer_sub_id = fr.subject_id
WHERE fr.subject_id IS NOT NULL



