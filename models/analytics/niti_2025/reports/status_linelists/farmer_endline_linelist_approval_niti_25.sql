-- Keeps the latest farmer_endline_niti_25 per farmer, adds registration fields, 
-- and unfiltered approval status.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


WITH farmer_latest_records AS (
    SELECT
        t1.*, -- Select all columns from the main table
        ROW_NUMBER() OVER (
            PARTITION BY t1.endline_farmer_sub_id
            ORDER BY
                t1.encounter_date_time::timestamp DESC
        ) AS rn
    FROM {{ ref('farmer_endline_niti_25') }} AS t1 
    WHERE t1.voided != TRUE
),

farmer_with_status AS (
    SELECT
        flr.*,
        asn.approval_status
    FROM farmer_latest_records AS flr
    LEFT JOIN {{ ref('approval_status_niti_25') }} AS asn
        ON flr.eid = asn.entity_id
    WHERE flr.rn = 1 -- Filter to include only the latest record for each farmer
)

SELECT
  fr.farmer_name,
  fe.encounter_date_time,
  fr.state,
  fr.district,
  fr.taluka,
  fr.village,
  fr.dam,
  fe.total_silt_excavated,
  fe.area_silt_spread,
  fe.type_of_land_silt_is_spread_on,
  fe.major_crops_grown,
  fe.major_to_be_grown,
  -- farmers' village is to be added, but it isnt the same as other village?
  fe.approval_status

from farmer_with_status as fe
LEFT JOIN {{ ref('farmer_regn_niti_25') }} AS fr
    ON fe.endline_farmer_sub_id = fr.subject_id




