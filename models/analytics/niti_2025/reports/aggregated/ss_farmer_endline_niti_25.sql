-- Farmer endline status for NITI 2025 using approved registrations and
-- approved farmer endline linelist data only.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2025", "analytical_models", "reports_niti_2025", "ss_niti_2025"]
) }}

SELECT
    fr.subject_id AS farmer_id,
    fr.farmer_name,
    fr.state,
    fr.taluka,
    fr.village,
    fr.dam,
    fr.district,
    fr.stakeholder_responsible AS ngo_name,
    fe.type_of_land_silt_is_spread_on,
    fe.area_silt_spread AS total_farm_area_silt_is_spread_on,
    fe.distance_from_waterbody,
    CASE
        WHEN fe.farmer_beneficiary_id IS NOT NULL THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('farmer_regn_niti_25') }} AS fr
LEFT JOIN {{ ref('farmer_endline_linelist_niti_25') }} AS fe
    ON fr.subject_id = fe.farmer_beneficiary_id
WHERE fr.approval_status = 'Approved'
