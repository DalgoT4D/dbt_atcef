{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2025", "analytical_models", "reports_niti_2025", "ss_niti_2025"]
) }}


-- -- cte 1: get the "farmer endline table"

WITH farmer_endline AS (
SELECT
    m.subject_id AS farmer_id,
    m.farmer_name,
    m.state,
    m.taluka,
    m.village,
    m.dam,
    m.district,
    m.stakeholder_responsible as ngo_name,
    -- w.ngo_name,
    e.type_of_land_silt_is_spread_on,
    e.area_silt_spread,
    e.distance_from_waterbody,
    CASE
        WHEN e.farmer_beneficiary_id IS NOT NULL THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('farmer_regn_niti_25') }} AS m
LEFT JOIN {{ ref('farmer_endline_linelist_niti_25') }} AS e
    ON m.subject_id = e.farmer_beneficiary_id
WHERE m.approval_status = 'Approved'
)

SELECT
    fc.updated_workorder_name as work_order_name,
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.stakeholder_responsible as ngo_name,
    fc.village,
    MAX(fc.encounter_date_time) AS date_time,
    MAX(fc.silt_target) AS silt_target,
    SUM(fc.silt_carted) AS silt_achieved,

    SUM(
        fe.area_silt_spread
    ) AS total_farm_area_silt_is_spread_on

FROM {{ ref('daily_farmer_linelist_niti_25') }} AS fc
INNER JOIN farmer_endline AS fe
    ON fc.farmer_id = fe.farmer_id
WHERE fc.silt_carted::text != 'NaN'
GROUP BY
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.updated_workorder_name,
    fc.stakeholder_responsible