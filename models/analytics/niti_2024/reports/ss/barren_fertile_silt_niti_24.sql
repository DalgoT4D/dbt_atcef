{{ config(
  materialized='table',
  tags=["analytics","analytical_models", "reports_niti_2024"]
) }}

WITH fc_agg AS (
    SELECT
    d.farmer_name,
    d.farmer_id,
    d.state,
    d.district,
    d.taluka,
    d.village, 
    d.dam,
    d.gp,
    d.stakeholder_responsible as ngo_name,
    SUM(d.silt_carted) AS silt_carted,
    SUM(d.amt_silt_used_non_farm_purpose) AS amt_silt_used_non_farm_purpose,
    COUNT(d.*) AS total_daily_recordings,
    fc.farmer_category,
    CASE WHEN fc.mobile_verified_status = 'true' THEN 1 ELSE 0 END AS mobile_verified_status,
    d.updated_workorder_name as work_order_name,
    fc.registration_date as date_time


    FROM {{ ref('daily_farmer_linelist_niti_24') }} AS d
    LEFT JOIN {{ ref('farmer_regn_niti_24') }} AS fc
        ON d.farmer_id = fc.subject_id
    GROUP BY
    d.farmer_name,
    d.farmer_id,
    d.state,
    d.district,
    d.taluka, 
    d.village,
    d.dam,
    d.gp,
    d.stakeholder_responsible,
    fc.farmer_category,
    fc.mobile_verified_status,
    d.updated_workorder_name,
    fc.registration_date
),


fe_agg AS (
    SELECT
        farmer_beneficiary_id AS farmer_id,
        SUM(
            area_silt_spread
        ) AS total_farm_area_silt_is_spread_on,
        MAX(type_of_land_silt_is_spread_on) AS type_of_land_silt_is_spread_on
    FROM {{ ref('farmer_endline_linelist_approval_niti_24') }}
    GROUP BY farmer_id
)

SELECT
    fc.work_order_name,
    fc.date_time,
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.ngo_name,
    -- fe_agg.endline_status,
    fe_agg.type_of_land_silt_is_spread_on,
    SUM(fc.silt_carted) AS silt_achieved_by_endline_farmers,
    SUM(fe_agg.total_farm_area_silt_is_spread_on) AS total_farm_area_silt_is_spread_on
FROM fc_agg AS fc
INNER JOIN fe_agg
    ON fc.farmer_id = fe_agg.farmer_id
GROUP BY
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.work_order_name,
    fc.ngo_name,
    fe_agg.type_of_land_silt_is_spread_on,
    fc.date_time
    -- fe_agg.endline_status
