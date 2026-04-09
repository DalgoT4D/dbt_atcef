-- Summarizes silt achieved by farmers with approved endlines, split by the
-- land type recorded in the farmer endline, using analytics lineage only.
{{ config(
  materialized='table',
  tags=["ss_2025", "ss_niti_2025"]
) }}

WITH fc_agg AS (
    SELECT
        d.farmer_id,
        d.workorder_name AS work_order_name,
        d.state,
        d.district,
        d.taluka,
        d.dam,
        d.village,
        d.stakeholder_responsible AS ngo_name,
        MAX(CAST(d.encounter_date_time AS DATE)) AS date_time,
        SUM(COALESCE(d.silt_carted, 0)) AS total_silt_carted
    FROM {{ ref('daily_farmer_linelist_niti_25') }} AS d
    WHERE d.silt_carted::text != 'NaN'
    GROUP BY
        d.farmer_id,
        d.workorder_name,
        d.state,
        d.district,
        d.taluka,
        d.dam,
        d.village,
        d.stakeholder_responsible
),

fe_agg AS (
    SELECT
        fe.farmer_beneficiary_id AS farmer_id,
        'Endline Done' AS endline_status,
        SUM(COALESCE(fe.area_silt_spread, 0)) AS total_farm_area_silt_is_spread_on,
        MAX(fe.type_of_land_silt_is_spread_on) AS type_of_land_silt_is_spread_on
    FROM {{ ref('farmer_endline_linelist_niti_25') }} AS fe
    GROUP BY fe.farmer_beneficiary_id
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
    fe.endline_status,
    fe.type_of_land_silt_is_spread_on,
    SUM(fc.total_silt_carted) AS silt_achieved_by_endline_farmers,
    SUM(fe.total_farm_area_silt_is_spread_on) AS total_farm_area_silt_is_spread_on
FROM fc_agg AS fc
INNER JOIN fe_agg AS fe
    ON fc.farmer_id = fe.farmer_id
GROUP BY
    fc.work_order_name,
    fc.date_time,
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.ngo_name,
    fe.endline_status,
    fe.type_of_land_silt_is_spread_on
