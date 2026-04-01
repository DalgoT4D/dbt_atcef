-- Combines approved farmer silt progress for GDGS 2025 using only analytics
-- lineage sources. GP fields are retained as zero for schema alignment.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "ss_gdgs_2025"]
) }}

WITH approved_work_orders AS (
    SELECT
        w.subject_id AS work_order_id,
        w.state,
        w.district,
        w.taluka,
        w.village,
        w.dam,
        w.workorder_first_name AS work_order_name,
        w.stakeholder_responsible AS ngo_name
    FROM {{ ref('work_order_regn_gdgs_25') }} AS w
    WHERE w.approval_status = 'Approved'
),

farmer_silt_by_wo AS (
    SELECT
        d.work_order_id,
        SUM(COALESCE(d.silt_carted, 0)) AS farmer_silt_achieved,
        MAX(CAST(d.encounter_date_time AS TIMESTAMP)) AS last_farmer_update,
        SUM(COALESCE(fe.area_silt_spread, 0)) AS total_farm_area_with_silt
    FROM {{ ref('daily_farmer_linelist_gdgs_25') }} AS d
    LEFT JOIN (
        SELECT DISTINCT
            farmer_beneficiary_id,
            COALESCE(area_silt_spread, 0) AS area_silt_spread
        FROM {{ ref('farmer_endline_linelist_gdgs_25') }}
    ) AS fe
        ON d.farmer_id = fe.farmer_beneficiary_id
    GROUP BY d.work_order_id
),

farmer_silt AS (
    SELECT
        w.work_order_id,
        w.state,
        w.district,
        w.taluka,
        w.village,
        w.dam,
        w.work_order_name,
        w.ngo_name,
        SUM(f.farmer_silt_achieved) AS farmer_silt_achieved,
        SUM(COALESCE(f.total_farm_area_with_silt, 0)) AS total_farm_area_with_silt,
        MAX(f.last_farmer_update) AS last_farmer_update
    FROM approved_work_orders AS w
    LEFT JOIN farmer_silt_by_wo AS f
        ON f.work_order_id = w.work_order_id
    GROUP BY
        w.work_order_id,
        w.state,
        w.district,
        w.taluka,
        w.village,
        w.dam,
        w.work_order_name,
        w.ngo_name
)

SELECT
    f.state,
    f.district,
    f.taluka,
    f.village,
    f.dam,
    f.work_order_name,
    f.ngo_name,
    SUM(COALESCE(f.farmer_silt_achieved, 0)) AS farmer_silt_achieved,
    SUM(COALESCE(f.total_farm_area_with_silt, 0)) AS total_farm_area_with_silt,
    CAST(0 AS NUMERIC) AS gp_silt_achieved,
    SUM(COALESCE(f.farmer_silt_achieved, 0)) AS total_silt_achieved,
    MAX(COALESCE(f.last_farmer_update, '1900-01-01'::timestamp)) AS last_updated
FROM farmer_silt AS f
GROUP BY
    f.work_order_name,
    f.state,
    f.district,
    f.taluka,
    f.village,
    f.dam,
    f.ngo_name
