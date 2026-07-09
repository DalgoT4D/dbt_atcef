-- Silt-per-acre summary for GraminPA 2026 using analytics lineage only.
{{ config(
  materialized='table',
  tags=["ss_2026", "ss_graminpa_2026"]
) }}

WITH farmer_daily_by_workorder AS (
    SELECT
        d.work_order_id,
        d.workorder_name AS work_order_name,
        d.state,
        d.district,
        d.taluka,
        d.dam,
        d.ngo_name,
        d.village,
        d.farmer_id,
        MAX(CAST(d.encounter_date_time AS DATE)) AS date_time,
        SUM(COALESCE(d.silt_carted, 0)) AS total_silt_carted
    FROM {{ ref('daily_farmer_linelist_graminpa_26') }} AS d
    WHERE d.silt_carted::text != 'NaN'
    GROUP BY
        d.work_order_id,
        d.workorder_name,
        d.state,
        d.district,
        d.taluka,
        d.dam,
        d.ngo_name,
        d.village,
        d.farmer_id
),

farmer_endline AS (
    SELECT
        fe.farmer_beneficiary_id AS farmer_id,
        COALESCE(fe.area_silt_spread, 0) AS total_farm_area_silt_is_spread_on
    FROM {{ ref('farmer_endline_linelist_graminpa_26') }} AS fe
)

SELECT
    fd.work_order_name,
    fd.state,
    fd.district,
    fd.taluka,
    fd.dam,
    fd.ngo_name,
    fd.village,
    'Endline Done' AS endline_status,
    MAX(fd.date_time) AS date_time,
    SUM(fd.total_silt_carted) AS silt_achieved_by_endline_farmers,
    SUM(fe.total_farm_area_silt_is_spread_on) AS total_farm_area_silt_is_spread_on,
    CASE
        WHEN SUM(fe.total_farm_area_silt_is_spread_on) > 0 THEN ROUND(SUM(fd.total_silt_carted) / NULLIF(SUM(fe.total_farm_area_silt_is_spread_on), 0), 2)
    END AS silt_per_acre,
    CASE
        WHEN
            SUM(fe.total_farm_area_silt_is_spread_on) > 0
            AND (SUM(fd.total_silt_carted) / NULLIF(SUM(fe.total_farm_area_silt_is_spread_on), 0)) >= 420
            THEN 'Above Benchmark'
        ELSE 'Below Benchmark'
    END AS silt_per_acre_benchmark_classification
FROM farmer_daily_by_workorder AS fd
INNER JOIN farmer_endline AS fe
    ON fd.farmer_id = fe.farmer_id
GROUP BY
    fd.work_order_name,
    fd.state,
    fd.district,
    fd.taluka,
    fd.dam,
    fd.ngo_name,
    fd.village
