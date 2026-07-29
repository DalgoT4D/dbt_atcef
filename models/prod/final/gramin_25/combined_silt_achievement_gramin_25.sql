{{ config(
  materialized='table',
  tags=["final", "final_gramin_niti", "gramin_niti", "gramin_25"]
) }}

WITH farmer_silt AS (
    SELECT
        state,
        district,
        taluka,
        village,
        dam,
        work_order_name,
        ngo_name,
        SUM(silt_achieved) AS total_silt_achieved,
        SUM(total_farm_area_silt_is_spread_on) AS total_farm_area_with_silt,
        MAX(date_time) AS last_farmer_update
    FROM {{ ref('work_order_silt_calc_25') }}
    GROUP BY state, district, taluka, village, dam, work_order_name, ngo_name
),

gp_silt AS (
    SELECT
        state,
        district,
        taluka,
        village,
        dam,
        work_order_name,
        ngo_name,
        SUM(total_silt_excavated_by_gp_for_non_farm_purpose) AS gp_silt_achieved,
        MAX(date_time) AS last_gp_update
    FROM {{ ref('gram_panchayat_gramin_25') }}
    GROUP BY state, district, taluka, village, dam, work_order_name, ngo_name
)

SELECT
    COALESCE(f.state, g.state) AS state,
    COALESCE(f.district, g.district) AS district,
    COALESCE(f.taluka, g.taluka) AS taluka,
    COALESCE(f.village, g.village) AS village,
    COALESCE(f.dam, g.dam) AS dam,
    COALESCE(f.work_order_name, g.work_order_name) AS work_order_name,
    COALESCE(f.ngo_name, g.ngo_name) AS ngo_name,
    GREATEST(
        COALESCE(f.total_silt_achieved, 0) - COALESCE(g.gp_silt_achieved, 0),
        0
    ) AS farmer_silt_achieved,
    COALESCE(f.total_farm_area_with_silt, 0) AS total_farm_area_with_silt,
    COALESCE(g.gp_silt_achieved, 0) AS gp_silt_achieved,
    COALESCE(f.total_silt_achieved, g.gp_silt_achieved, 0)
        AS total_silt_achieved,
    GREATEST(
        COALESCE(f.last_farmer_update, '1900-01-01'::date),
        COALESCE(g.last_gp_update, '1900-01-01'::date)
    ) AS last_updated
FROM farmer_silt AS f
FULL OUTER JOIN gp_silt AS g
    ON f.state = g.state
    AND f.district = g.district
    AND f.taluka = g.taluka
    AND f.village = g.village
    AND f.dam = g.dam
    AND f.work_order_name = g.work_order_name
    AND f.ngo_name = g.ngo_name
