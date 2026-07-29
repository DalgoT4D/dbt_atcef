{{ config(
  materialized='table',
  tags=["final", "final_gramin_niti", "gramin_niti", "gramin_25"]
) }}

WITH farmer_silt AS (
    SELECT
        fc.work_order_id,
        fc.work_order_name,
        fc.state,
        fc.district,
        fc.taluka,
        fc.dam,
        fc.ngo_name,
        fc.village,
        MAX(fc.date_time) AS date_time,
        MAX(fc.silt_target) AS silt_target,
        SUM(fc.total_silt_carted) AS farmer_silt_achieved,
        SUM(fe.total_farm_area_silt_is_spread_on)
            AS total_farm_area_silt_is_spread_on
    FROM {{ ref('farmer_calc_silt_gramin_25') }} AS fc
    INNER JOIN {{ ref('farmer_endline_gramin_25') }} AS fe
        ON fc.farmer_id = fe.farmer_id
    WHERE fc.total_silt_carted::text != 'NaN'
    GROUP BY
        fc.work_order_id,
        fc.state,
        fc.district,
        fc.taluka,
        fc.dam,
        fc.village,
        fc.work_order_name,
        fc.ngo_name
),

gp_silt AS (
    SELECT
        work_order_id,
        work_order_name,
        state,
        district,
        taluka,
        dam,
        ngo_name,
        village,
        MAX(date_time) AS date_time,
        MAX(silt_target) AS silt_target,
        SUM(total_silt_excavated_by_gp_for_non_farm_purpose)
            AS gp_silt_achieved
    FROM {{ ref('gram_panchayat_gramin_25') }}
    GROUP BY
        work_order_id,
        state,
        district,
        taluka,
        dam,
        village,
        work_order_name,
        ngo_name
)

SELECT
    COALESCE(f.work_order_name, g.work_order_name) AS work_order_name,
    COALESCE(f.state, g.state) AS state,
    COALESCE(f.district, g.district) AS district,
    COALESCE(f.taluka, g.taluka) AS taluka,
    COALESCE(f.dam, g.dam) AS dam,
    COALESCE(f.ngo_name, g.ngo_name) AS ngo_name,
    COALESCE(f.village, g.village) AS village,
    GREATEST(f.date_time, g.date_time) AS date_time,
    COALESCE(f.silt_target, g.silt_target) AS silt_target,
    COALESCE(f.farmer_silt_achieved, 0) + COALESCE(g.gp_silt_achieved, 0)
        AS silt_achieved,
    COALESCE(f.total_farm_area_silt_is_spread_on, 0)
        AS total_farm_area_silt_is_spread_on
FROM farmer_silt AS f
FULL OUTER JOIN gp_silt AS g
    ON f.work_order_id = g.work_order_id
