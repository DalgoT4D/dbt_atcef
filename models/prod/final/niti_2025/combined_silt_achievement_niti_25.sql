{{ config(
  materialized='table',
  tags=["final","final_niti_2025", "niti_2025", "niti"]
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
        SUM(silt_achieved) as farmer_silt_achieved,
        SUM(total_farm_area_silt_is_spread_on) as total_farm_area_with_silt,
        MAX(date_time) as last_farmer_update
    FROM {{ ref('work_order_metric_niti_2025') }}
    GROUP BY
        state,
        district,
        taluka,
        village,
        dam,
        work_order_name,
        ngo_name
),

gp_silt AS (
    SELECT
        state,
        district,
        taluka,
        village,
        first_name as work_order_name,
        ngo_name,
        SUM(cast(total_silt_excavated_by_gp_for_non_farm_purpose as numeric)) as gp_silt_achieved,
        MAX(date_time) as last_gp_update
    FROM {{ ref('work_order_2025_niti') }}
    WHERE encounter_type = 'Gram Panchayat Endline'
        AND cast(total_silt_excavated_by_gp_for_non_farm_purpose as numeric) != 0
    GROUP BY
        state,
        district,
        taluka,
        village,
        first_name,
        ngo_name
)

SELECT
    COALESCE(f.state, g.state) as state,
    COALESCE(f.district, g.district) as district,
    COALESCE(f.taluka, g.taluka) as taluka,
    COALESCE(f.village, g.village) as village,
    f.dam as dam_name,
    COALESCE(f.work_order_name, g.work_order_name) as work_order_name,
    COALESCE(f.ngo_name, g.ngo_name) as ngo_name,
    COALESCE(f.farmer_silt_achieved, 0) as farmer_silt_achieved,
    COALESCE(f.total_farm_area_with_silt, 0) as total_farm_area_with_silt,
    COALESCE(g.gp_silt_achieved, 0) as gp_silt_achieved,
    (COALESCE(f.farmer_silt_achieved, 0) + COALESCE(g.gp_silt_achieved, 0)) as total_silt_achieved,
    GREATEST(
        COALESCE(f.last_farmer_update, '1900-01-01'::timestamp),
        COALESCE(g.last_gp_update, '1900-01-01'::timestamp)
    ) as last_updated
FROM farmer_silt f
FULL OUTER JOIN gp_silt g
    ON f.state = g.state
    AND f.district = g.district
    AND f.taluka = g.taluka
    AND f.village = g.village
    AND f.work_order_name = g.work_order_name
    AND f.ngo_name = g.ngo_name 