{{ config(
  materialized='table',
  tags=["final","final_gramin_niti", "gramin_niti", "gramin_25"]
) }}

WITH fc_agg AS (
    SELECT
        farmer_id,
        work_order_name,
        state,
        district,
        taluka,
        dam,
        village,
        ngo_name,
        MAX(date_time) AS date_time,
        -- Aggregating at the farmer level
        SUM(total_silt_carted) AS total_silt_carted
    FROM {{ ref('farmer_calc_silt_gramin_25') }}
    WHERE total_silt_carted::text != 'NaN'
    GROUP BY
        farmer_id,
        work_order_name,
        state,
        district,
        taluka,
        dam,
        village,
        ngo_name
),

fe_agg AS (
    SELECT
        farmer_id,
        endline_status,
        SUM(
            total_farm_area_silt_is_spread_on
        ) AS total_farm_area_silt_is_spread_on,
        MAX(type_of_land_silt_is_spread_on) AS type_of_land_silt_is_spread_on
    FROM {{ ref('farmer_endline_gramin_25') }}
    WHERE endline_status = 'Endline Done'
    GROUP BY farmer_id, endline_status
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
    fe_agg.endline_status,
    fe_agg.type_of_land_silt_is_spread_on,
    SUM(fc.total_silt_carted) AS silt_achieved_by_endline_farmers,
    SUM(
        fe_agg.total_farm_area_silt_is_spread_on
    ) AS total_farm_area_silt_is_spread_on
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
    fc.date_time,
    fe_agg.endline_status
