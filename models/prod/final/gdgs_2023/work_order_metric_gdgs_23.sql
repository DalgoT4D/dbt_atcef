{{ config(
  materialized='table',
  tags=["final","final_gdgs_2023"]
) }}


    SELECT 
        fc.work_order_name,
        MAX(fc.date_time) as date_time,
        fc.state, 
        fc.district,
        fc.taluka,
        fc.dam,
        fc.village,
        fc.ngo_name,
        MAX(fc.silt_target) as silt_target,
        SUM(fc.total_silt_carted) as silt_achieved,
        SUM(fe.total_farm_area_silt_is_spread_on) as total_farm_area_silt_is_spread_on,
        CASE 
          WHEN SUM(fe.total_farm_area_silt_is_spread_on) > 0 
          THEN ROUND(SUM(fc.total_silt_carted) / NULLIF(SUM(fe.total_farm_area_silt_is_spread_on), 0), 2) 
          ELSE NULL 
        END AS silt_per_acre,
        CASE 
            WHEN SUM(fe.total_farm_area_silt_is_spread_on) > 0 
            AND (SUM(fc.total_silt_carted) / NULLIF(SUM(fe.total_farm_area_silt_is_spread_on), 0)) >= 420 
            THEN 'Above Benchmark' 
            ELSE 'Below Benchmark' 
        END AS silt_per_acre_benchmark_classification
    FROM {{ref('farmer_calc_silt_gdgs_23')}} AS fc
    INNER JOIN {{ref('farmer_endline_gdgs_2023')}} AS fe
    ON fe.farmer_id = fc.farmer_id

    WHERE total_silt_carted != 'NaN'
    GROUP BY
        fc.state, fc.district, fc.taluka, fc.dam, fc.village, fc.work_order_name, fc.ngo_name


