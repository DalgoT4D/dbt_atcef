{{ config(
  materialized='table',
  tags=["final","final_niti_2022"]
) }}


    SELECT 
        fc.work_order_name,
        MAX(fc.date_time) as date_time,
        fc.state, 
        fc.district,
        fc.taluka,
        fc.dam,
        fc.ngo_name,
        fc.village,
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
        
    FROM {{ref('farmer_calc_silt_niti_22')}} AS fc
    INNER JOIN {{ref('farmer_endline_niti_2022')}} AS fe
    on fc.farmer_id = fe.farmer_id
    WHERE total_silt_carted::text != 'NaN'
    GROUP BY
        fc.state, fc.district, fc.taluka, fc.dam, fc.village, fc.work_order_name, fc.ngo_name


