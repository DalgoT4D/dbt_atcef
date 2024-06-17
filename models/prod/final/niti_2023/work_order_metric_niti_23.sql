{{ config(
  materialized='table'
) }}


    SELECT 
        work_order_name,
        MAX(date_time) as date_time,
        state, 
        district,
        taluka,
        dam,
        village,
        MAX(silt_target) as silt_target,
        SUM(total_silt_carted) as silt_achieved
    FROM {{ref('farmer_silt_calc_niti_2023')}}
    WHERE total_silt_carted::text != 'NaN'
    GROUP BY
        state, district, taluka, dam, village, work_order_name


