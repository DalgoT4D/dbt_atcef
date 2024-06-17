{{ config(
  materialized='table'
) }}

with cte as (select distinct farmer_id, 
       max(date_time) as date_time,
       state,
       district, 
       village,
       taluka,
       dam,
       farmer_name,
       mobile_number,
       mobile_verified,
       category_of_farmer
from {{ref('farmer_calc_silt_niti_22')}} 
group by 
       farmer_id,
       state,
       district, 
       village,
       taluka,
       dam,
       farmer_name,
       mobile_number,
       mobile_verified,
       category_of_farmer)


SELECT
    distinct dam,
    date_time,
    state,
    district,
    taluka,
    village,
    SUM(CASE 
            WHEN mobile_verified = 'True' THEN 1 
            ELSE 0 
        END) AS verified_farmers,
    SUM(CASE 
            WHEN mobile_verified = 'False' THEN 1 
            ELSE 0 
        END) AS unverified_farmers,
    COUNT(*) AS total,
    COUNT(CASE WHEN category_of_farmer = 'farmer_niti_2022' THEN 1 END) AS "farmer_niti_2022"
FROM
    cte
GROUP BY
    date_time,
    state,
    district,
    taluka,
    village,
    dam
