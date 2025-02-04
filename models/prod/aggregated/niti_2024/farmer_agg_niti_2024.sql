{{ config(
  materialized='table',
  tags=["aggregated","aggregated_niti_2024"]
) }}

with cte as (select distinct farmer_id, 
       max(date_time) as date_time,
       state,
       district, 
       village,
       taluka,
       dam,
       ngo_name,
       farmer_name,
       mobile_number,
       mobile_verified,
       category_of_farmer
from {{ref('farmer_calc_silt_niti_2024')}} 
group by 
       farmer_id,
       state,
       district, 
       village,
       taluka,
       dam,
       ngo_name,
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
    ngo_name,
    SUM(CASE 
            WHEN mobile_verified = 'True' THEN 1 
            ELSE 0 
        END) AS verified_farmers,
    SUM(CASE 
            WHEN mobile_verified = 'False' THEN 1 
            ELSE 0 
        END) AS unverified_farmers,
    COUNT(*) AS total,
    COUNT(CASE WHEN category_of_farmer = 'Marginal (0-2.49 acres)' THEN 1 END) AS "vulnerable_marginal",
    COUNT(CASE WHEN category_of_farmer = 'Small (2.5-4.99 acres)' THEN 1 END) AS "vulnerable_small",
    COUNT(CASE WHEN category_of_farmer = 'Semi-medium (5-9.99 acres)' THEN 1 END) AS "semi_medium",
    COUNT(CASE WHEN category_of_farmer = 'Medium (10-24.99 acres)' THEN 1 END) AS "medium",
    COUNT(CASE WHEN category_of_farmer = 'Large (25+ acres)' THEN 1 END) AS "large"
FROM
    cte
GROUP BY
    date_time,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name
