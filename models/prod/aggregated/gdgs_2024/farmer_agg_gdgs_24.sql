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
from {{ref('farmer_calc_silt_gdgs_24')}} 
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
    COUNT(CASE WHEN category_of_farmer = 'Marginal (0-2.49 acres)' THEN 1 END) AS "vulnerable_marginal",
    COUNT(CASE WHEN category_of_farmer = 'Small (2.5-4.99 acres)' THEN 1 END) AS "vulnerable_small",
    COUNT(CASE WHEN category_of_farmer = 'Semi-medium (5 to 9.99 acre)' THEN 1 END) AS "semi_medium",
    COUNT(CASE WHEN category_of_farmer = 'Medium (10-24.99 acres)' THEN 1 END) AS "medium",
    COUNT(CASE WHEN category_of_farmer = 'Large (above 25 acres)' THEN 1 END) AS "large",
    COUNT(CASE WHEN category_of_farmer = 'Widow' THEN 1 END) AS "widow",
    COUNT(CASE WHEN category_of_farmer = 'Disabled' THEN 1 END) AS "disabled",
    COUNT(CASE WHEN category_of_farmer = 'Family of farmer who committed suicide' THEN 1 END) AS "family_of_farmer_who_committed_suicide"
FROM
    cte
GROUP BY
    date_time,
    state,
    district,
    taluka,
    village,
    dam
