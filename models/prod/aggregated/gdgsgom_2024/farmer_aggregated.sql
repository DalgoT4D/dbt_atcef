{{ config(
  materialized='table'
) }}


WITH cte AS (
    SELECT
        uid,
        date_time,
        first_name, 
        mobile_number,
        state,
        district,
        taluka,
        village, 
        dam, 
        TRIM(category_of_farmer) AS category_of_farmer, 
        mobile_verified
    FROM 
        {{ ref('farmer_gdgs_count_24') }}
)
  
SELECT
    state,
    district,
    taluka,
    village,
    dam AS waterbodies,
    SUM(CASE 
            WHEN mobile_verified = 'True' THEN 1 
            ELSE 0 
        END) AS verified_farmers,
    SUM(CASE 
            WHEN mobile_verified = 'False' THEN 1 
            ELSE 0 
        END) AS unverified_farmers,
    COUNT(*) AS total,
    SUM(CASE 
            WHEN category_of_farmer = 'Small (2.5-4.99 acres)' THEN 1
            ELSE 0
        END) AS vulnerable_small,
    SUM(CASE 
            WHEN category_of_farmer = 'Marginal (0-2.49 acres)' THEN 1
            ELSE 0
        END) AS vulnerable_marginal,
    SUM(CASE 
            WHEN category_of_farmer = 'Semi-medium (5 to 9.99 acre)' THEN 1
            ELSE 0
        END) AS semi_medium,
    SUM(CASE 
            WHEN category_of_farmer = 'Medium (10-24.99 acres)' THEN 1
            ELSE 0
        END) AS medium,
    SUM(CASE 
            WHEN category_of_farmer = 'Large (above 25 acres)' THEN 1
            ELSE 0
        END) AS large,
    SUM(CASE 
            WHEN category_of_farmer = 'Widow' THEN 1
            ELSE 0
        END) AS widow,
    SUM(CASE 
            WHEN category_of_farmer = 'Disabled' THEN 1
            ELSE 0
        END) AS Disabled,
    SUM(CASE 
            WHEN category_of_farmer = 'Family of farmer who committed suicide' THEN 1
            ELSE 0
        END) AS family_of_farmer_who_committed_suicide
FROM
    cte
GROUP BY
    state,
    district,
    taluka,
    village,
    dam
