{{ config(
  materialized='table'
) }}


with cte as (SELECT DISTINCT on (first_name, mobile_number, uid)
  uid,
  date_time,
  first_name, 
  mobile_number,
  state,
  district,
  taluka,
  village, 
  dam, 
  trim(category_of_farmer) as category_of_farmer, 
  mobile_verified, 
  encounter_type
FROM 
  {{ ref('work_order_gdgs_2023') }}
)
  
SELECT
    state,
    district,
    taluka,
    village,
    dam AS waterbodies,
    SUM( CASE 
            WHEN mobile_verified = 'True' THEN 1 
            else 0 
         END
        ) AS verified_farmers,
    SUM( CASE 
            WHEN mobile_verified = 'False' THEN 1 
            else 0 
         END
        ) AS unverified_farmers,
    SUM(CASE 
            WHEN mobile_verified = 'True' OR mobile_verified = 'False' THEN 1 
            ELSE 0 
        END
    ) AS total,
    SUM(
        CASE
            WHEN category_of_farmer = 'Small (2.5-4.99 Acres)' THEN 1
            ELSE 0
        END
    ) AS vulnerable_small,
    SUM(
        CASE
            WHEN category_of_farmer = 'Marginal (0-2.49 Acres)' THEN 1
            ELSE 0
        END
    ) AS vulnerable_marginal,
    SUM(
        CASE
            WHEN category_of_farmer = 'Semi-Medium (5 To 9.99 Acre)' THEN 1
            ELSE 0
        END
    ) AS semi_medium,
    SUM(
        CASE
            WHEN category_of_farmer = 'Medium (10-24.99 Acres)' THEN 1
            ELSE 0
        END
    ) AS medium,
    SUM(
        CASE
            WHEN category_of_farmer = 'Large (Above 25 Acres)' THEN 1
            ELSE 0
        END
    ) AS large,
     SUM(
        CASE
            WHEN category_of_farmer = 'Widow' THEN 1
            ELSE 0
        END
    ) AS widow,
      SUM(
        CASE
            WHEN category_of_farmer = 'Disabled' THEN 1
            ELSE 0
        END
    ) AS Disabled,
      SUM(
        CASE
            WHEN category_of_farmer = 'Family Of Farmer Who Committed Suicide' THEN 1
            ELSE 0
        END
    ) AS family_of_farmer_who_committed_suicide
    
FROM
    cte
GROUP BY
    state,
    district,
    taluka,
    village,
    dam
