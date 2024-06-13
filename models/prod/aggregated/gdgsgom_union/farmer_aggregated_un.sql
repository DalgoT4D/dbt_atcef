{{ config(
  materialized='table'
) }}


with farmer_union as (select uid, 
       date_time, 
       first_name,
       mobile_number, 
       mobile_verified,
       state, 
       district, 
       taluka, 
       village,
       dam, 
       category_of_farmer 
FROM
    {{ ref('farmer_gdgs_count_23') }} 
UNION ALL 
select uid, 
       date_time, 
       first_name,
       mobile_number, 
       mobile_verified,
       state, 
       district, 
       taluka, 
       village,
       dam, 
       category_of_farmer 
FROM
    {{ ref('farmer_gdgs_count_24') }} )
  
SELECT
    date_time,
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
            WHEN LOWER(category_of_farmer) LIKE LOWER('small (2.5-4.99 acres)') THEN 1
            ELSE 0
        END) AS vulnerable_small,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('marginal (0-2.49 acres)') THEN 1
            ELSE 0
        END) AS vulnerable_marginal,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('semi-medium (5 to 9.99 acre)') THEN 1
            ELSE 0
        END) AS semi_medium,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('medium (10-24.99 acres)') THEN 1
            ELSE 0
        END) AS medium,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('large (above 25 acres)') THEN 1
            ELSE 0
        END) AS large,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('widow') THEN 1
            ELSE 0
        END) AS widow,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('disabled') THEN 1
            ELSE 0
        END) AS disabled,
    SUM(CASE 
            WHEN LOWER(category_of_farmer) LIKE LOWER('family of farmer who committed suicide') THEN 1
            ELSE 0
        END) AS family_of_farmer_who_committed_suicide
FROM
    farmer_union
GROUP BY
    date_time,
    state,
    district,
    taluka,
    village,
    dam
