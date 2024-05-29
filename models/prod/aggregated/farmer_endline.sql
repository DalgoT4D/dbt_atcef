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
    {{ ref('farmer_niti_count_22') }} 
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
    {{ ref('farmer_niti_count_23') }} )


SELECT
    max(date_time) AS date_time,
    state,
    district,
    taluka,
    village,
    dam AS waterbodies,
    SUM(CASE 
            WHEN mobile_verified = 'True' THEN 1 
            ELSE 0 
        END
    ) AS verified_farmers,
    SUM(CASE 
            WHEN mobile_verified = 'False' THEN 1 
            ELSE 0 
        END
    ) AS unverified_farmers,
    SUM(CASE 
            WHEN mobile_verified = 'True' THEN 1 
            ELSE 0 
        END
    ) + SUM(CASE 
            WHEN mobile_verified = 'False' THEN 1 
            ELSE 0 
        END
    ) AS total_farmers,
    SUM(CASE
            WHEN category_of_farmer = 'Small (2.5-4.99 acres)' THEN 1
            ELSE 0
        END
    ) AS vulnerable_small,
    SUM(CASE
            WHEN category_of_farmer = 'Marginal (0-2.49 acres)' THEN 1
            ELSE 0
        END
    ) AS vulnerable_marginal,
    SUM(CASE
            WHEN category_of_farmer = 'Semi-medium (5-9.55 acres)' THEN 1
            ELSE 0
        END
    ) AS semi_medium,
    SUM(CASE
            WHEN category_of_farmer = 'Medium (10-24.99 acres)' THEN 1
            ELSE 0
        END
    ) AS medium,
    SUM(CASE
            WHEN category_of_farmer = 'Large (above 25 acres)' THEN 1
            ELSE 0
        END
    ) AS large
FROM
    farmer_union
WHERE LOWER(dam) NOT LIKE '%voided%'
GROUP BY
    state,
    district,
    taluka,
    village,
    dam
