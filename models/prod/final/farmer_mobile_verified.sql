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
    state,
    district,
    taluka,
    village,
    dam,
	date_time,
    COUNT(CASE WHEN mobile_verified = TRUE THEN 1 ELSE NULL END) AS mobile_verified_count,
    COUNT(CASE WHEN mobile_verified = FALSE THEN 1 ELSE NULL END) AS mobile_unverified_count,
    COUNT(*) AS total -- Counts the total number of records (both verified and unverified)
FROM
    farmer_union
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
	date_time
