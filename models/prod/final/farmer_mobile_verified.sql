{{ config(
  materialized='table'
) }}

WITH DistinctRecords AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number,
		date_time,
        mobile_verified,
        encounter_type,
        state,
        district,
        taluka,
        village,
        dam,
		ngo_name
    FROM
        {{ ref('work_order_union') }}
    WHERE
       encounter_type = 'Farmer Endline'
    ORDER BY mobile_number -- Ensure the query is ordered by mobile_number for DISTINCT ON to work properly
)
SELECT
    state,
    district,
    taluka,
    village,
    dam,
	date_time,
	ngo_name,
    COUNT(CASE WHEN mobile_verified = TRUE THEN 1 ELSE NULL END) AS mobile_verified_count,
    COUNT(CASE WHEN mobile_verified = FALSE THEN 1 ELSE NULL END) AS mobile_unverified_count,
    COUNT(*) AS total -- Counts the total number of records (both verified and unverified)
FROM
    DistinctRecords
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
	date_time,
	ngo_name
