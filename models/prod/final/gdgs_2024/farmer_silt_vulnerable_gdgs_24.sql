{{ config(
  materialized='table'
) }}

WITH farmer_silt AS (
    SELECT 
        dam,
        state,
        district,
        taluka,
        village,
        ngo_name,
        SUM(CASE 
                WHEN category_of_farmer IN ('Marginal (0-2.49 acres)', 'Small (2.5-4.99 acres)', 'Widow', 'Disabled', 'Family of farmer who committed suicide')
                THEN total_silt_carted ELSE 0 END) AS total_silt_vulnerable,
        SUM(CASE 
                WHEN category_of_farmer IN ('Semi-medium (5 to 9.99 acre)', 'Medium (10-24.99 acres)', 'Large (above 25 acres)')
                THEN total_silt_carted ELSE 0 END) AS total_silt_others
    FROM {{ ref('farmer_calc_silt_gdgs_24') }}
    GROUP BY 
        dam, state, district, taluka, village, ngo_name
)

SELECT 
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    total_silt_vulnerable,
    total_silt_others
FROM farmer_silt
ORDER BY state, district, taluka, village, dam
