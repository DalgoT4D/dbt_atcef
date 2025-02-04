{{ config(
  materialized='table',
  tags=["final","final_niti_2023"]
) }}

WITH farmer_silt AS (
    SELECT
        f.state,
        f.district,
        f.taluka,
        f.village,
        f.dam,
        e.ngo_name,
        SUM(CASE 
                WHEN f.category_of_farmer IN ('Marginal (0-2.49 acres)', 'Small (2.5-4.99 acres)', 'Widow', 'Disabled', 'Family of farmer who committed suicide')
                THEN e.total_silt_carted ELSE 0 
            END) AS vulnerable_silt,
        SUM(CASE 
                WHEN f.category_of_farmer IN ('Semi-medium (5 to 9.99 acre)', 'Medium (10-24.99 acres)', 'Large (above 25 acres)')
                THEN e.total_silt_carted ELSE 0 
            END) AS others_silt
    FROM
        {{ ref('farmer_silt_calc_niti_2023') }} AS e
    LEFT JOIN {{ ref('farmer_niti_2023') }} AS f
        ON e.farmer_id = f.farmer_id
    GROUP BY
        f.state,
        f.district,
        f.taluka,
        f.village,
        f.dam,
        e.ngo_name
)

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'vulnerable' AS farmer_type,
    SUM(vulnerable_silt) AS total_silt_carted
FROM
    farmer_silt
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name

UNION ALL

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'others' AS farmer_type,
    SUM(others_silt) AS total_silt_carted
FROM
    farmer_silt
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name

ORDER BY
    state,
    district,
    taluka,
    village,
    dam, farmer_type
