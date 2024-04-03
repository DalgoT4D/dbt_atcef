{{ config(
  materialized='table'
) }}


WITH aggregated_data AS (
    SELECT 
        dam, 
        MAX(date_time) as date_time,
        district, 
        state, 
        taluka, 
        village, 
        COALESCE(MAX(silt_to_be_excavated), 0) as silt_target, 
        COALESCE(CAST(ROUND(SUM(CASE WHEN total_silt_carted::text <> 'NaN' THEN total_silt_carted ELSE 0 END)) AS numeric), 0) AS silt_achieved,
        COUNT(CASE WHEN mobile_verified = TRUE THEN 1 ELSE NULL END) AS mobile_verified_count,
        COUNT(CASE WHEN mobile_verified = FALSE THEN 1 ELSE NULL END) AS mobile_unverified_count,
        SUM(CASE WHEN  category_of_farmer = 'Small (2.5-4.99 acres)' THEN 1 ELSE 0 END) AS vulnerable_small,
        SUM(CASE WHEN  category_of_farmer = 'Marginal (0-2.49 acres)' THEN 1 ELSE 0 END) AS vulnerable_marginal,
        SUM(CASE WHEN  category_of_farmer = 'Semi-medium (5-9.55 acres)' THEN 1 ELSE 0 END) AS semi_medium,
        SUM(CASE WHEN  category_of_farmer = 'Medium (10-24.99 acres)' THEN 1 ELSE 0 END) AS medium,
        SUM(CASE WHEN  category_of_farmer = 'Large (above 25 acres)' THEN 1 ELSE 0 END) AS large
    FROM
        {{ ref('work_order_union') }} 
    WHERE
       dam is not null and date_time is not null
    GROUP BY
        state, district, taluka, village, dam
),
state_geojson AS (
    SELECT
        st_nm,
        MAX(geojson) AS state_geojson
    FROM
        state_jeojson
    GROUP BY
        st_nm
),
district_geojson AS (
    SELECT
        dtname,
        MAX(geojson) AS district_geojson
    FROM
        public.fips_geojson
    GROUP BY
        dtname
)

SELECT
    ad.*,
    sg.state_geojson,
    dj.district_geojson
FROM
    aggregated_data ad
LEFT JOIN state_geojson sg ON ad.state = sg.st_nm
LEFT JOIN district_geojson dj ON ad.district = dj.dtname
