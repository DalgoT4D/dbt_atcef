{{ config(
  materialized='table'
) }}

WITH aggregated_data AS (
    SELECT 
        detailed_data.state, 
        CAST(SUM(detailed_data.silt_target) AS numeric(10,2)) AS state_silt_target,
        CAST(SUM(detailed_data.silt_achieved) AS numeric(10,2)) AS state_silt_achieved
    FROM {{ ref('work_order_metric') }} AS detailed_data
    GROUP BY
        detailed_data.state
),
state_geojson AS (
    SELECT
        st_nm,
        geojson AS state_geojson
    FROM
        state_jeojson
    GROUP BY
        st_nm, geojson
)
SELECT
    ad.*,
    sg.state_geojson
FROM
    aggregated_data ad
LEFT JOIN state_geojson sg ON ad.state = sg.st_nm
