{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin_niti"]
) }}

WITH subject_locs AS (
    SELECT
        "ID" AS uid,
        INITCAP(COALESCE(location->>'Dam', '')) AS dam,  
        INITCAP(COALESCE(location->>'District', '')) AS district,  
        CASE  -- Standardize state names
            WHEN LOWER(location->>'State') LIKE '%maharashtra%' THEN 'Maharashtra'
            WHEN LOWER(location->>'State') LIKE '%maharshatra%' THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location->>'State', ''))
        END AS state,
        INITCAP(COALESCE(location->>'Taluka', '')) AS taluka, 
        INITCAP(COALESCE(location->>'GP/Village', '')) AS village
    FROM 
        {{ source('source_gramin', 'subjects_gramin') }}
    WHERE "Voided" IS FALSE
),

cte AS (
    SELECT 
        "ID" AS eid,
        "Subject_ID" AS subject_id,
        "Subject_type" AS subject_type,
        "Encounter_location" AS encounter_location,
        "Encounter_type" AS encounter_type,
        observations->>'Excavating Machine' AS machine_sub_id,
        observations->>'Farmer/Beneficiary' AS farmer_sub_id,
        observations->>'Working Hours as per time' AS working_hours_as_per_time,
        observations->>'Total working hours of machine by time' AS total_working_hours_of_machine_by_time,
        CAST(observations->>'Total working hours of machine' AS NUMERIC) AS total_working_hours_of_machine,
        CAST(observations->>'Silt excavated as per MB recording' AS NUMERIC) AS silt_excavated_as_per_MB_recording,
        CAST(observations->>'Total silt excavated' AS NUMERIC) AS total_silt_excavated_encounter,
        CAST(TO_DATE("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS DATE) AS date_time,
        CAST(observations->>'Total Silt carted' AS FLOAT) AS total_silt_carted,
        observations->>'Silt carted by farmer - Number of trolleys' AS silt_carted_by_farmer_trolleys,
        observations->>'The total farm area on which Silt is spread' AS total_farm_area,
        observations->>'Area covered by silt' AS area_covered_by_silt,
        observations->>'Number of trolleys carted' AS number_of_trolleys_carted,
        observations->>'Distance from waterbody' AS distance_from_waterbody,
        CAST(observations->>'The total farm area on which Silt is spread' AS FLOAT) AS total_farm_area_on_which_silt_is_spread,
        "Voided" AS voided
    FROM {{ source('source_gramin', 'encounters_gramin') }}
    WHERE "Voided" IS FALSE
),

approval_encounters AS (
    SELECT 
        d.*, 
        a.approval_status,
        s.state,
        s.district,
        s.taluka,
        s.village
    FROM cte d
    JOIN {{ ref('approval_status_gramin') }} a 
        ON d.eid = a.entity_id
    LEFT JOIN subject_locs s 
        ON s.uid = d.subject_id
    WHERE a.entity_type = 'Encounter' 
      AND a.approval_status = 'Approved'
)

{{ dbt_utils.deduplicate(
    relation='approval_encounters',
    partition_by='eid',
    order_by='eid DESC'
) }}