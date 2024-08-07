{{ config(
  materialized='table'
) }}


WITH mycte AS (
  SELECT
    "ID" AS eid,
    "Subject_ID" AS subject_id,
    "Subject_type" AS subject_type,
    "Encounter_location" AS encounter_location,
    "Encounter_type" AS encounter_type,
    observations ->> 'Excavating Machine' as machine_sub_id,
    observations ->> 'Farmer/Beneficiary' as farmer_sub_id,
    CAST(observations->>'Working Hours as per time' AS numeric) AS working_hours_as_per_time,
    observations->>'Total working hours of machine by time' AS total_working_hours_of_machine_by_time,
    CAST(observations->>'Total working hours of machine' AS numeric) AS total_working_hours_of_machine,
    CAST(observations->>'Silt excavated as per MB recording' AS numeric) AS silt_excavated_as_per_MB_recording,
    CAST(observations->>'Total silt excavated' AS numeric) AS total_silt_excavated,
    CAST(TO_DATE("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS date) AS date_time,
    CAST(observations ->> 'Total Silt carted' AS FLOAT) AS total_silt_carted,
    observations ->> 'Silt carted by farmer - Number of trolleys' AS silt_carted_by_farmer_trolleys,
    observations ->> 'The total farm area on which Silt is spread' AS total_farm_area,
    observations ->> 'Area covered by silt' AS area_covered_by_silt,
    observations ->> 'Number of trolleys carted' AS number_of_trolleys_carted,
    observations ->> 'Distance from waterbody' as distance_from_waterbody,
    CAST(observations ->> 'The total farm area on which Silt is spread' AS FLOAT) AS total_farm_area_on_which_Silt_is_spread,
    observations ->> 'Total silt excavated by GP (for non-farm purpose)' AS total_silt_excavated_by_GP_for_non_farm_purpose,
    "Voided" as encounter_voided
  FROM {{ source('source_atecf_surveys', 'encounter_2023') }} 
), 

approval_encounters AS (
SELECT d.*, a.approval_status
FROM mycte d
JOIN {{ ref('approval_statuses_niti_2023') }} a ON d.eid = a.entity_id
WHERE a.entity_type = 'Encounter' 
and a.approval_status = 'Approved'
AND d.encounter_voided = 'false'
)

{{ dbt_utils.deduplicate(
      relation='approval_encounters',
      partition_by='eid',
      order_by='eid desc'
)}}