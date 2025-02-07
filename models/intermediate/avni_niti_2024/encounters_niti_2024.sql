{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2024"]
) }}


with cte as (
Select 
 "ID" AS eid,
 "Subject_ID" AS subject_id,
 "Subject_type" AS subject_type,
 "Encounter_location" AS encounter_location,
  "Encounter_type" AS encounter_type,
  observations ->> 'Distance from waterbody' as distance_from_waterbody,
  observations ->> 'Type of land silt is spread on' as type_of_land_silt_is_spread_on,
  CAST(observations ->> 'The total farm area on which Silt is spread' as NUMERIC) as total_farm_area_silt_is_spread_on,
  observations ->> 'Excavating Machine' as machine_sub_id,
  observations ->> 'Farmer/Beneficiary' as farmer_sub_id,
  observations->>'Working Hours as per time' as working_hours_as_per_time,
  observations->>'Total working hours of machine by time' as total_working_hours_of_machine_by_time,
  CAST(observations->>'Total working hours of machine' AS numeric) as total_working_hours_of_machine,
  CAST(observations->>'Silt excavated as per MB recording' AS numeric) as silt_excavated_as_per_MB_recording,
  CAST(observations->>'Total silt excavated' as numeric) as total_silt_excavated_encounter,
  CAST(TO_DATE("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS date) AS date_time,
  CAST(observations ->> 'Total Silt carted' AS NUMERIC) AS total_silt_carted,
  observations ->> 'Silt carted by farmer - Number of trolleys' AS silt_carted_by_farmer_trolleys,
      observations ->> 'Total silt excavated by GP (for non-farm purpose)' AS total_silt_excavated_by_GP_for_non_farm_purpose,
  observations ->> 'Area covered by silt' as area_covered_by_silt,
  observations ->> 'Number of trolleys carted' as number_of_trolleys_carted,
  "Voided" as voided
  
FROM {{ source('source_avni_niti_2024', 'encounters_niti_2024') }}),


approval_encounters AS (
SELECT d.*, a.approval_status
FROM cte d
JOIN {{ ref('approval_status_niti_2024') }} a ON d.eid = a.entity_id
WHERE a.entity_type = 'Encounter' 
and a.approval_status = 'Approved'
)



{{ dbt_utils.deduplicate(
      relation='approval_encounters',
      partition_by='eid',
      order_by='eid desc'
)}}