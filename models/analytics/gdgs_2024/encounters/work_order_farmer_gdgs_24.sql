-- contains information from the encounters staging table filtered for "Work order daily Recording - Farmer"
  {{ config(
    materialized='table',
    tags=["analytics","analytics_gdgs_2024",  "analytics_intermediate", "analytics_encounters_gdgs_24"]
  ) }}

with base as (
  SELECT
      e.eid,
      e.subject_id as farmer_work_order_sub_id,
      e.encounter_type,
      e.subject_type,
      e.encounter_date_time,
      CAST(e.observations ->> 'Total Silt carted' AS NUMERIC) AS silt_carted,
      e.observations ->> 'Excavating Machine' AS machine_sub_id,
      e.observations ->> 'Farmer/Beneficiary' AS farmer_beneficiary_id,
      CAST(e.observations ->> 'Number of trolleys carted' AS NUMERIC) AS trolleys_carted,
      CAST(e.observations ->> 'Capacity of trolleys in cu.m.' AS NUMERIC) AS capacity_trolleys,
      CAST(e.observations ->> 'Number of hyvas/dumper carted' AS NUMERIC) AS hyvas_carted,
      e.observations ->> 'The silt has been used for non-farm purpose' AS if_silt_used_non_farm_purpose,
      REPLACE(
        REPLACE(
            REPLACE(
                e.observations ->> 'Purpose of carting silt',
                '[', ''
            ),
            ']', ''
        ),
        '"', ''
    ) AS purpose_of_carting_silt,
      CAST(e.observations ->> 'How much silt has been used for non-farm purpose' AS NUMERIC)  AS amt_silt_used_non_farm_purpose,
      e.voided,
    
    e.observations ->> 'Silt taken by' AS silt_taken_by, -- new
    e.observations ->> 'Other person taking silt' AS other_person_taking_silt, -- new
    e.observations ->> 'Other purpose of carting silt' AS other_purpose_of_carting_silt -- new
    
  FROM {{ ref('encounter_type_gdgs_24') }} e
  WHERE e.encounter_type = 'Work order daily Recording - Farmer'
  --  and e.voided = false
)

  {{ dbt_utils.deduplicate(
    relation='base',
    partition_by='eid',
    order_by='eid desc',
   )
}}
