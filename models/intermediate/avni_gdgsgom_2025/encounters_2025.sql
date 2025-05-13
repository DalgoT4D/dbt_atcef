{{ config(
  materialized='table',
  tags=["intermediate", "intermediate_gdgs_2025", "gdgs_2025", "gdgs"]
) }}


with mycte as (
    select
        "ID" as eid,
        "Subject_ID" as subject_id,
        "Subject_type" as subject_type,
        "Encounter_location" as encounter_location,
        "Encounter_type" as encounter_type,
        observations ->> 'Excavating Machine' as machine_sub_id,
        observations ->> 'Distance from waterbody' as distance_from_waterbody,
        observations
        ->> 'Type of land silt is spread on' as type_of_land_silt_is_spread_on,
        CAST(
            observations
            ->> 'The total farm area on which Silt is spread' as NUMERIC
        ) as total_farm_area_silt_is_spread_on,
        observations ->> 'Farmer/Beneficiary' as farmer_sub_id,
        CAST(
            observations ->> 'Working Hours as per time' as NUMERIC
        ) as working_hours_as_per_time,
        observations
        ->> 'Total working hours of machine by time' as total_working_hours_of_machine_by_time,
        CAST(
            observations ->> 'Total working hours of machine' as NUMERIC
        ) as total_working_hours_of_machine,
        CAST(
            observations ->> 'Silt excavated as per MB recording' as NUMERIC
        ) as silt_excavated_as_per_mb_recording,
        CAST(
            observations ->> 'Total silt excavated' as NUMERIC
        ) as total_silt_excavated_encounter,
        CAST(
            TO_DATE(
                "Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ) as DATE
        ) as date_time,
        CAST(
            observations ->> 'Total Silt carted' as NUMERIC
        ) as total_silt_carted,
        observations
        ->> 'Silt carted by farmer - Number of trolleys' as silt_carted_by_farmer_trolleys,
        observations ->> 'Area covered by silt' as area_covered_by_silt,
        observations
        ->> 'Number of trolleys carted' as number_of_trolleys_carted

    from {{ source('gdgs_25_surveys', 'encounters_gdgs_2025') }}
    where "Voided" is FALSE
),


approval_encounters as (
    select
        d.*,
        a.approval_status
    from mycte as d
    inner join {{ ref('approval_statuses_gdgs_25') }} as a
        on
            d.eid = a.entity_id
            and a.entity_type = 'Encounter'
    where
        a.approval_status = 'Approved'
)

{{ dbt_utils.deduplicate(
    relation='approval_encounters',
    partition_by='eid',
    order_by='eid desc',
   )
}}
