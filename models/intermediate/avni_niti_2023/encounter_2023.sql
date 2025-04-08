{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2023", "niti_2023", "niti"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS eid,
        "Subject_ID" AS subject_id,
        "Subject_type" AS subject_type,
        "Encounter_location" AS encounter_location,
        "Encounter_type" AS encounter_type,
        "Voided" AS encounter_voided,
        observations ->> 'Distance from waterbody' AS distance_from_waterbody,
        observations
        ->> 'Type of land silt is spread on' AS type_of_land_silt_is_spread_on,
        CAST(
            observations
            ->> 'The total farm area on which Silt is spread' AS NUMERIC
        ) AS total_farm_area_silt_is_spread_on,
        observations ->> 'Excavating Machine' AS machine_sub_id,
        observations ->> 'Farmer/Beneficiary' AS farmer_sub_id,
        CAST(
            observations ->> 'Working Hours as per time' AS NUMERIC
        ) AS working_hours_as_per_time,
        observations
        ->> 'Total working hours of machine by time' AS total_working_hours_of_machine_by_time,
        CAST(
            observations ->> 'Total working hours of machine' AS NUMERIC
        ) AS total_working_hours_of_machine,
        CAST(
            observations ->> 'Silt excavated as per MB recording' AS NUMERIC
        ) AS silt_excavated_as_per_mb_recording,
        CAST(
            observations ->> 'Total silt excavated' AS NUMERIC
        ) AS total_silt_excavated,
        CAST(
            TO_DATE(
                "Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ) AS DATE
        ) AS date_time,
        CAST(
            observations ->> 'Total Silt carted' AS NUMERIC
        ) AS total_silt_carted,
        observations
        ->> 'Silt carted by farmer - Number of trolleys' AS silt_carted_by_farmer_trolleys,
        observations ->> 'Area covered by silt' AS area_covered_by_silt,
        observations
        ->> 'Number of trolleys carted' AS number_of_trolleys_carted,
        observations
        ->> 'Total silt excavated by GP (for non-farm purpose)' AS total_silt_excavated_by_gp_for_non_farm_purpose
    FROM {{ source('source_atecf_surveys', 'encounter_2023') }}
),

approval_encounters AS (
    SELECT
        d.*,
        a.approval_status
    FROM mycte AS d
    INNER JOIN {{ ref('approval_statuses_niti_2023') }} AS a
        ON
            d.eid = a.entity_id
            AND a.entity_type = 'Encounter'
    WHERE
        a.approval_status = 'Approved'
        AND d.encounter_voided = 'false'
)

{{ dbt_utils.deduplicate(
      relation='approval_encounters',
      partition_by='eid',
      order_by='eid desc'
) }}
