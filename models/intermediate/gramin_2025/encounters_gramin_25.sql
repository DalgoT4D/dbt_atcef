{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin_niti", "gramin_niti", "gramin_25"]
) }}

WITH subject_locs AS (
    SELECT
        "ID" AS uid,
        INITCAP(COALESCE(location ->> 'Dam', '')) AS dam,
        INITCAP(COALESCE(location ->> 'District', '')) AS district,
        CASE  -- Standardize state names
            WHEN
                LOWER(location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(location ->> 'Taluka', '')) AS taluka,
        INITCAP(COALESCE(location ->> 'GP/Village', '')) AS village
    FROM
        {{ source('source_gramin_25', 'subjects_gramin_25') }}
    WHERE "Voided" IS FALSE
),

cte AS (
    SELECT
        "ID" AS eid,
        "Subject_ID" AS subject_id,
        "Subject_type" AS subject_type,
        "Encounter_location" AS encounter_location,
        "Encounter_type" AS encounter_type,
        "Voided" AS voided,
        observations ->> 'Distance from waterbody' AS distance_from_waterbody,
        observations
        ->> 'Type of land silt is spread on' AS type_of_land_silt_is_spread_on,
        CAST(
            observations
            ->> 'The total farm area on which Silt is spread' AS NUMERIC
        ) AS total_farm_area_silt_is_spread_on,
        observations ->> 'Excavating Machine' AS machine_sub_id,
        observations ->> 'Farmer/Beneficiary' AS farmer_sub_id,
        observations
        ->> 'Working Hours as per time' AS working_hours_as_per_time,
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
        ) AS total_silt_excavated_encounter,
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
        ->> 'Number of trolleys carted' AS number_of_trolleys_carted
    FROM {{ source('source_gramin_25', 'encounters_gramin_25') }}
    WHERE "Voided" IS FALSE
),

encounters AS (
    SELECT
        d.*,
        s.state,
        s.district,
        s.taluka,
        s.village
    FROM cte AS d
    LEFT JOIN subject_locs AS s
        ON
            d.subject_id = s.uid
)

{{ dbt_utils.deduplicate(
    relation='encounters',
    partition_by='eid',
    order_by='eid DESC'
) }}
