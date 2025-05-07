{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2024", "niti_2024", "niti"]
) }}

WITH WORKORDERENCOUNTERS AS (
    SELECT
        E.*,
        SUBJECT_ID AS SUBJECT_UID
    FROM {{ ref('encounters_niti_2025') }} AS E
    LEFT JOIN {{ ref('subjects_niti_2025') }} AS S
        ON
            E.SUBJECT_ID = S.UID
            AND E.ENCOUNTER_TYPE <> 'Work order daily Recording - Farmer'
            AND S.SUBJECT_VOIDED = false
    LEFT JOIN {{ ref('subjects_niti_2025') }} AS F
        ON
            E.FARMER_SUB_ID = F.UID
            AND E.ENCOUNTER_TYPE = 'Work order daily Recording - Farmer'
            AND F.SUBJECT_VOIDED = false
    WHERE
        (
            E.ENCOUNTER_TYPE = 'Work order daily Recording - Farmer'
            AND E.FARMER_SUB_ID IS NOT null
            AND E.MACHINE_SUB_ID IS NOT null
            AND F.UID IS NOT null
        )
        OR (
            E.ENCOUNTER_TYPE <> 'Work order daily Recording - Farmer'
            AND S.UID IS NOT null
        )
)

SELECT
    S.*,
    WOE.*,
    CASE
        WHEN
            WOE.SUBJECT_ID IS NOT null
            AND WOE.ENCOUNTER_TYPE <> 'Work order endline'
            THEN 'Ongoing'
    END AS PROJECT_ONGOING,
    CASE
        WHEN WOE.SUBJECT_ID IS null THEN 'Yet to start'
    END AS PROJECT_NOT_STARTED,
    CASE
        WHEN WOE.ENCOUNTER_TYPE = 'Work order endline' THEN 'Completed'
    END AS PROJECT_COMPLETED
FROM {{ ref('subjects_niti_2025') }} AS S
LEFT JOIN WORKORDERENCOUNTERS AS WOE
    ON S.UID = WOE.SUBJECT_UID
WHERE
    S.DAM IS NOT null
    AND S.DISTRICT IS NOT null
    AND S.TALUKA IS NOT null
    AND S.STATE IS NOT null
    AND S.VILLAGE IS NOT null
    AND S.SUBJECT_VOIDED = false
