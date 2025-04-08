-- noqa: ST11
{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2024", "gdgs_2024", "gdgs"]
) }}


WITH WORKORDERENCOUNTERS AS (
    SELECT
        E.*,
        SUBJECT_ID AS SUBJECT_UID
    FROM {{ ref('encounters_2024') }} AS E
    LEFT JOIN {{ ref('subjects_2024') }} AS S
        ON
            E.SUBJECT_ID = S.UID
            AND E.ENCOUNTER_TYPE <> 'Work order daily Recording - Farmer'
            AND S.SUBJECT_VOIDED = false
    LEFT JOIN {{ ref('subjects_2024') }} AS F
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
    WOE.*
FROM
    {{ ref('subjects_2024') }} AS S
LEFT JOIN
    WORKORDERENCOUNTERS AS WOE
    ON
        S.UID = WOE.SUBJECT_UID
WHERE
    S.DAM <> 'Test'
