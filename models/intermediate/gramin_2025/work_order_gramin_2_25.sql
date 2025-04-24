{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin", "gramin_niti", "gramin_25"]
) }}

WITH WORKORDERENCOUNTERS AS (
    SELECT
        E.*,
        SUBJECT_ID AS SUBJECT_UID
    FROM {{ ref('encounters_gramin_25') }} AS E
    LEFT JOIN {{ ref('subjects_gramin_25') }} AS S
        ON
            E.SUBJECT_ID = S.UID
            AND E.ENCOUNTER_TYPE <> 'Work order daily Recording - Farmer'
    LEFT JOIN {{ ref('subjects_gramin_25') }} AS F
        ON
            E.FARMER_SUB_ID = F.UID
            AND E.ENCOUNTER_TYPE = 'Work order daily Recording - Farmer'
    WHERE
        (
            E.ENCOUNTER_TYPE = 'Work order daily Recording - Farmer'
        )
        OR (
            E.ENCOUNTER_TYPE <> 'Work order daily Recording - Farmer'
        )
)

SELECT
    WOE.*,
    S.UID,
    S.FIRST_NAME,
    S.DAM,
    S.TYPE_OF_MACHINE,
    S.CATEGORY_OF_FARMER,
    S.MOBILE_NUMBER,
    S.MOBILE_VERIFIED,
    S.NGO_NAME,
    S.SILT_TARGET,
    S.SUBJECT_VOIDED,
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
FROM {{ ref('subjects_gramin_25') }} AS S
LEFT JOIN WORKORDERENCOUNTERS AS WOE
    ON S.UID = WOE.SUBJECT_UID
WHERE
    S.DAM IS NOT null
    AND S.DISTRICT IS NOT null
    AND S.TALUKA IS NOT null
    AND S.STATE IS NOT null
    AND S.VILLAGE IS NOT null
    AND S.SUBJECT_VOIDED = false
