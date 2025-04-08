{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2022", "niti_2022", "niti"]
) }}

-- This part of the query creates a CTE named WorkOrderEncounters which will be used later in the main query.
-- It selects all columns from the table encounter_2022 and renames subject_id to subject_uid.
-- There are two LEFT JOIN operations:
-- The first LEFT JOIN joins encounter_2022 (e) with subjects_2022 (s) based on subject_id and filters out records where the encounter type is 'Work order daily Recording - Farmer' and s.subject_voided is false.
-- The second LEFT JOIN joins encounter_2022 (e) with subjects_2022 (f) based on farmer_sub_id and includes only records where the encounter type is 'Work order daily Recording - Farmer' and f.subject_voided is false.
-- The WHERE clause filters records based on two conditions:
-- The first condition includes records where encounter_type is 'Work order daily Recording - Farmer' and farmer_sub_id, machine_sub_id, and f.uid are all not null.
-- The second condition includes records where encounter_type is not 'Work order daily Recording - Farmer' and s.uid is not null.

-- This part of the query selects all columns from the subjects_2022 table (s) and the CTE WorkOrderEncounters (woe).
-- It also includes three CASE statements to categorize the projects based on their status:
-- project_ongoing is set to 'Ongoing' if woe.subject_id is not null and woe.encounter_type is not 'Work order endline'.
-- project_not_started is set to 'Yet to start' if woe.subject_id is null.
-- project_completed is set to 'Completed' if woe.encounter_type is 'Work order endline'.
-- There is a LEFT JOIN between subjects_2022 (s) and the CTE WorkOrderEncounters (woe) on s.uid and woe.subject_uid.
-- The WHERE clause filters records to include only those where s.dam, s.district, s.taluka, s.state, and s.village are not null and s.subject_voided is false.
-- Summary
-- The query is designed to:

-- Create a temporary table (WorkOrderEncounters) that includes encounter records linked to subjects and farmers under certain conditions.
-- Retrieve all subjects with relevant details, along with their encounter details from the CTE.
-- Categorize each subject's project status as 'Ongoing', 'Yet to start', or 'Completed' based on the conditions specified in the CASE statements.
-- Ensure only subjects with valid geographic details and not marked as voided are included in the final result set.

WITH WORKORDERENCOUNTERS AS (
    SELECT
        E.*,
        SUBJECT_ID AS SUBJECT_UID
    FROM {{ ref('encounter_2022') }} AS E
    LEFT JOIN {{ ref('subjects_2022') }} AS S
        ON
            E.SUBJECT_ID = S.UID
            AND E.ENCOUNTER_TYPE <> 'Work order daily Recording - Farmer'
            AND S.SUBJECT_VOIDED = false
    LEFT JOIN {{ ref('subjects_2022') }} AS F
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
FROM {{ ref('subjects_2022') }} AS S
LEFT JOIN WORKORDERENCOUNTERS AS WOE
    ON S.UID = WOE.SUBJECT_UID
WHERE
    S.DAM IS NOT null
    AND S.DISTRICT IS NOT null
    AND S.TALUKA IS NOT null
    AND S.STATE IS NOT null
    AND S.VILLAGE IS NOT null
    AND S.SUBJECT_VOIDED = false
