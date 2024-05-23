{{ config(
  materialized='table'
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

WITH WorkOrderEncounters AS (
    SELECT 
        e.*,
        subject_id as subject_uid
    FROM {{ ref('encounter_2022') }} AS e
    LEFT JOIN {{ ref('subjects_2022') }} AS s 
        ON e.subject_id = s.uid 
        AND e.encounter_type <> 'Work order daily Recording - Farmer'
        AND s.subject_voided = false
    LEFT JOIN {{ ref('subjects_2022') }} AS f 
        ON e.farmer_sub_id = f.uid 
        AND e.encounter_type = 'Work order daily Recording - Farmer'
        AND f.subject_voided = false
    WHERE 
        (e.encounter_type = 'Work order daily Recording - Farmer' 
            AND e.farmer_sub_id IS NOT NULL 
            AND e.machine_sub_id IS NOT NULL 
            AND f.uid IS NOT NULL)
        OR (e.encounter_type <> 'Work order daily Recording - Farmer' 
            AND s.uid IS NOT NULL)
)

SELECT 
    s.*,
    woe.*,
    CASE 
        WHEN woe.subject_id IS NOT NULL AND woe.encounter_type <> 'Work order endline' THEN 'Ongoing'
    ELSE NULL
    END AS project_ongoing,
    CASE 
        WHEN woe.subject_id IS NULL THEN 'Yet to start' 
        ELSE NULL
    END AS project_not_started,
    CASE 
        WHEN woe.encounter_type = 'Work order endline' THEN 'Completed'
        ELSE NULL
    END AS project_completed
FROM {{ ref('subjects_2022') }} AS s
LEFT JOIN WorkOrderEncounters AS woe 
    ON s.uid = woe.subject_uid
WHERE 
    s.dam IS NOT NULL
    AND s.district IS NOT NULL
    AND s.taluka IS NOT NULL
    AND s.state IS NOT NULL
    AND s.village IS NOT NULL
    AND s.subject_voided = false

