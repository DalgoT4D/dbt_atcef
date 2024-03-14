WITH WorkOrderEncounters AS (
    SELECT
        subject_id,
        COUNT(*) AS encounters_count, -- Total number of encounters per subject
        MAX(date_time) AS last_encounter_date -- Date of the latest encounter
    FROM {{ ref('encounter_2022') }}
    GROUP BY subject_id
)
SELECT
    s.uid,
    s.first_name,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.silt_to_be_excavated,
    COALESCE(woe.encounters_count, 0) AS encounters_count,
    woe.last_encounter_date,
    CASE
        WHEN woe.encounters_count IS NULL THEN 'Not Started'
        ELSE 'Ongoing'
    END AS project_status
FROM {{ ref('subjects_2022') }} AS s
LEFT JOIN WorkOrderEncounters AS woe ON s.uid = woe.subject_id
