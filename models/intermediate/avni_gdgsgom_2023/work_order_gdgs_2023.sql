{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2023", "gdgs_2023", "gdgs"]
) }}


WITH WorkOrderEncounters AS (
    SELECT 
        e.*,
        subject_id as subject_uid
    FROM {{ ref('encounters_gdgs_2023') }} AS e
    LEFT JOIN {{ ref('subjects_gdgs_2023') }} AS s 
        ON e.subject_id = s.uid 
        AND e.encounter_type <> 'Work order daily Recording - Farmer'
        AND s.subject_voided = false
    LEFT JOIN {{ ref('subjects_gdgs_2023') }} AS f 
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
    woe.*
FROM 
    {{ ref('subjects_gdgs_2023') }} AS s
LEFT JOIN 
    WorkOrderEncounters AS woe 
ON 
    s.uid = woe.subject_uid
WHERE 
    s.dam != 'Test'
