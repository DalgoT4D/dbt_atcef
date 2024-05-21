{{ config(
  materialized='table'
) }}

WITH WorkOrderEncounters AS (
    SELECT 
        e.*,
        subject_id as subject_uid,
        s.first_name AS subject_first_name_s,
        s.mobile_verified AS subject_mobile_verified_s,
        s.mobile_number AS subject_mobile_number_s,
        s.dam AS subject_dam_s,
        s.ngo_name AS subject_ngo_name_s,
        s.district AS subject_district_s,
        s.state AS subject_state_s,
        s.taluka AS subject_taluka_s,
        s.village AS subject_village_s,
        s.type_of_machine AS subject_type_of_machine_s,
        s.silt_target AS subject_silt_target_s,
        s.category_of_farmer AS subject_category_of_farmer_s,
        f.first_name AS farmer_first_name,
        f.mobile_verified AS farmer_mobile_verified,
        f.mobile_number AS farmer_mobile_number,
        f.category_of_farmer AS farmer_category_of_farmer,
        m.first_name AS machine_first_name,
        m.type_of_machine AS machine_type_of_machine
    FROM {{ ref('encounter_2022') }} AS e
    LEFT JOIN {{ ref('subjects_2022') }} AS s 
        ON e.subject_id = s.uid 
        AND e.encounter_type <> 'Work order daily Recording - Farmer'
        AND s.voided = false
    LEFT JOIN {{ ref('subjects_2022') }} AS f 
        ON e.farmer_sub_id = f.uid 
        AND e.encounter_type = 'Work order daily Recording - Farmer'
        AND f.voided = false
    LEFT JOIN {{ ref('subjects_2022') }} AS m 
        ON e.machine_sub_id = m.uid 
        AND e.encounter_type = 'Work order daily Recording - Farmer'
        AND m.voided = false
    WHERE 
        (e.encounter_type = 'Work order daily Recording - Farmer' 
            AND e.farmer_sub_id IS NOT NULL 
            AND e.machine_sub_id IS NOT NULL 
            AND f.uid IS NOT NULL 
            AND m.uid IS NOT NULL)
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
    AND s.voided = false

