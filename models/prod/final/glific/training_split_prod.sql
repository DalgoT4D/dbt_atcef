{{ config(
  materialized='table',
  tags=["glific"]
) }}

WITH training_categories AS (
    SELECT 
        CASE 
            WHEN avni_training_completed = 1 AND rwb_training_completed = 0 THEN 'Avni Training Only'
            WHEN avni_training_completed = 0 AND rwb_training_completed = 1 THEN 'RWB Training Only'
            WHEN avni_training_completed = 1 AND rwb_training_completed = 1 THEN 'Both Trainings'
            ELSE 'Training Not Started'
        END AS training_type,
        COUNT(DISTINCT contact_id) AS num_people
    FROM {{ ref('contact_flow_status_prod') }}
    GROUP BY 
        CASE 
            WHEN avni_training_completed = 1 AND rwb_training_completed = 0 THEN 'Avni Training Only'
            WHEN avni_training_completed = 0 AND rwb_training_completed = 1 THEN 'RWB Training Only'
            WHEN avni_training_completed = 1 AND rwb_training_completed = 1 THEN 'Both Trainings'
            ELSE 'Training Not Started'
        END
)

SELECT *
FROM training_categories
ORDER BY 
    CASE training_type
        WHEN 'Avni Training Only' THEN 1
        WHEN 'Both Trainings' THEN 2
        WHEN 'RWB Training Only' THEN 3
        WHEN 'Training Not Started' THEN 4
    END
