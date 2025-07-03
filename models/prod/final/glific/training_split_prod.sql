{{ config(
  materialized='table',
  tags=["glific"]
) }}

WITH base_training_status AS (
    SELECT 
        contact_id,
        avni_training_completed,
        rwb_training_completed,
        -- Pre-calculate training type to avoid repetition
        CASE 
            WHEN avni_training_completed = 1 AND rwb_training_completed = 0 THEN 'Avni Training Only'
            WHEN avni_training_completed = 0 AND rwb_training_completed = 1 THEN 'RWB Training Only'
            WHEN avni_training_completed = 1 AND rwb_training_completed = 1 THEN 'Both Trainings'
            ELSE 'Training Not Started'
        END AS training_type
    FROM {{ ref('contact_flow_status_prod') }}
)

SELECT 
    training_type,
    COUNT(DISTINCT contact_id) AS num_people
FROM base_training_status
GROUP BY training_type
ORDER BY 
    CASE training_type
        WHEN 'Avni Training Only' THEN 1
        WHEN 'Both Trainings' THEN 2
        WHEN 'RWB Training Only' THEN 3
        WHEN 'Training Not Started' THEN 4
    END
