{{ config(
  materialized='table',
  tags=["glific"]
) }}


SELECT 
  CASE 
    WHEN u.is_avni_training_done = 1 AND u.is_rwb_training_done = 0 THEN 'Avni Training Only'
    WHEN u.is_avni_training_done = 0 AND u.is_rwb_training_done = 1 THEN 'RWB Training Only'
    WHEN u.is_avni_training_done = 1 AND u.is_rwb_training_done = 1 THEN 'Both Trainings'
    ELSE 'None'
  END AS training_type,
  COUNT(DISTINCT u.contact_phone) AS num_people
FROM {{ref('training_completion_prod')}} u
WHERE u.is_avni_training_done = 1 OR u.is_rwb_training_done = 1  -- Only consider those who completed either training
GROUP BY training_type
