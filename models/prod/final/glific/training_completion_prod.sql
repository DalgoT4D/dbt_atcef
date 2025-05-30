{{ config(
  materialized='table',
  tags=["glific"]
) }}

SELECT 
  contact_id,
  username_lms,
  SPLIT_PART(username_lms, '@', 2) AS project,
  -- Convert 'is_registered' to 1 if 'y', 0 if empty or NULL
  CASE 
    WHEN is_registered = 'y' THEN 1
    WHEN is_registered IS NULL OR is_registered = '' THEN 0
    ELSE NULL 
  END AS is_registered,

  -- Convert 'is_avni_training_done' to 1 if 'y', 0 if empty or NULL
  CASE 
    WHEN is_avni_training_done = 'y' THEN 1
    WHEN is_avni_training_done IS NULL OR is_avni_training_done = '' THEN 0
    ELSE NULL 
  END AS is_avni_training_done,

  -- Convert 'is_rwb_training_done' to 1 if 'y', 0 if empty or NULL
  CASE 
    WHEN is_rwb_training_done = 'y' THEN 1
    WHEN is_rwb_training_done IS NULL OR is_rwb_training_done = '' THEN 0
    ELSE NULL 
  END AS is_rwb_training_done

FROM {{ref('contacts_field_flatten_int')}}