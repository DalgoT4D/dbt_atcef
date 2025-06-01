{{ config(
  materialized='table',
  tags=["glific"]
) }}

with cte as (SELECT 
  contact_id,
  phone AS contact_phone,
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

FROM {{ref('contacts_field_flatten_int')}})


SELECT 
  u.contact_id,
  u.contact_phone,
  u.username_lms,
  u.project,
  u.is_registered,
  u.is_avni_training_done,
  u.is_rwb_training_done,
  
  -- Flag for whether they initiated the 'AVNI Gramin App Training Flow'
  CASE
    WHEN l_avni.contact_phone IS NOT NULL THEN 1  -- Initiated the AVNI Gramin flow
    ELSE 0   -- Did not initiate
  END AS avni_gramin_initiated_flow,

  -- Flag for whether they initiated the 'RWB Training'
  CASE
    WHEN l_rwb.contact_phone IS NOT NULL THEN 1  -- Initiated the RWB training flow
    ELSE 0   -- Did not initiate
  END AS rwb_initiated_flow
  
FROM cte u

-- LEFT JOIN for AVNI Gramin App Training Flow initiation
LEFT JOIN {{ref('training_info_prod')}} l_avni
  ON u.contact_phone = l_avni.contact_phone AND l_avni.flow_name = 'AVNI Gramin App Training Flow'

-- LEFT JOIN for RWB Training initiation
LEFT JOIN {{ref('training_info_prod')}} l_rwb
  ON u.contact_phone = l_rwb.contact_phone AND l_rwb.flow_name = 'RWB Training'

GROUP BY 
  u.contact_id, 
  u.contact_phone, 
  u.username_lms, 
  u.project, 
  u.is_registered, 
  u.is_avni_training_done, 
  u.is_rwb_training_done, 
  l_avni.contact_phone, 
  l_rwb.contact_phone
