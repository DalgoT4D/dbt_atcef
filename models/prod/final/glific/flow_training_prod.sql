{{ config(
  materialized='table',
  tags=["glific"]
) }}

WITH filtered_messages AS (
  SELECT
    id,
    flow_name
  FROM intermediate_glific.messages_int
  WHERE flow = 'inbound'
    AND flow_name IN ('Hi Flow', 'AVNI Training  flow')
),

contact_flow_flags AS (
  SELECT
    id,
    MAX(CASE WHEN flow_name = 'Hi Flow' THEN 1 ELSE 0 END) AS has_help_flow,
    MAX(CASE WHEN flow_name = 'AVNI Training  flow' THEN 1 ELSE 0 END) AS has_training_flow
  FROM filtered_messages
  GROUP BY id
),

flow_group AS (
  SELECT
    id,
    CASE 
      WHEN has_help_flow = 1 AND has_training_flow = 1 THEN 'Both Flows'
      WHEN has_help_flow = 1 THEN 'Only Help Flow'
      WHEN has_training_flow = 1 THEN 'Only Training Flow'
    END AS flow_category
  FROM contact_flow_flags
)

SELECT
  flow_category,
  COUNT(DISTINCT id) AS user_count
FROM flow_group
GROUP BY flow_category
