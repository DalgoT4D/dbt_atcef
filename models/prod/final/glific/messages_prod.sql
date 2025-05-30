{{ config(
  materialized='table',
  tags=["glific"]
) }}


SELECT contact_phone, COUNT(*) AS message_count
FROM {{ref ('messages_int')}}
WHERE flow_name = 'Hi Flow'
GROUP BY contact_phone