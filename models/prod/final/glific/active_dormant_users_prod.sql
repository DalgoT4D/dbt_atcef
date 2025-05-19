{{ config(
  materialized='table',
  tags=["glific"]
) }}

SELECT
  contact_phone,
  COUNT(*) FILTER (WHERE flow = 'inbound') AS inbound_message_count,
  COUNT(*) FILTER (WHERE flow = 'outbound') AS outbound_message_count
FROM {{ref('messages_int')}}
WHERE contact_phone NOT LIKE '987654321%'
GROUP BY contact_phone

