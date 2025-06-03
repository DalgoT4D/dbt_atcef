{{ config(
  materialized='table',
  tags=["glific"]
) }}

WITH latest_inbound_messages AS (
  SELECT
    contact_phone,
    flow_name,
    is_hsm,
    bsp_status,
    inserted_date,
    updated_date,
    ROW_NUMBER() OVER (PARTITION BY contact_phone ORDER BY updated_date DESC NULLS LAST) AS rn
  FROM {{ ref('messages_int') }}
  WHERE flow = 'inbound'
)

SELECT
  contact_phone,
  flow_name,
  is_hsm,
  bsp_status,
  inserted_date,
  updated_date
FROM latest_inbound_messages
WHERE rn = 1
