{{ config(
  materialized='table',
  tags=["glific"]
) }}

SELECT * from {{ref('message_conversations_int')}}

