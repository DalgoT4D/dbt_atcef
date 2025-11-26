{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti"]
) }}


SELECT * FROM {{ ref('encounters_niti_2025') }} 