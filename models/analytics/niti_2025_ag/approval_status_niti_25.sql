{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytics_intermediate"]
) }}


WITH RankedStatuses AS (
    SELECT
        "Entity_ID" AS entity_id,
        "Entity_type" AS entity_type,
        "Approval_status" AS approval_status,
        "Status_date_time" AS status_date_time,
        ROW_NUMBER() OVER (
            PARTITION BY "Entity_ID"
            ORDER BY "Status_date_time" DESC
        ) AS rn
    FROM
        {{ source('rwb_niti_2025', 'approval_statuses') }}
)

SELECT
    entity_id,
    entity_type,
    approval_status
FROM
    RankedStatuses
WHERE
    rn = 1
