-- Keeps the latest approval status per entity from the gdgs_2025 approval_statuses source 
-- so downstream models can read a single current status flag.

{{ config(
  materialized='table',
  tags=["analytics", "analytics_intermediate", "source_cleaned_gdgs_2025"]
) }}


WITH RankedStatuses AS (
    SELECT
        "Entity_ID" AS entity_id, -- this can be a workorder, subject or encounter id
        "Entity_type" AS entity_type,
        "Approval_status" AS approval_status,
       cast("Status_date_time" AS timestamp) AS status_date_time,
        ROW_NUMBER() OVER (
            PARTITION BY "Entity_ID"
            ORDER BY "Status_date_time" DESC
        ) AS rn
    FROM
        {{ source('gdgs_25_surveys', 'approval_statuses') }}
)

SELECT
    entity_id,
    entity_type,
    approval_status,
    status_date_time
FROM
    RankedStatuses
WHERE
    rn = 1