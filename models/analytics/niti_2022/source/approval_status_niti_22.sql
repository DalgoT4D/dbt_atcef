-- Keeps the latest approval status per entity from the rwb_niti_2022 approval_statuses source 
-- so downstream models can read a single current status flag.

{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2022", "analytics_intermediate", "source_cleaned_niti_2022"]
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
        {{ source('source_atecf_surveyss', 'approval_statuses') }}
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
