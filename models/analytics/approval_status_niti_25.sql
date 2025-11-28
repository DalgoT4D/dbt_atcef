{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti"]
) }}


SELECT
"Entity_ID" as entity_id,
"Entity_type" as entity_type,
"Approval_status" as approval_status

--from staging_rwb_niti_25_26.approvalStatuses
from {{ source('rwb_niti_2025', 'approval_statuses') }}

