{{ config(
  materialized='table'
) }}


select "Entity_ID" as entity_id, 
       "Approval_status_comment" as approval_status_comment,
       "Entity_type" as entity_type,
       "Approval_status" as approval_status 
from {{ source('source_atecf_surveyss', 'approval_statuses') }} 