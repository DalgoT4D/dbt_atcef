{{ config(
  materialized='table'
) }}


with converted_statuses as (
    select 
        "Entity_ID" as entity_id, 
        "Approval_status_comment" as approval_status_comment,
        "Entity_type" as entity_type,
        "Approval_status" as approval_status, 
        cast("Status_date_time" as timestamp) as status_date
    from {{ source('source_gramin', 'approval_statuses') }}
),
latest_entries as (
    select 
        entity_id,
        approval_status_comment,
        entity_type,
        approval_status,
        status_date
    from (
        select 
            entity_id,
            approval_status_comment,
            entity_type,
            approval_status,
            status_date,
            row_number() over (partition by entity_id order by status_date desc) as row_num
        from converted_statuses
    ) sub
    where row_num = 1
)

select * from latest_entries
