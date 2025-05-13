{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2024", "gdgs_2024", "gdgs"]
) }}


with converted_statuses as (
    select
        "Entity_ID" as entity_id,
        "Approval_status_comment" as approval_status_comment,
        "Entity_type" as entity_type,
        "Approval_status" as approval_status,
        cast("Status_date_time" as timestamp) as status_date
    from {{ source('gdgs_25_surveys', 'approval_statuses') }}
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
            row_number() over (
                partition by entity_id
                order by status_date desc
            ) as row_num
        from converted_statuses
    ) as sub
    where row_num = 1
)

select * from latest_entries
