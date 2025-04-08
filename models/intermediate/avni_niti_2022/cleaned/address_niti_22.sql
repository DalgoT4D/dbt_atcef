{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2022", "niti_2022", "niti"]
) }}


with cte as (
    select
        "ID" as address_id,
        "Title" as dam,
        "customProperties" ->> 'Estimated quantity of Silt' as silt_target,
        "customProperties"
        ->> 'Stakeholder responsible' as stakeholder_responsible
    from
        {{ source('source_atecf_surveyss', 'address_niti_2022') }}
    where
        "Title" not like '%(voided~%'
)

select * from cte
