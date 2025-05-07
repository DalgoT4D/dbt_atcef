{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2025", "niti_2025", "niti"]
) }}

with mycte as (
    select
        "ID" as farmer_id,
        "Subject_type" as subject_type,
        (
            observations -> 'Mobile Number' ->> 'verified'
        )::boolean as mobile_verified,
        "Voided" as farmer_voided,
        INITCAP(TRIM(COALESCE(observations ->> 'First name'))) as farmer_name,
        INITCAP(COALESCE(location ->> 'Dam')) as dam,
        INITCAP(COALESCE(location ->> 'District')) as district,
        case  -- Standardize state names
            when
                LOWER(location ->> 'State') like '%maharashtra%'
                then 'Maharashtra'
            when
                LOWER(location ->> 'State') like '%maharshatra%'
                then 'Maharashtra'
            else INITCAP(COALESCE(location ->> 'State', ''))
        end as state,
        INITCAP(COALESCE(location ->> 'Taluka')) as taluka,
        INITCAP(COALESCE(location ->> 'GP/Village')) as village,
        observations ->> 'Category of farmer' as category_of_farmer,
        observations -> 'Mobile Number' ->> 'phoneNumber' as mobile_number
    from
        {{ source('rwb_niti_2024', 'subjects_niti_2025') }}
    where
        "Subject_type" = 'Farmer'
        and "Voided" = 'False'
        and not (LOWER(location ->> 'Dam') ~ 'voided')
)

{{ dbt_utils.deduplicate(
      relation='mycte',
      partition_by='farmer_id',
      order_by='farmer_id desc'
) }}
