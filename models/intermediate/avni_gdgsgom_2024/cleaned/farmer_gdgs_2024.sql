{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2024", "gdgs_2024", "gdgs"]
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
        {{ source('source_gdgsom_surveys', 'subjects_2024') }}
    where
        "Subject_type" = 'Farmer'
        and "Voided" = 'False'
        and not (LOWER(location ->> 'Dam') ~ 'voided')
),

approval_farmers as (
    select
        d.*,
        a.approval_status as farmer_approval_status
    from mycte as d
    inner join {{ ref('approval_statuses_gdgs_24') }} as a
        on
            d.farmer_id = a.entity_id
            and a.entity_type = 'Subject'
    where
        a.approval_status = 'Approved'
)

{{ dbt_utils.deduplicate(
      relation='approval_farmers',
      partition_by='farmer_id',
      order_by='farmer_id desc'
) }}
