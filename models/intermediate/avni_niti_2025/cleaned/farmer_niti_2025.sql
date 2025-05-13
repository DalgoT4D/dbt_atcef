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
        INITCAP(COALESCE(rwb.dam, '')) as dam,
        INITCAP(COALESCE(rwb.district, '')) as district,
        case  -- Standardize state names
            when
                LOWER(rwb.state) like '%maharashtra%'
                then 'Maharashtra'
            when
                LOWER(rwb.state) like '%maharshatra%'
                then 'Maharashtra'
            else INITCAP(COALESCE(rwb.state, ''))
        end as state,
        INITCAP(COALESCE(rwb.taluka)) as taluka,
        INITCAP(COALESCE(rwb.village)) as village,
        observations ->> 'Category of farmer' as category_of_farmer,
        observations -> 'Mobile Number' ->> 'phoneNumber' as mobile_number
    from
        {{ source('rwb_niti_2025', 'subjects_niti_2025') }}
    LEFT JOIN
        {{ ref('address_niti_2025') }} AS rwb
        ON
            location ->> 'Nalla' = rwb.dam
    where
        "Subject_type" = 'Farmer'
        and "Voided" = 'False'
),

approval_farmers as (
    select
        d.*,
        a.approval_status as farmer_approval_status
    from mycte as d
    inner join {{ ref('approval_status_niti_2025') }} as a
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
