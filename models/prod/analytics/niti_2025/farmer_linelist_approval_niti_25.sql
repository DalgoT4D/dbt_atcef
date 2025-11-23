{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

with newtab as 
    (select
    s."ID" as farmer_id,
    INITCAP(TRIM(COALESCE(s.observations ->> 'First name'))) as farmer_name,
    INITCAP(COALESCE(rwb.dam, '')) as dam,
    INITCAP(COALESCE(rwb.district, '')) as district,
    case  -- Standardize state names
        when
        LOWER(rwb.state) like '%maharashtra%'
            then 'Maharashtra'
        when
        LOWER(rwb.state) like '%maharshatra%'
            then 'Maharashtra'
        else INITCAP(COALESCE(rwb.state, '')) end as state,
    INITCAP(COALESCE(rwb.taluka)) as taluka,
    INITCAP(COALESCE(rwb.village)) as village,
    s.observations ->> 'Category of farmer' as category_of_farmer,
    s.observations -> 'Mobile Number' ->> 'phoneNumber' as mobile_number,
    s.observations -> 'Land holding' as land_holding,
    e.total_silt_carted,
    e.total_trolleys_carted,
    e.total_hyvasdumper_carted,
    e.area_covered_by_silt

    from
        {{ source('rwb_niti_2025', 'subjects_niti_2025') }} as s
    LEFT JOIN
        {{ ref('address_niti_2025') }} AS rwb
        ON
            s."Location_ID" = rwb.address_id
    LEFT JOIN 
        {{ ref('farmer_linelist_niti_25') }} AS e
        ON
            s."ID" = e.farmer_id
    where
        s."Subject_type" = 'Farmer'
        and s."Voided" = False
),

approval_farmers as (
    select
        d.*,
        a.approval_status as farmer_approval_status
    from newtab as d
    left join {{ ref('approval_status_niti_2025') }} as a
        on
            d.farmer_id = a.entity_id
            and a.entity_type = 'Subject'
)

{{ dbt_utils.deduplicate(
      relation='approval_farmers',
      partition_by='farmer_id',
      order_by='farmer_id desc'
) }}
