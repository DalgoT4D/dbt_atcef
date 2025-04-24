{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin_niti", "gramin_niti", "gramin_25"]
) }}

with farmers as (
    select
        subjects."ID" as farmer_id,
        subjects."Subject_type" as subject_type,
        subjects."Location_ID" as address_id,
        (
            subjects.observations -> 'Mobile Number' ->> 'verified'
        )::boolean as mobile_verified,
        subjects."Voided" as farmer_voided,
        INITCAP(TRIM(COALESCE(subjects.observations ->> 'First name'))) as farmer_name,
        rwb.dam as dam,
        rwb.district as district,
        case  -- Standardize state names
            when
                LOWER(rwb.state) like '%maharashtra%'
                then 'Maharashtra'
            when
                LOWER(rwb.state) like '%maharshatra%'
                then 'Maharashtra'
            else INITCAP(COALESCE(rwb.state, ''))
        end as state,
        rwb.taluka as taluka,
        rwb.village as village,
        subjects.observations ->> 'Category of farmer' as category_of_farmer,
        subjects.observations -> 'Mobile Number' ->> 'phoneNumber' as mobile_number
    from
        {{ source('source_gramin_25', 'subjects_gramin_25') }} subjects
    LEFT JOIN
        {{ ref('address_gramin_25') }} AS rwb
        ON
            rwb.address_id = subjects."Location_ID"
    where
        "Subject_type" = 'Farmer'
)

{{ dbt_utils.deduplicate(
      relation='farmers',
      partition_by='farmer_id',
      order_by='farmer_id desc'
) }}
