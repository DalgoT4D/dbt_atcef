{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2022", "niti_2022", "niti"]
) }}

with cte as (
    select
        s."ID" as uid, -- Replace 'id' with the actual unique identifier column name if different
        NULL as category_of_farmer,
        (
            s.observations -> 'Mobile Number' ->> 'verified'
        )::boolean as mobile_verified,
        (
            COALESCE(NULLIF(rwb."Estimated quantity of Silt", ''), '0')
        )::numeric as silt_target,
        s."Voided" as subject_voided,
        s.observations ->> 'First name' as first_name,
        s.location ->> 'Dam' as dam,
        s.location ->> 'District' as district,
        s.location ->> 'State' as state,
        s.location ->> 'Taluka' as taluka,
        s.location ->> 'GP/Village' as village,
        s.observations ->> 'Type of Machine' as type_of_machine,
        s.observations -> 'Mobile Number' ->> 'phoneNumber' as mobile_number,
        COALESCE(NULLIF(rwb."Project/NGO", ''), 'Unknown') as ngo_name
    from rwb_niti_2022.rwbniti22 as rwb
    left join {{ source('source_atecf_surveyss', 'subjects_2022') }} as s
        on rwb."Dam" = s.location ->> 'Dam'
    where not (LOWER(s.location ->> 'Dam') ~ 'voided')
),

removing_nulls as (
    select * from cte
    where
        dam is not NULL
        and district is not NULL
        and taluka is not NULL
        and state is not NULL
        and village is not NULL
        and subject_voided = 'false'
),

approved_subjects as (
    select r.*
    from removing_nulls as r
    inner join {{ ref('approval_statuses_niti_2022') }} as a
        on r.uid = a.entity_id
    where a.entity_type = 'Subject' and a.approval_status = 'Approved'
),

deduplicated as ({{ dbt_utils.deduplicate(
    relation='approved_subjects',
    partition_by='uid',
    order_by='uid desc',
   )
}}
)

select *
from deduplicated
