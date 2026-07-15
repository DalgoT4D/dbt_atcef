with ordered_stages as (
    select
        stage_order,
        funnel_contact_count,
        lag(funnel_contact_count) over (order by stage_order) as previous_funnel_contact_count
    from {{ ref('integrated_training_funnel_year_stage_2026_prod') }}
)

select *
from ordered_stages
where previous_funnel_contact_count is not null
  and funnel_contact_count > previous_funnel_contact_count
