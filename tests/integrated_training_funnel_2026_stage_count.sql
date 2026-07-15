with stage_count as (
    select count(*) as total_stages
    from {{ ref('integrated_training_funnel_year_stage_2026_prod') }}
)

select *
from stage_count
where total_stages != 5
