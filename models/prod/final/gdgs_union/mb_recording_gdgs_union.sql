{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}


with cte as (
    select
        state,
        district,
        taluka,
        dam,
        ngo_name,
        village,
        max(date_time) as date_time,
        sum(
            coalesce(silt_excavated_as_per_mb_recording, 0)
        ) as silt_excavated_as_per_mb_recording,
        max(
            coalesce(silt_to_be_excavated_as_per_plan, 0)
        ) as total_silt_excavated
    from
        {{ ref('work_order_gdgs_union') }}
    group by
        state,
        district,
        taluka,
        dam,
        ngo_name,
        village
)

select * from cte
where total_silt_excavated != 0
