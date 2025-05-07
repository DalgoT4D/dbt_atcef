{{ config(
  materialized='table',
  tags=["aggregated","aggregated_gdgs_2025", "gdgs_2025", "gdgs"]
) }}

with cte as (
    select
        farmer_id,
        state,
        district,
        village,
        taluka,
        dam,
        ngo_name,
        farmer_name,
        mobile_number,
        mobile_verified,
        category_of_farmer,
        max(date_time) as date_time
    from {{ ref('farmer_calc_silt_gdgs_25') }}
    group by
        farmer_id,
        state,
        district,
        village,
        taluka,
        dam,
        ngo_name,
        farmer_name,
        mobile_number,
        mobile_verified,
        category_of_farmer
)


select
    dam,
    date_time,
    state,
    district,
    taluka,
    village,
    ngo_name,
    sum(case
        when mobile_verified = 'True' then 1
        else 0
    end) as verified_farmers,
    sum(case
        when mobile_verified = 'False' then 1
        else 0
    end) as unverified_farmers,
    count(*) as total,
    count(
        case when category_of_farmer = 'Marginal (0-2.49 acres)' then 1 end
    ) as vulnerable_marginal,
    count(
        case when category_of_farmer = 'Small (2.5-4.99 acres)' then 1 end
    ) as vulnerable_small,
    count(
        case when category_of_farmer = 'Semi-medium (5 to 9.99 acre)' then 1 end
    ) as semi_medium,
    count(
        case when category_of_farmer = 'Medium (10-24.99 acres)' then 1 end
    ) as medium,
    count(
        case when category_of_farmer = 'Large (above 25 acres)' then 1 end
    ) as large,
    count(case when category_of_farmer = 'Widow' then 1 end) as widow,
    count(case when category_of_farmer = 'Disabled' then 1 end) as disabled,
    count(
        case
            when
                category_of_farmer = 'Family of farmer who committed suicide'
                then 1
        end
    ) as family_of_farmer_who_committed_suicide
from
    cte
group by
    date_time,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name
