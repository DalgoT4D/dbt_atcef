{{ config(
  materialized='table',
  tags=["aggregated","aggregated_niti_2024", "niti_2024", "niti"]
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
    from {{ ref('farmer_calc_silt_niti_2024') }}
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
        case when category_of_farmer = 'Semi-medium (5-9.99 acres)' then 1 end
    ) as semi_medium,
    count(
        case when category_of_farmer = 'Medium (10-24.99 acres)' then 1 end
    ) as medium,
    count(
        case when category_of_farmer = 'Large (25+ acres)' then 1 end
    ) as large
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
