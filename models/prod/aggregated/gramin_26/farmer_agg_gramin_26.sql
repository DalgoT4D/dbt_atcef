{{ config(
  materialized='table',
  tags=["aggregated","aggregated_gramin_niti", "gramin_niti", "gramin_26"]
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
    from {{ ref('farmer_calc_silt_gramin_26') }}
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
        case when category_of_farmer = 'Marginal: 0-2.47 acres' then 1 end
    ) as vulnerable_marginal,
    count(
        case when category_of_farmer = 'Small: 2.48 to 4.94 acres' then 1 end
    ) as vulnerable_small,
    count(
        case when category_of_farmer = 'Semi Medium: 4.95 to 9.88 acres' then 1 end
    ) as semi_medium,
    count(
        case when category_of_farmer = 'Medium: 9.89 acres to 24.7 acres' then 1 end
    ) as medium,
    count(
        case when category_of_farmer = 'Large: Above 24.7 acres' then 1 end
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
