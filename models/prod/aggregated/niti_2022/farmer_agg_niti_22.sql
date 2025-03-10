{{ config(
  materialized='table',
  tags=["aggregated","aggregated_niti_2022", "niti_2022", "niti"]
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
    from {{ ref('farmer_calc_silt_niti_22') }}
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
        case when category_of_farmer = 'farmer_niti_2022' then 1 end
    ) as farmer_niti_2022
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
