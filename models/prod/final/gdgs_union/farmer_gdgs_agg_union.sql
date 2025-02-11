{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}


select 
    date_time,
    dam,
    ngo_name,
    state,
    district,
    taluka, 
    village,
    verified_farmers,
    unverified_farmers,
    total,
    vulnerable_marginal,
    vulnerable_small,
    semi_medium,
    medium,
    large,
    widow,
    disabled,
    family_of_farmer_who_committed_suicide

from {{ref('farmer_agg_gdgs_23')}}

union all 

select 
    date_time,
    dam,
    ngo_name,
    state,
    district,
    taluka, 
    village,
    verified_farmers,
    unverified_farmers,
    total,
    vulnerable_marginal,
    vulnerable_small,
    semi_medium,
    medium,
    large,
    widow,
    disabled,
    family_of_farmer_who_committed_suicide

from {{ref('farmer_agg_gdgs_24')}}

