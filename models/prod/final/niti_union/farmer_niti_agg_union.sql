{{ config(
  materialized='table',
  tags=["final","final_niti_union"]
) }}


select 
    date_time,
    dam,
    state,
    district,
    taluka, 
    village,
    ngo_name,
    verified_farmers,
    unverified_farmers,
    total,
    vulnerable_marginal,
    vulnerable_small,
    semi_medium,
    medium,
    large,
    null as widow,
    null as disabled,
    null as family_of_farmer_who_committed_suicide,
    null as farmer_niti_22

from {{ref('farmer_agg_niti_23')}}

union all 

select 
    date_time,
    dam,
    state,
    district,
    taluka, 
    village,
    ngo_name,
    verified_farmers,
    unverified_farmers,
    total,
    null as vulnerable_marginal,
    null as vulnerable_small,
    null as semi_medium,
    null as medium,
    null as large,
    null as widow,
    null as disabled,
    null as family_of_farmer_who_committed_suicide,
    farmer_niti_2022

from {{ref('farmer_agg_niti_22')}}

union all 

select 
    date_time,
    dam,
    state,
    district,
    taluka, 
    village,
    ngo_name,
    verified_farmers,
    unverified_farmers,
    total,
    vulnerable_marginal,
    vulnerable_small,
    semi_medium,
    medium,
    large,
    null as widow,
    null as disabled,
    null as family_of_farmer_who_committed_suicide,
    null as farmer_niti_22

from {{ref('farmer_agg_niti_2024')}}