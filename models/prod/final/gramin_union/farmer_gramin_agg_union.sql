{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['dam', 'date_time', 'state', 'district', 'taluka', 'village', 'ngo_name', 'verified_farmers', 'unverified_farmers', 'total', 'vulnerable_marginal', 'vulnerable_small', 'semi_medium', 'medium', 'large'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_agg_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_agg_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_farmer_agg_graminpa_26') }}
