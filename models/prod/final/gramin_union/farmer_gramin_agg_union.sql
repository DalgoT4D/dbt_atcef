{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('farmer_agg_gramin') }}

union all

select * from {{ ref('farmer_agg_gramin_25') }}

union all

select * from {{ ref('farmer_agg_gramin_26') }}
