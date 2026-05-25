{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('silt_per_acre_gramin') }}

union all

select * from {{ ref('silt_per_acre_gramin_25') }}

union all

select * from {{ ref('silt_per_acre_gramin_26') }}
