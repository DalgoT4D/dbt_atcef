{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('barren_fertile_silt_gramin') }}

union all

select * from {{ ref('barren_fertile_silt_gramin_25') }}

union all

select * from {{ ref('barren_fertile_silt_gramin_26') }}
