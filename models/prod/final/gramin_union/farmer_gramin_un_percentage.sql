{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('farmer_gramin_percentage') }}

union all

select * from {{ ref('farmer_gramin_percentage_25') }}

union all

select * from {{ ref('farmer_gramin_percentage_26') }}
