{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('work_order_silt_calc') }}

union all

select * from {{ ref('work_order_silt_calc_25') }}

union all

select * from {{ ref('work_order_silt_calc_26') }}
