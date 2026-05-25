{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('machine_gramin_metric') }}

union all

select * from {{ ref('machine_gramin_metric_25') }}

union all

select * from {{ ref('machine_gramin_metric_26') }}
