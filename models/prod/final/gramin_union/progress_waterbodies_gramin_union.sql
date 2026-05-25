{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select * from {{ ref('progress_waterbodies_gramin') }}

union all

select * from {{ ref('progress_waterbodies_gramin_25') }}

union all

select * from {{ ref('progress_waterbodies_gramin_26') }}
