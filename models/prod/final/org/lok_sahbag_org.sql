{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

select *, 'GDGS' AS "project" 
from {{ref('lok_sahbag_gdgs_union')}} 