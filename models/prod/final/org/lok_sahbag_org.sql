{{ config(
  materialized='table'
) }}

select *, 'GDGS' AS "project" 
from {{ref('lok_sahbag_gdgs_union')}} 