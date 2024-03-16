{{ config(
  materialized='table'
) }}


with cte as (SELECT
	*
FROM
	{{ ref('work_order') }} as a
	RIGHT JOIN public.fips_geojson as b ON a.district = b.dtname)

SELECT * from cte where district is not null