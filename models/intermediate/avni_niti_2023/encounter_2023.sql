{{ config(
  materialized='table'
) }}


SELECT
 "ID" as eid,
 "Subject_ID",
  observations ->> 'Total silt excavated' AS "Total Silt Excavated",
  observations ->> 'Total Silt carted' AS "Total Silt Carted",
  observations ->> 'Silt excavated as per MB recording' AS "Silt Excavated as Per MB Recording",
  observations ->> 'The total farm area on which Silt is spread' AS "Total Farm Area on Which Silt Is Spread"
FROM staging.encounters
