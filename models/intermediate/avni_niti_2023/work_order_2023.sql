{{ config(
  materialized='table'
) }}

SELECT a.*, 
       b."Total Silt Excavated",
       b."Total Silt Carted"
FROM prod.subjects_2023 as a
RIGHT JOIN prod.encounter_2023 AS b ON a.uid = b."Subject_ID"
