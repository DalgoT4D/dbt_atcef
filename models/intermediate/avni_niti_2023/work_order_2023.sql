{{ config(
  materialized='table'
) }}

WITH WorkOrderEncounters AS (
    SELECT * 
    FROM prod.encounter_2023 
)

SELECT a.*,
       woe.encounter_location,
       woe.total_silt_carted,
       woe.date_time


FROM prod.subjects_2023 AS a
RIGHT JOIN WorkOrderEncounters AS woe ON a.uid = woe.subject_id