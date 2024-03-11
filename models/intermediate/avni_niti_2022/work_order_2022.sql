{{ config(
  materialized='table'
) }}



WITH WorkOrderEncounters AS (
    SELECT * 
    FROM prod.encounter_2022
    WHERE subject_type = 'Work Order'
)

SELECT a.*,
       woe.encounter_location,
       woe.total_silt_carted,
       woe.date_time


FROM prod.subjects_2022 AS a
RIGHT JOIN WorkOrderEncounters AS woe ON a.uid = woe.subject_id