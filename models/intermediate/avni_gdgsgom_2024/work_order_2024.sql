{{ config(
  materialized='table'
) }}


select * FROM {{ ref('subjects_2024') }} as s
LEFT JOIN {{ ref('encounters_2024') }} AS woe ON s.uid = woe.subject_id 