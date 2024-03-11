{{ config(
  materialized='table'
) }}

WITH total_silt_excavated AS (
  SELECT
    dam,
    SUM(total_silt_carted) AS total_silt_excavated
  FROM prod.work_order_union
  GROUP BY dam
),
silt_target AS (
  SELECT DISTINCT
    dam,
    silt_to_be_excavated AS silt_target
  FROM prod.work_order_union
),
latest_details AS (
  SELECT
    dam,
    district,
    state,
    taluka,
    date_time,
    ROW_NUMBER() OVER(PARTITION BY dam ORDER BY date_time DESC) as rn
  FROM prod.work_order_union
)
SELECT
  ld.date_time AS "Date",
  ld.dam AS "Dam Name",
  ld.district AS "District",
  ld.state AS "State",
  ld.taluka AS "Taluka",
  tse.total_silt_excavated AS "Total Silt Excavated",
  st.silt_target AS "Silt Target"
FROM total_silt_excavated tse
JOIN silt_target st ON tse.dam = st.dam
JOIN latest_details ld ON tse.dam = ld.dam AND ld.rn = 1
