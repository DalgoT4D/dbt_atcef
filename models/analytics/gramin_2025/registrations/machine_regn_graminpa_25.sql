-- Machine registration table with cleaned identifiers, location, and approval metadata for graminpa 2025.
{{ config(
  materialized='table',
    tags=["analytics","analytics_gramin_2025", "analytics_graminpa_2025", "registrations_graminpa_2025"]
) }}

-- SELECT 
--   m.subject_id,
--   m.registration_date,
--   m.subject_type,
--   m.location_id,
--   -- m.ngo_name as stakeholder_responsible,
--   UPPER(REPLACE(REPLACE(REGEXP_REPLACE(
--                 COALESCE(m.machine_name::TEXT, ''),  -- 1. Cast to TEXT for string operations
--                 '[-\s]+', -- 2. Target ONE OR MORE hyphens (-) or whitespace characters (\s)
--                 '',
--                 'g'
--             ),CHR(9), 
--             ''),CHR(160),''))::VARCHAR AS machine_name,
--   m.machine_type,
--   m.contractor_name,
--   m.contractor_mobile_number,
--   m.voided,
--   l.*,
--   a.approval_status,
--   a.status_date_time as approval_date_time


-- FROM 
-- {{ ref('dim_subjects_machine_graminpa_25') }} AS m
-- LEFT JOIN 
-- {{ ref('location_graminpa_25') }} AS l
--     ON m.location_id = l.address_id
-- LEFT JOIN 
-- {{ ref('approval_status_graminpa_25') }} AS a
--     ON m.subject_id = a.entity_id

-- WHERE m.voided != TRUE


WITH m_ranked AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (
            PARTITION BY m.subject_id
            ORDER BY m.registration_date DESC
        ) AS rn
    FROM {{ ref('dim_subjects_machine_graminpa_25') }} m
    WHERE m.voided != TRUE
),

m_dedup AS (
    SELECT
        *
    FROM m_ranked
    WHERE rn = 1
)


SELECT 
  m.subject_id,
  m.registration_date,
  m.subject_type,
  m.location_id,
  UPPER(
    REPLACE(
      REPLACE(
        REGEXP_REPLACE(COALESCE(m.machine_name::TEXT, ''), '[-\s]+', '', 'g'),
        CHR(9), ''
      ),
      CHR(160), ''
    )
  )::VARCHAR AS machine_name,
  m.machine_type,
  m.contractor_name,
  m.contractor_mobile_number,
  m.voided,
  l.*,
  a.approval_status,
  a.status_date_time AS approval_date_time
FROM m_dedup m

LEFT JOIN (
    SELECT DISTINCT ON (address_id)
        *
    FROM {{ ref('location_graminpa_25') }}
    ORDER BY address_id
) l
    ON m.location_id = l.address_id

LEFT JOIN (
    SELECT DISTINCT ON (entity_id)
        entity_id,
        approval_status,
        status_date_time
    FROM {{ ref('approval_status_graminpa_25') }}
    ORDER BY entity_id, status_date_time DESC
) a
    ON m.subject_id = a.entity_id

WHERE m.voided != TRUE


