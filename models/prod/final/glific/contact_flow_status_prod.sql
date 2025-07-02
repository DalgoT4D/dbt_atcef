{{ config(
    materialized='table',
    tags=['glific', 'prod']
) }}

WITH contact_flows AS (
    SELECT 
        c.contact_id,
        c.phone,
        c.inserted_date,
        c.updated_date,
        c.username_lms,
        SPLIT_PART(c.username_lms, '@', 2) AS project,
        -- Get training status from flow_contexts_prod
        COALESCE(fcp.avni_gramin_initiated_flow, 0) as avni_gramin_initiated_flow,
        COALESCE(fcp.rwb_training_initiated_flow, 0) as rwb_training_initiated_flow,
        COALESCE(fcp.completed_both_trainings, 0) as completed_both_trainings
    FROM {{ ref('contacts_field_flatten_int') }} c
    LEFT JOIN {{ ref('flow_contexts_int') }} fc 
        ON CAST(c.contact_id AS VARCHAR) = CAST(fc.contact_id AS VARCHAR)
    LEFT JOIN {{ ref('flow_contexts_prod') }} fcp
        ON CAST(c.contact_id AS VARCHAR) = CAST(fcp.contact_id AS VARCHAR)
    GROUP BY 
        c.contact_id,
        c.phone,
        c.inserted_date,
        c.updated_date,
        c.username_lms,
        SPLIT_PART(c.username_lms, '@', 2),
        fcp.avni_gramin_initiated_flow,
        fcp.rwb_training_initiated_flow,
        fcp.completed_both_trainings
)

SELECT 
    cf.*,
    -- Add training completion status from contacts_field_flatten_int
    CASE 
        WHEN cff.is_avni_training_done = 'y' THEN 1
        ELSE 0
    END as avni_training_completed,
    CASE 
        WHEN cff.is_rwb_training_done = 'y' THEN 1
        ELSE 0
    END as rwb_training_completed
FROM contact_flows cf
LEFT JOIN {{ ref('contacts_field_flatten_int') }} cff 
    ON cf.contact_id = cff.contact_id 