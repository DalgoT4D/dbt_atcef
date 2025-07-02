{{ config(
    materialized='table',
    tags=['glific', 'prod']
) }}

WITH flow_completions AS (
    SELECT 
        contact_id,
        -- Track AVNI Gramin Training completion
        MAX(CASE 
            WHEN flow_uuid = 'a0620d48-b427-4efe-870d-410051ef47c8' THEN 1 
            ELSE 0 
        END) as avni_gramin_initiated_flow,
        -- Track RWB Training completion
        MAX(CASE 
            WHEN flow_uuid = '21b8db12-473e-403c-8daf-76575ffc2a66' THEN 1 
            ELSE 0 
        END) as rwb_training_initiated_flow
    FROM {{ ref('flow_contexts_int') }}
    GROUP BY contact_id
)

SELECT 
    fc.*,
    -- Add flag for contacts who completed both trainings
    CASE 
        WHEN fc.avni_gramin_initiated_flow = 1 
        AND fc.rwb_training_initiated_flow = 1 THEN 1
        ELSE 0
    END as completed_both_trainings
FROM flow_completions fc 