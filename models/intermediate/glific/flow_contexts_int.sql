{{ config(
    materialized='table',
    tags=['glific', 'intermediate']
) }}

WITH source_data AS (
    SELECT 
        {{ dbt_utils.star(
            from=source('staging_glific', 'flow_contexts'),
            except=[
                'reason',
                'bq_uuid',
                'flow_id',
                'results',
                'flow_name',
                'is_killed',
                'node_uuid',
                'parent_id',
                'wakeup_at',
                'profile_id',
                'wa_group_id',
                'completed_at',
                'contact_phone',
                'wa_group_name',
                'bq_inserted_at',
                'recent_inbound',
                'is_await_result',
                'recent_outbound',
                'wa_group_bsp_id',
                'flow_broadcast_id',
                'is_background_flow',
                'message_broadcast_id',
                '_airbyte_raw_id',
                '_airbyte_extracted_at',
                '_airbyte_meta',
                'inserted_at',
                'updated_at'
            ])
        }}
    FROM {{ source('staging_glific', 'flow_contexts') }}
)

SELECT 
    source_data.*,
    CAST(s.inserted_at AS TIMESTAMPTZ) as inserted_at,
    CAST(s.updated_at AS TIMESTAMPTZ) as updated_at
FROM source_data
LEFT JOIN {{ source('staging_glific', 'flow_contexts') }} s 
    ON source_data.id = s.id 