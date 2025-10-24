{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(date)',
    order_by=['property_id']
) }}

SELECT *
FROM {{ ref('stg_dim_property') }}