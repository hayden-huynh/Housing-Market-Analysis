{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by=['date', 'property_id']
) }}

SELECT *
FROM {{ ref('stg_fact_listing') }}