{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=["property_id"]
) }}

SELECT date, property_id, price, status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY date DESC) rn
    FROM {{ ref('mart_fact_listing') }}
)
WHERE rn = 1