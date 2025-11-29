{{ config(
    materialized="table",
    engine="MergeTree",
    order_by=["property_id"]
) }}

SELECT
    date,
    toUInt64(property_id) as property_id,
    price,
    status
FROM {{ source('raw', 'realestate_housing_raw') }}