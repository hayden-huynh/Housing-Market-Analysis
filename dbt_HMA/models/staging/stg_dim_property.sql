{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=["property_id"]
) }}

SELECT
    date,
    toUInt64(property_id) as property_id,
    state,
    city,
    neighborhood,
    zip,
    property_type,
    lat,
    long,
    url,
    full_address,
    has_cooling,
    has_heating,
    story_count,
    level_count,
    has_garage,
    has_attached_garage,
    has_carport,
    garage_parking_count,
    carport_parking_count,
    covered_parking_count,
    days_on_market,
    year_built,
    {{ normalize_hoa('HOA_monthly') }} as HOA_monthly,
    {{ normalize_area('lot_area') }} as lot_area_sqft,
    {{ normalize_bed_bath('bed_count') }} as bed_count,
    {{ normalize_bed_bath('bath_count') }} as bath_count,
    {{ normalize_area('floorSpace') }} as floor_space_sqft,
    tax_year,
    case
        when tax_amount is NULL then NULL
        else round(toFloat32OrZero(replaceRegexpAll(tax_amount, '[^0-9.]', '')), 2)
    end as tax_amount
FROM {{ source('raw', 'realestate_housing_raw') }}