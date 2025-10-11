-- Create databases
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS mart;

-- Raw, one big table
CREATE TABLE IF NOT EXISTS raw.realestate_housing_raw (
    date Date,
    property_id String,
    state LowCardinality(String),
    city String,
    neighborhood Nullable(String),
    zip String,
    price UInt32,
    status Nullable(String),
    property_type LowCardinality(String),
    lat Float64,
    long Float64,
    url String,
    full_address Nullable(String),
    has_cooling Bool,
    has_heating Bool,
    story_count Nullable(Float32),
    level_count Nullable(String),
    has_garage Bool,
    has_attached_garage Bool,
    has_carport Bool,
    garage_parking_count UInt8,
    carport_parking_count UInt8,
    covered_parking_count UInt8,
    days_on_market UInt16,
    year_built Nullable(UInt16),
    HOA_monthly Nullable(String),
    lot_area Nullable(String),
    bed_count Nullable(String),
    bath_count Nullable(String),
    floorSpace Nullable(String),
    tax_year Nullable(UInt16),
    tax_amount Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (property_id);