from datetime import datetime
import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PWD = os.getenv("CLICKHOUSE_PWD")
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PWD = os.getenv("MINIO_PWD")


def main_load():
    clickhouse_client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PWD,
    )

    clickhouse_client.command(
        """
        TRUNCATE TABLE raw.realestate_housing_raw
        """
    )

    clickhouse_client.command(
        f"""
        INSERT INTO raw.realestate_housing_raw
        SELECT *
        FROM s3('http://minio-server:9000/real-estate-properties/{datetime.now().strftime("%Y-%m-%d")}/*.json', '{MINIO_USER}', '{MINIO_PWD}', 'JSONEachRow', 'date Date, property_id String, state LowCardinality(String), city String, neighborhood Nullable(String), zip String, price UInt32, status Nullable(String), property_type LowCardinality(String), lat Float64, long Float64, url String, full_address Nullable(String), has_cooling Bool, has_heating Bool, story_count Nullable(Float32), level_count Nullable(String), has_garage Bool, has_attached_garage Bool, has_carport Bool, garage_parking_count UInt8, carport_parking_count UInt8, covered_parking_count UInt8, days_on_market UInt16, year_built Nullable(UInt16), HOA_monthly Nullable(String), lot_area Nullable(String), bed_count Nullable(String), bath_count Nullable(String), floorSpace Nullable(String), tax_year Nullable(UInt16), tax_amount Nullable(String)')
        """
    )


if __name__ == "__main__":
    main_load()
