from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, TimestampType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import HourTransform
import pandas as pd
import pyarrow as pa
import json
import requests

WAREHOUSE_LOC = "s3://iceberg-shkim/iceberg/glue_catalog/tables/"


def get_catalog():
    # Glue Catalog 설정
    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "warehouse": WAREHOUSE_LOC,
            # "glue.endpoint": "https://glue.us-east-1.amazonaws.com", 생략 가능
            "glue.region": "us-east-1"
        }
    )

def get_schema():
    # 스키마 정의
    return Schema(
        NestedField(1, "hash", StringType()),
        NestedField(2, "difficulty", DoubleType()),
        NestedField(3, "time", TimestampType())
    )


def get_hourly_partition(source_id):
    # 파티션 정의 (시간 단위)
    return PartitionSpec(
        PartitionField(source_id=source_id, field_id=1000, transform=HourTransform(), name="hour")
    )

def create_iceberg_table(catalog, schema, partition, db_name, table_name):
    # 테이블 생성
    return catalog.create_table(
        identifier=f"{db_name}.{table_name}",
        schema=schema,
        partition_spec=partition,
        location=f"{WAREHOUSE_LOC}{table_name}",
        properties={
            "write.delete.mode": "copy-on-write",
            "write.update.mode": "copy-on-write",
            "write.merge.mode": "copy-on-write",
            "write.target-file-size-bytes": "134217728"  # 128MB
        }
    )

iceberg_catalog = get_catalog()
iceberg_table = create_iceberg_table(catalog=iceberg_catalog, schema=get_schema(), partition=get_hourly_partition, db_name="default", table_name="tmp")
print(f"Table created: {iceberg_table._identifier}")


def load_iceberg_table(db_name, table_name):
    return iceberg_catalog.load_table(f"{db_name}.{table_name}")

load_iceberg_table(db_name="default", table_name="tmp")
