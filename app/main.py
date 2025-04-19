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

btc_qn_url = 'https://restless-burned-grass.btc.quiknode.pro/933b0c939b6ef928c0c4cf1f3d1f700873518147/'
param_form = {"method": "", "params": [], "id": 1, "jsonrpc": "2.0"}
param_form["method"] = "getbestblockhash"

param_form["method"] = "getblock"
param_form = {"method": "getblock", "params": ['0000000000000000000057aac5f21e20cd1866e9fb9b632e4f0b715d3df4ee55', 3]}
# API에서 JSON 데이터 가져오기
try:
    response = requests.post(url=btc_qn_url, data=json.dumps(param_form))
    response.raise_for_status()
    json_data = response.json()  # 리스트: [{"id": "record1", "value": 100.0, "timestamp": "2025-04-08 13:01:00"}, ...]
except requests.RequestException as e:
    print(f"API error: {e}")
    exit(1)

# pandas DataFrame으로 변환
df = pd.DataFrame(json_data['result'])

# 데이터 타입 조정
df["time"] = pd.to_datetime(df["time"], unit="s").astype("datetime64[us]")
df["difficulty"] = df["difficulty"]
df["hash"] = df["hash"]

arrow_table = pa.Table.from_pandas(df[["hash", "difficulty", "time"]])
print(arrow_table)
# Iceberg 테이블에 append
try:
    table.append(arrow_table)
    print(f"Appended {len(df)} records to db.my_table")
except Exception as e:
    print(f"Append error: {e}")
    exit(1)

# 메타데이터 확인 (선택적)
snapshots = table.metadata.snapshots
print(f"Current snapshots: {len(snapshots)}")