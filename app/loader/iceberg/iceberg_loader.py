from app.loader import Loader
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, TimestampType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import HourTransform


WAREHOUSE_LOC = "s3://iceberg-shkim/iceberg/glue_catalog/tables/"

class IcebergLoader(Loader):
    
    def _init_catalog(cls):
        cls.catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "warehouse": "s3://iceberg-shkim/iceberg/glue_catalog/tables/",
                # "glue.endpoint": "https://glue.us-east-1.amazonaws.com", # auto making
                "glue.region": "us-east-1",
                "s3.region": "us-east-1",
                "downcast-ns-timestamp-to-us-on-write": True # not support timestamp[ns] on iceberg
            }
        )
        
    def get_schema(self):
        # 스키마 정의
        return Schema(
            NestedField(1, "hash", StringType()),
            NestedField(2, "difficulty", DoubleType()),
            NestedField(3, "time", TimestampType())
        )

    def get_hourly_partition(self, source_id):
        # 파티션 정의 (시간 단위)
        return PartitionSpec(
            PartitionField(source_id=source_id, field_id=1000, transform=HourTransform(), name="hour")
        )


    def load(cls, db_name, table_name, schema, partition):
        cls.catalog.create_table_if_not_exists(
            identifier=f"{db_name}.{table_name}",
            schema=schema,
            partition_spec=partition,
            location=f"{WAREHOUSE_LOC}{table_name}",
            properties={
                "write.delete.mode": "copy-on-write",
                "write.update.mode": "copy-on-write",
                "write.merge.mode": "copy-on-write",
                "write.target-file-size-bytes": "134217728",  # 128MB
            }
        )