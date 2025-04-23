import yaml
from dataclasses import dataclass, fields
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.types import LongType, TimestampType, StringType, IntegerType, BooleanType, FloatType, DecimalType
from pyiceberg.schema import NestedField
from pyiceberg.transforms import HourTransform, DayTransform, IdentityTransform
from decimal import Decimal


class IcebergHandler:
    def __init__(self, config: dict):
        self.config = config
        self._init_catalog()

    def _init_catalog(self):
        # _catalog_config = self.config["catalog"]
        _catalog_config = self.config
        _catalog_config["downcast-ns-timestamp-to-us-on-write"] = True # not support timestamp[ns] on iceberg
        self.warehouse_loc = _catalog_config["warehouse"]
        self.catalog = load_catalog(**_catalog_config)
        
    # 2. 동적으로 Schema 만들기
    def create_schema_from_dataclass(self, dataclass_instance) -> Schema:
        # 데이터 클래스의 필드를 가져옴
        schema_fields = []
        
        # 각 필드에 대해 적절한 타입을 매핑하여 schema를 생성
        for _idx, field in enumerate(fields(dataclass_instance)):
            if field.type == str:
                schema_fields.append(NestedField(_idx, field.name, StringType()))
            elif field.type == int:
                schema_fields.append(NestedField(_idx, field.name, IntegerType()))
            elif field.type == float:
                schema_fields.append(NestedField(_idx, field.name, FloatType()))
            elif field.type == bool:
                schema_fields.append(NestedField(_idx, field.name, BooleanType()))
            elif field.type == datetime:
                schema_fields.append(NestedField(_idx, field.name, TimestampType()))
            elif field.type == Decimal:
                # DecimalType은 Precision과 Scale을 지정해야 함
                schema_fields.append(NestedField(_idx, field.name, DecimalType(precision=18, scale=8)))  # 예: Precision=10, Scale=2
            # 추가적으로 다른 타입을 여기에 추가할 수 있음
            else:
                raise ValueError(f"Invalid field type [{field.type}]")
                

        # 필드들을 이용해 Schema 객체 생성
        return Schema(*schema_fields)

    def _get_schema_field(self, field_id: int, field_name: str, field_type: str):
        if field_type == 'int':
            return NestedField(field_id=field_id, name=field_name, field_type=IntegerType())
        elif field_type == 'long':
            return NestedField(field_id=field_id, name=field_name, field_type=LongType())
        elif field_type == 'timestamp':
            return NestedField(field_id=field_id, name=field_name, field_type=TimestampType())
        elif field_type == 'string':
            return NestedField(field_id=field_id, name=field_name, field_type=StringType())
        elif field_type == 'bool':
            return NestedField(field_id=field_id, name=field_name, field_type=BooleanType())
        else:
            raise ValueError(f"Invalid field type [{field_type}]")
    
    def _get_partition_field(self, id: int, partition_column: str, partition_type: str) -> PartitionSpec:
        if partition_type == 'day':
            _transform = DayTransform()
        elif partition_type == 'hour':
            _transform = HourTransform()
        elif partition_type == 'identity':
            _transform = IdentityTransform()
        else:
            raise ValueError(f"Invalid transform type [{partition_type}]")
            
        return PartitionField(
                source_id=id,
                field_id=1000 + id,
                name=partition_column,
                transform=_transform
            )
    
    def _load_schema_and_partition(self, table_name: str):
        """테이블의 스키마와 파티션을 설정합니다."""
        table_config = next(t for t in self.config['tables'] if t['name'] == table_name)
        schema_fields = []
        
        for field in table_config['schema']['fields']:
            schema_fields.append(self._get_schema_field(field_id=len(schema_fields) + 1, field_name=field['name'], field_type=field['type']))

        schema = Schema(*schema_fields)
        name_to_field_id = {field.name: field.field_id for field in schema.fields}
        partition_type = table_config['partition']['type']
        partition_column = table_config['partition']['column']
        partition_spec = PartitionSpec(
            self._get_partition_field(id=name_to_field_id[partition_column], partition_column=partition_column, partition_type=partition_type)
        )
        
        return schema, partition_spec
    
    
    def _load_schema_and_partition_v2(self, table_name: str):
        from app.domain.btc.dto.cq.block import BitcoinBlock
        from app.domain.btc.dto.cq.transaction import BitcoinTransaction
        
        if table_name == "block":
            _model = BitcoinBlock

        schema = self.create_schema_from_dataclass(dataclass_instance=_model)
        print(schema)
        name_to_field_id = {field.name: field.field_id for field in schema.fields}
        partition_type = "day"
        partition_column = "block_time"
        partition_spec = PartitionSpec(
            self._get_partition_field(id=name_to_field_id[partition_column], partition_column=partition_column, partition_type=partition_type)
        )
        
        return schema, partition_spec
    

    def create_table(self, db_name: str, table_name: str):
        """Iceberg 테이블을 생성합니다."""
        identifier = f"{db_name}.{table_name}"
        # schema, partition_spec = self._load_schema_and_partition(table_name)
        schema, partition_spec = self._load_schema_and_partition_v2(table_name)
        
        return self.catalog.create_table_if_not_exists(
            identifier=identifier,
            location=f"{self.warehouse_loc}{table_name}",
            schema=schema,
            partition_spec=partition_spec,
            properties={
                "write.delete.mode": "copy-on-write",
                "write.update.mode": "copy-on-write",
                "write.merge.mode": "copy-on-write",
                "write.target-file-size-bytes": "134217728",  # 128MB
            }
        )

    def load_table(self, db_name: str, table_name: str):
        """Iceberg 테이블을 로드합니다."""
        identifier = f"{db_name}.{table_name}"
        
        return self.catalog.load_table(identifier)