from app.handler.iceberg_handler import IcebergHandler
from app.config import IcebergConfig
from pytest import fixture


@fixture
def iceberg_handler() -> IcebergHandler:
    # return IcebergHandler(config=IcebergConfig.load_config(config_name="btc_table_config.yaml"))
    return IcebergHandler(config=IcebergConfig.get_config())

def test_load_schema_and_partition(iceberg_handler: IcebergHandler):
    schema, partition = iceberg_handler._load_schema_and_partition(table_name="btc_table")
    print(schema, partition)
    
def test_load_schema_and_partition_v2(iceberg_handler: IcebergHandler):
    schema, partition = iceberg_handler._load_schema_and_partition_v2(table_name="block")
    print(schema, partition)
    
def test_create_schema_from_dataclass(iceberg_handler: IcebergHandler):
    from app.domain.btc.dto.node.raw_data import BitcoinRawBlock
    from app.converter.btc.block_converter import BitcoinBlockConverter
    import json
    
    with open("/Users/test/Workspace/CQ/tech-multichain-etl/tests/block887980.json") as r:
        ret = json.loads(r.read())
    ret = BitcoinBlockConverter.convert_node_data_to_cq_data(node_raw_data=BitcoinRawBlock(**ret))
    schema = iceberg_handler.create_schema_from_dataclass(dataclass_instance=ret)
    print(schema)
    
def test_create_table(iceberg_handler: IcebergHandler):
    table = iceberg_handler.create_table(db_name="default", table_name="block")
    print(table)
    
    ret = iceberg_handler.load_table(db_name="default", table_name="block")
    print(ret)
    # iceberg_handler.catalog.drop_table(identifier=("default","btc_table"))
    # iceberg_handler.catalog.purge_table(identifier=("default","btc_table"))
    
