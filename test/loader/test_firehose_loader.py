from app.loader.firehose_loader import FirehoseClient
from app.domain.btc.dto.node.raw_data import BitcoinRawBlock
from app.converter.btc.block_converter import BitcoinBlockConverter
from app.handler.iceberg_handler import IcebergHandler
from app.domain.btc.dto.cq.block import BitcoinBlock
from dataclasses import asdict
from datetime import datetime, timezone
import json


def test_firehose_load():
    client = FirehoseClient(delivery_stream_name = 'PUT-ICE-btc-block-shkim', region='us-east-1')
    with open("/Users/test/Workspace/CQ/tech-multichain-etl/tests/block887980.json") as r:
        ret = json.loads(r.read())
    # print(ret)
    ret: BitcoinBlock = BitcoinBlockConverter.convert_node_data_to_cq_data(node_raw_data=BitcoinRawBlock(**ret))
    print(asdict(ret))
    client.put_record(record=asdict(ret))