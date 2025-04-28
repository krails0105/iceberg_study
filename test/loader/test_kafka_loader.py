from app.loader.kafka_loader import KafkaLoader, json_serializer
from app.domain.btc.dto.node.raw_data import BitcoinRawBlock
from app.converter.btc.block_converter import BitcoinBlockConverter
from app.converter.btc.transaction_converter import BitcoinTransactionConverter
from app.converter.btc.transaction_input_converter import BitcoinTransactionInputConverter
from app.converter.btc.transaction_output_converter import BitcoinTransactionOutputConverter
from app.domain.btc.dto.cq.block import BitcoinBlock
from app.domain.btc.dto.cq.transaction import BitcoinTransaction
from app.domain.btc.dto.cq.transaction_input import BitcoinTransactionInput
from app.domain.btc.dto.cq.transaction_output import BitcoinTransactionOutput
from dataclasses import asdict
from typing import List
import json
from unittest.mock import MagicMock


def test_kafka_batch_upload(monkeypatch):
    mock_producer = MagicMock()
    monkeypatch.setattr("app.loader.kafka_loader.Producer", lambda conf: mock_producer)
    
    loader = KafkaLoader()
    with open("/Users/test/Workspace/CQ/tech-multichain-etl/tests/block887980.json") as r:
        raw_block = json.loads(r.read())
        
    ret: BitcoinBlock = BitcoinBlockConverter.convert_node_data_to_cq_data(node_raw_data=BitcoinRawBlock(**raw_block))
    loader.batch_upload(chain="btc", data_type="block", data_list=[asdict(ret)])
    mock_producer.produce.assert_called_once_with(topic="btc_block", key=None, value=json.dumps(asdict(ret), default=json_serializer).encode("utf-8"), on_delivery=loader.delivery_report)
    
    ret: List[BitcoinTransaction] = BitcoinTransactionConverter.convert_node_data_to_cq_data(node_raw_data=BitcoinRawBlock(**raw_block))
    loader.batch_upload(chain="btc", data_type="transaction", data_list=[asdict(ret[0])])
    mock_producer.produce.assert_called_with(topic="btc_transaction", key=None, value=json.dumps(asdict(ret[0]), default=json_serializer).encode("utf-8"), on_delivery=loader.delivery_report)
    
    ret: BitcoinTransactionInput = BitcoinTransactionInputConverter.convert_node_data_to_cq_data(node_raw_data=BitcoinRawBlock(**raw_block))
    loader.batch_upload(chain="btc", data_type="transaction_input", data_list=[asdict(ret[0])])
    mock_producer.produce.assert_called_with(topic="btc_transaction_input", key=None, value=json.dumps(asdict(ret[0]), default=json_serializer).encode("utf-8"), on_delivery=loader.delivery_report)
    
    ret: BitcoinTransactionOutput = BitcoinTransactionOutputConverter.convert_node_data_to_cq_data(node_raw_data=BitcoinRawBlock(**raw_block))
    loader.batch_upload(chain="btc", data_type="transaction_output", data_list=[asdict(ret[0])])
    mock_producer.produce.assert_called_with(topic="btc_transaction_output", key=None, value=json.dumps(asdict(ret[0]), default=json_serializer).encode("utf-8"), on_delivery=loader.delivery_report)
    