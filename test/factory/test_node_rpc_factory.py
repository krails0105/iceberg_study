from pytest import fixture
from app.factory.node_rpc_factory import BitcoinNodeRpcFactory

@fixture
def node_rpc_factory() -> BitcoinNodeRpcFactory:
    return BitcoinNodeRpcFactory()

def test_get_block_request_body(node_rpc_factory: BitcoinNodeRpcFactory):
    test_block_hash = "0000000000000000000057aac5f21e20cd1866e9fb9b632e4f0b715d3df4ee55"
    ret = node_rpc_factory.get_block_request_body(block_hash=test_block_hash)
    print(ret)
    