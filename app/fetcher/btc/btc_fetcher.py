from app.fetcher import Fetcher
from app.config import NodeRpcConfig
from app.factory.node_rpc_factory import BitcoinNodeRpcFactory
from requests import get, post

class BitcoinFetcher(Fetcher):
    
    def __init__(self):
        super().__init__()
        self.chain: str = "btc"
        self.node_url: str = NodeRpcConfig.get_node_url(chain=self.chain)
        
    def fetch_block(self):
        block_hash = "0000000000000000000057aac5f21e20cd1866e9fb9b632e4f0b715d3df4ee55"
        request_body = BitcoinNodeRpcFactory.get_block_request_body(block_hash=block_hash)
        try:
            return post(url=self.node_url, data=request_body)
        except Exception as e:
            raise e
 