from copy import deepcopy
import json

        
class BitcoinNodeRpcFactory:
    param_form = {"method": "", "params": [], "id": 1, "jsonrpc": "2.0"}
    
    @classmethod
    def get_block_request_body(cls, block_hash: str):
        
        _param_form = deepcopy(cls.param_form)
        _param_form["method"] = "getblock"
        _param_form["params"] = [block_hash, 3]
        return json.dumps(_param_form)