from app.enum import Chain

class NodeRpcConfig:
    @classmethod
    def get_node_url(cls, chain: str):
        if chain.upper() == Chain.BTC:
            return 'https://restless-burned-grass.btc.quiknode.pro/933b0c939b6ef928c0c4cf1f3d1f700873518147/'
        else:
            raise ValueError(f"Invalid chain [{chain}]")
        

class IcebergConfig:
    warehouse_location = "s3://iceberg-shkim/iceberg/glue_catalog/tables/"
    