from app.domain.btc.dto.node.raw_data import BitcoinRawBlock, BitcoinRawVin
from app.domain.btc.dto.cq.transaction import BitcoinTransaction
from datetime import datetime, timezone


class BitcoinTransactionConverter:
    
    @classmethod
    def convert_node_data_to_cq_data(cls, node_raw_data: BitcoinRawBlock) -> list:
        txes = []
        for i, tx in enumerate(node_raw_data.tx):
            txes.append(
                BitcoinTransaction(
                    block_height=node_raw_data.height,
                    block_time=datetime.strftime(datetime.fromtimestamp(timestamp=node_raw_data.time,tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                    block_timestamp=node_raw_data.time,
                    block_hash=node_raw_data.hash,
                    transaction_hash=tx.txid,
                    is_coinbase_transaction=True if i == 0 else False,
                    size=tx.size,
                    virtual_size=tx.vsize,
                    weight=tx.weight,
                    version=tx.version,
                    lock_time=tx.locktime,
                    fee=tx.fee,
                    hex=tx.hex,
                    spent_utxo_count=len([vin for vin in tx.vin if isinstance(vin, BitcoinRawVin)]),
                    created_utxo_count=len([vout for vout in tx.vout]),
                    spent_utxo_total=sum([vin.prevout.value for vin in tx.vin if isinstance(vin, BitcoinRawVin)]),
                    created_utxo_total=sum([vout.value for vout in tx.vout]),
                    updated_at=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                )
            )
        return txes
        