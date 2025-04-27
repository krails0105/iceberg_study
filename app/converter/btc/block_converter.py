from app.domain.btc.dto.node.raw_data import BitcoinRawBlock, BitcoinRawVin
from app.domain.btc.dto.cq.block import BitcoinBlock
from datetime import datetime, timezone


class BitcoinBlockConverter:
    
    @classmethod
    def convert_node_data_to_cq_data(cls, node_raw_data: BitcoinRawBlock):
        return BitcoinBlock(
            block_height=node_raw_data.height,
            block_time=datetime.strftime(datetime.fromtimestamp(timestamp=node_raw_data.time,tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
            block_timestamp=node_raw_data.time,
            block_hash=node_raw_data.hash,
            merkle_root=node_raw_data.merkleroot,
            chain_work=node_raw_data.chainwork,
            coinbase_script=node_raw_data.tx[0].vin[0].coinbase,
            coinbase_transaction_hash=node_raw_data.tx[0].txid,
            bits=node_raw_data.bits,
            size=node_raw_data.size,
            stripped_size=node_raw_data.strippedsize,
            weight=node_raw_data.weight,
            version=node_raw_data.version,
            difficulty=node_raw_data.difficulty,
            fee_reward=sum([tx.fee for tx in node_raw_data.tx if tx.fee is not None]),
            block_reward=sum([vout.value for vout in node_raw_data.tx[0].vout]),
            transaction_count=node_raw_data.nTx,
            spent_utxo_count=len([vin for tx in node_raw_data.tx for vin in tx.vin if isinstance(vin, BitcoinRawVin)]),
            created_utxo_count=len([vout for tx in node_raw_data.tx for vout in tx.vout if vout.value > 0]),
            spent_utxo_total=sum([vin.prevout.value for tx in node_raw_data.tx for vin in tx.vin if isinstance(vin, BitcoinRawVin)]),
            created_utxo_total=sum([vout.value for tx in node_raw_data.tx for vout in tx.vout]),
            updated_at=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
        )
        