from app.domain.btc.dto.node.raw_data import BitcoinRawBlock, BitcoinRawVin
from app.domain.btc.dto.cq.transaction_output import BitcoinTransactionOutput
from datetime import datetime, timezone


class BitcoinTransactionOutputConverter:
    
    @classmethod
    def convert_node_data_to_cq_data(cls, node_raw_data: BitcoinRawBlock):
        vouts = []
        for tx in node_raw_data.tx:
            for vout in tx.vout:
                vouts.append(
                BitcoinTransactionOutput(
                    block_height=node_raw_data.height,
                    block_time=datetime.strftime(datetime.fromtimestamp(timestamp=node_raw_data.time,tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                    block_timestamp=node_raw_data.time,
                    block_hash=node_raw_data.hash,
                    transaction_hash=tx.txid,
                    transaction_index=vout.n,
                    value=round(vout.value, 9),
                    address_type=vout.scriptPubKey.type,
                    address=vout.scriptPubKey.address,
                    script_pub_key_asm=vout.scriptPubKey.asm,
                    script_pub_key_desc=vout.scriptPubKey.desc,
                    script_pub_key_hex=vout.scriptPubKey.hex,
                    script_pub_key_address=vout.scriptPubKey.address,
                    updated_at=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                )
            )
        return vouts
        