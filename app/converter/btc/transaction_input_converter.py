from app.domain.btc.dto.node.raw_data import BitcoinRawBlock, BitcoinRawVin
from app.domain.btc.dto.cq.transaction_input import BitcoinTransactionInput
from datetime import datetime, timezone


class BitcoinTransactionInputConverter:
    
    @classmethod
    def convert_node_data_to_cq_data(cls, node_raw_data: BitcoinRawBlock):
        vins = []
        block_height_set = set()
        for tx in node_raw_data.tx:
            for idx, vin in enumerate(tx.vin):
                if isinstance(vin, BitcoinRawVin):
                    vins.append(
                        BitcoinTransactionInput(
                            block_height=node_raw_data.height,
                            block_time=datetime.strftime(datetime.fromtimestamp(timestamp=node_raw_data.time,tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                            block_timestamp=node_raw_data.time,
                            block_hash=node_raw_data.hash,
                            transaction_hash=tx.txid,
                            transaction_index=idx,
                            value=round(vin.prevout.value, 9),
                            address_type=vin.prevout.scriptPubKey.type,
                            address=vin.prevout.scriptPubKey.address,
                            coinbase=None,
                            txinwitness=None,
                            sequence=vin.sequence,
                            prevout_block_height=vin.prevout.height,
                            prevout_transaction_hash=vin.txid,
                            prevout_transaction_index=vin.vout,
                            script_sig_asm=vin.scriptSig.asm,
                            script_sig_hex=vin.scriptSig.hex,
                            script_pub_key_asm=vin.prevout.scriptPubKey.asm,
                            script_pub_key_desc=vin.prevout.scriptPubKey.desc,
                            script_pub_key_hex=vin.prevout.scriptPubKey.hex,
                            script_pub_key_address=vin.prevout.scriptPubKey.address,
                            updated_at=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                        )
                    )
                    block_height_set.add(vin.prevout.height)
                else:  # coinbase vin (tx)
                    vins.append(
                        BitcoinTransactionInput(
                            block_height=node_raw_data.height,
                            block_time=datetime.strftime(datetime.fromtimestamp(timestamp=node_raw_data.time,tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                            block_timestamp=node_raw_data.time,
                            block_hash=node_raw_data.hash,
                            transaction_hash=tx.txid,
                            transaction_index=idx,
                            value=None,
                            address_type=None,
                            address=None,
                            coinbase=vin.coinbase,
                            txinwitness=vin.txinwitness,
                            sequence=vin.sequence,
                            prevout_block_height=None,
                            prevout_transaction_hash=None,
                            prevout_transaction_index=None,
                            script_sig_asm=None,
                            script_sig_hex=None,
                            script_pub_key_asm=None,
                            script_pub_key_desc=None,
                            script_pub_key_hex=None,
                            script_pub_key_address=None,
                            updated_at=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d %H:%M:%S"),
                        )
                    )
                    
        return vins
