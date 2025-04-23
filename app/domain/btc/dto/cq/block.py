from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass


@dataclass
class BitcoinBlock:
    __slots__ = [
        "block_height",
        "block_time",
        "block_timestamp",
        "block_hash",
        "merkle_root",
        "chain_work",
        "coinbase_script",
        "coinbase_transaction_hash",
        "bits",
        "size",
        "stripped_size",
        "weight",
        "version",
        "difficulty",
        "fee_reward",
        "block_reward",
        "transaction_count",
        "spent_utxo_count",
        "created_utxo_count",
        "spent_utxo_total",
        "created_utxo_total",
        "updated_at",
    ]

    block_height: int
    block_time: datetime
    block_timestamp: int
    block_hash: str

    merkle_root: str
    chain_work: str
    coinbase_script: str
    coinbase_transaction_hash: str

    bits: str
    size: int
    stripped_size: int
    weight: int
    version: int

    difficulty: Decimal
    fee_reward: Decimal
    block_reward: Decimal

    transaction_count: int
    spent_utxo_count: int
    created_utxo_count: int
    spent_utxo_total: Decimal
    created_utxo_total: Decimal

    updated_at: datetime
