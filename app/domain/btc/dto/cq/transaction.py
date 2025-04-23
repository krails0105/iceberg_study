from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass


@dataclass
class BitcoinTransaction:
    __slots__ = [
        "block_height",
        "block_time",
        "block_timestamp",
        "block_hash",
        "transaction_hash",
        "is_coinbase_transaction",
        "size",
        "virtual_size",
        "weight",
        "version",
        "lock_time",
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

    transaction_hash: str
    is_coinbase_transaction: bool
    size: int
    virtual_size: int
    weight: int
    version: int
    lock_time: int

    spent_utxo_count: int
    created_utxo_count: int
    spent_utxo_total: Decimal
    created_utxo_total: Decimal

    updated_at: datetime
