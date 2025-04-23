from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass


@dataclass
class BitcoinTransactionInput:
    __slots__ = [
        "block_height",
        "block_time",
        "block_timestamp",
        "transaction_hash",
        "transaction_index",
        "value",
        "estimated_value",
        "owner_count",
        "address",
        "address_type",
        "prevout_transaction_hash",
        "prevout_transaction_index",
        "prevout_block_height",
        "prevout_block_time",
        "prevout_block_timestamp",
        "updated_at",
    ]

    block_height: int
    block_time: datetime
    block_timestamp: int
    block_hash: str

    transaction_hash: str
    transaction_index: int
    value: Decimal
    estimated_value: Decimal
    owner_count: int
    address: str
    address_type: str

    prevout_transaction_hash: str
    prevout_transaction_index: int
    prevout_block_height: int
    prevout_block_time: datetime
    prevout_block_timestamp: int

    updated_at: datetime
