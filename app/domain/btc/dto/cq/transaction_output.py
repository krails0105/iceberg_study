from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass


@dataclass
class BitcoinTransactionOutput:
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

    updated_at: datetime
