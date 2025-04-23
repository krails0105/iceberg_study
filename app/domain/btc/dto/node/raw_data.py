from typing import List, Union
from decimal import Decimal
from pydantic import BaseModel


class BitcoinRawVinScriptSig(BaseModel):
    asm: str
    hex: str


class BitcoinRawScriptPubKey(BaseModel):
    asm: str
    desc: str
    hex: str
    type: str
    address: Union[str, None] = None


class BitcoinRawVinPrevout(BaseModel):
    generated: bool
    height: int
    value: Decimal
    scriptPubKey: BitcoinRawScriptPubKey


class BitcoinRawVin(BaseModel):
    txid: str
    vout: int
    scriptSig: BitcoinRawVinScriptSig
    prevout: BitcoinRawVinPrevout
    sequence: int


class BitcoinRawVinCoinbase(BaseModel):
    coinbase: str
    txinwitness: Union[list, None] = None
    sequence: int


class BitcoinRawVout(BaseModel):
    value: Decimal
    n: int
    scriptPubKey: BitcoinRawScriptPubKey


class BitcoinRawTransaction(BaseModel):
    txid: str
    hash: str
    version: int
    size: int
    vsize: int
    weight: int
    locktime: int
    vin: List[Union[BitcoinRawVinCoinbase, BitcoinRawVin]]
    vout: List[BitcoinRawVout]
    fee: Union[Decimal, None] = None
    hex: str


class BitcoinRawBlock(BaseModel):
    hash: str
    confirmations: int
    height: int
    version: int
    versionHex: str
    merkleroot: str
    time: int
    mediantime: int
    nonce: int
    bits: str
    difficulty: Decimal
    chainwork: str
    nTx: int
    previousblockhash: Union[str, None] = None
    nextblockhash: Union[str, None] = None 
    strippedsize: int
    size: int
    weight: int
    tx: List[BitcoinRawTransaction]