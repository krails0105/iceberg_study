from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, NewType

import pendulum

from enums import Categories
from xrpl.models.requests.request import LookupByLedgerRequest, Request, RequestMethod
from xrpl.models.utils import require_kwargs_on_init


Ledger = NewType("Ledger", Dict[str, Any])


@require_kwargs_on_init
@dataclass(frozen=True)
class ClioLedger(Request, LookupByLedgerRequest):
    """
    Retrieve information about the public ledger.
    `See ledger <https://xrpl.org/ledger.html>`_
    """

    method: RequestMethod = field(default=RequestMethod.LEDGER, init=False)
    transactions: bool = False
    expand: bool = False
    owner_funds: bool = False
    binary: bool = False
    queue: bool = False
    diff: bool = False


class XrplLedgerDataModel:
    # not define every field since we only need index, close_time_human

    @classmethod
    def get_blocktime_height_map(cls, upload_data_by_block: Dict[int, List[Tuple[Categories, Ledger]]]) -> List[Tuple[int, datetime]]:
        """get blocktime, height map from upload_data_by_block
        :param upload_data_by_block: upload data by block
        (ex. {1001: [(LEDGER, {...})], 1002: [(LEDGER, {...})]})
        :return: blocktime, height map
        """
        blocktime_height_map: List[Tuple[int, datetime]] = []

        for ledger_index, raw_data in upload_data_by_block.items():
            _, raw_ledger = raw_data[0]
            close_time_human = raw_ledger["result"]["ledger"]["close_time_human"]
            close_datetime = cls.convert_datetime_string_to_datetime(close_time_human, "%Y-%b-%d %H:%M:%S.%f000 UTC")
            blocktime_height_map.append((ledger_index, close_datetime))

        return blocktime_height_map

    @classmethod
    def convert_datetime_string_to_datetime(cls, datetime_string: str, fmt: str) -> datetime:
        return datetime.strptime(datetime_string, fmt).replace(tzinfo=pendulum.timezone("UTC"))
