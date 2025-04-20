from app.loader import Loader
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, TimestampType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import HourTransform

class IcebergLoader(Loader):
    
    def _init_catalog(cls):
        cls.catalog = load_catalog()
    
    def load(cls):
        pass