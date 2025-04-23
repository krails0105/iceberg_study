from pytest import fixture
from app.loader.iceberg.iceberg_loader import IcebergLoader


@fixture
def loader():
    return IcebergLoader()

def test_load(loader: IcebergLoader):
    loader._init_catalog()