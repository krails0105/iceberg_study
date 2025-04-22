from pytest import fixture
from app.loader.iceberg import IcebergLoader


@fixture
def loader():
    return iceberg_loader()

def test_load(loader: )