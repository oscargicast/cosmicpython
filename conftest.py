import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, clear_mappers

from orm import metadata, start_mappers

from raw.after import Restaurant, MockRepository, CSVRepository


@pytest.fixture
def in_memory_db():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


@pytest.fixture
def session(in_memory_db):
    start_mappers()
    yield sessionmaker(bind=in_memory_db)()
    clear_mappers()


@pytest.fixture
def raw_sql_session(in_memory_db):
    yield sessionmaker(bind=in_memory_db)()


@pytest.fixture
def mock_restaurants_repo():
    repo = MockRepository()
    repo.add(Restaurant(name="Burger King", address="123 Main St"))
    repo.add(Restaurant(name="McDonald's", address="456 Elm St"))
    repo.add(Restaurant(name="KFC", address="789 Oak St"))
    repo.add(Restaurant(name="Subway", address="101 Pine St"))
    repo.add(Restaurant(name="Pizza Hut", address="202 Maple St"))
    repo.add(Restaurant(name="Taco Bell", address="303 Cedar St"))
    return repo


@pytest.fixture
def csv_restaurants_repo(tmp_path):
    temp_filename = tmp_path / "example.txt"
    repo = CSVRepository(filename=temp_filename)
    repo.add(Restaurant(name="Burger King", address="123 Main St"))
    repo.add(Restaurant(name="McDonald's", address="456 Elm St"))
    repo.add(Restaurant(name="KFC", address="789 Oak St"))
    repo.add(Restaurant(name="Subway", address="101 Pine St"))
    repo.add(Restaurant(name="Pizza Hut", address="202 Maple St"))
    repo.add(Restaurant(name="Taco Bell", address="303 Cedar St"))
    return repo
