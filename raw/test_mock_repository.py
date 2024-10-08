import pytest

from raw.after import MockRepository, Restaurant


def test_add_restaurant():
    repo = MockRepository()
    repo.add(Restaurant(name="Burger King", address="123 Main St"))
    repo.add(Restaurant(name="Burger King", address="123 Main St"))
    assert repo.get(1).name == "Burger King"
    assert repo.get(2).name == "Burger King"


def test_get_restaurant(mock_restaurants_repo):
    repo = mock_restaurants_repo
    assert repo.get(1).name == "Burger King"
    assert repo.get(2).name == "McDonald's"
