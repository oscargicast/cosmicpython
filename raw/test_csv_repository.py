from raw.after import CSVRepository, Restaurant


def test_add_restaurant(csv_restaurants_repo):
    repo = csv_restaurants_repo
    assert repo.get(1).name == "Burger King"
    assert repo.get(2).name == "McDonald's"
