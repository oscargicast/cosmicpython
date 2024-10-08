import contextlib
import sqlite3

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Restaurant:
    name: str
    address: str
    id: int | None = None


class Repository[T](ABC):
    @abstractmethod
    def get(self, id: int) -> T:
        raise NotImplementedError

    @abstractmethod
    def get_all(self) -> list[T]:
        raise NotImplementedError

    @abstractmethod
    def add(self, **kwargs: object) -> None:
        raise NotImplementedError

    @abstractmethod
    def update(self, id: int, **kwargs: object) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: int) -> None:
        raise NotImplementedError


class SQLiteRepository(Repository[Restaurant]):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.create_table()

    @contextlib.contextmanager
    def connect(self):
        with sqlite3.connect(self.db_path) as conn:
            yield conn.cursor()

    def create_table(self):
        with self.connect() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS restaurants (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    address TEXT NOT NULL
                )
            """
            )

    def get(self, id: int) -> Restaurant:
        with self.connect() as cursor:
            cursor.execute("SELECT * FROM restaurants WHERE id = ?", (id,))
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Restaurant with id {id} not found")
            return Restaurant(*row)

    def get_all(self) -> list[Restaurant]:
        with self.connect() as cursor:
            cursor.execute("SELECT id, name, address FROM restaurants")
            rows = cursor.fetchall()
            restaurants: list[Restaurant] = []
            for row in rows:
                id, name, address = row
                restaurants.append(
                    Restaurant(
                        id=id,
                        name=name,
                        address=address,
                    )
                )
            return restaurants

    def add(self, **kwargs: object) -> None:
        name = kwargs.get("name")
        address = kwargs.get("address")
        if not name or not address:
            raise ValueError("Name and address are required")
        with self.connect() as cursor:
            cursor.execute(
                "INSERT INTO restaurants (name, address) VALUES (:name, :address)",
                {
                    "name": name,
                    "address": address,
                },
            )

    def update(self, id: int, **kwargs: object) -> None:
        name = kwargs.get("name")
        address = kwargs.get("address")
        if not name and not address:
            raise ValueError("Name or address is required")
        if "name" in kwargs:
            with self.connect() as cursor:
                cursor.execute(
                    "UPDATE restaurants SET name = :name WHERE id = :id",
                    {
                        "name": name,
                        "id": id,
                    },
                )
        if "address" in kwargs:
            with self.connect() as cursor:
                cursor.execute(
                    "UPDATE restaurants SET address = :address WHERE id = :id",
                    {
                        "address": address,
                        "id": id,
                    },
                )

    def delete(self, id: int) -> None:
        with self.connect() as cursor:
            cursor.execute("DELETE FROM restaurants WHERE id = ?", (id,))


class MockRepository(Repository[Restaurant]):
    def __init__(self, restaurants: dict[int, Restaurant] | None = None):
        self.restaurants = restaurants or {}

    def get(self, id: int) -> Restaurant:
        if id not in self.restaurants:
            raise ValueError(f"Restaurant with id {id} not found")
        return self.restaurants[id]

    def get_all(self) -> list[Restaurant]:
        return list(self.restaurants.values())

    def add(self, restaurant: Restaurant) -> None:
        self.restaurants[len(self.restaurants) + 1] = restaurant

    def update(self, restaurant: Restaurant) -> None:
        if restaurant.id is None:
            raise ValueError("Restaurant id is required")
        self.restaurants[restaurant.id] = restaurant

    def delete(self, restaurant: Restaurant) -> None:
        if restaurant.id is None:
            raise ValueError("Restaurant id is required")
        del self.restaurants[restaurant.id]


class CSVRepository(Repository[Restaurant]):
    def __init__(self, filename: str):
        self.filename = filename
        self.create_file()

    @contextlib.contextmanager
    def _open_file(self, mode="r"):
        with open(self.filename, mode) as file:
            yield file

    def _next_id(self) -> int:
        with self._open_file("r") as file:
            lines = file.readlines()
        return len(lines)

    def create_file(self):
        with self._open_file("w") as file:
            file.write("id,name,address\n")

    def add(self, restaurant: Restaurant) -> None:
        name = restaurant.name
        address = restaurant.address
        if not name or not address:
            raise ValueError("Name and address are required")
        with self._open_file("a") as file:
            file.write(f"{self._next_id()},{name},{address}\n")

    def get_all(self) -> list[Restaurant]:
        restaurants: list[Restaurant] = []
        with self._open_file("r") as file:
            lines = file.readlines()
            for line in lines[1:]:
                id, name, address = line.strip().split(",")
                restaurants.append(Restaurant(id=int(id), name=name, address=address))
        return restaurants

    def get(self, id: int) -> Restaurant:
        with self._open_file("r") as file:
            lines = file.readlines()
            for line in lines[1:]:
                line_id, name, address = line.strip().split(",")
                if int(line_id) == id:
                    return Restaurant(id=int(line_id), name=name, address=address)
            raise ValueError(f"Restaurant with id {id} not found")

    def delete(self, id: int) -> None:
        with self._open_file("r") as file:
            lines = file.readlines()
            new_lines = ["id,name,address\n"]
            for line in lines[1:]:
                line_id, _, _ = line.strip().split(",")
                if int(line_id) != id:
                    new_lines.append(line)
        with self._open_file("w") as file:
            file.writelines(new_lines)

    def update(self, id: int, **kwargs: object) -> None:
        pass


def main():
    # repo = SQLiteRepository("restaurants_after.db")
    repo = CSVRepository("restaurants_after.csv")
    repo.add(Restaurant(name="Pizza Hut", address="123 Main St"))
    repo.add(Restaurant(name="McDonald's", address="456 Elm St"))
    print(repo.get_all())

    repo.update(1, name="Pizza Hut Express")
    print(repo.get_all())

    repo.delete(2)
    print(repo.get_all())


if __name__ == "__main__":
    main()
