import contextlib
import sqlite3

from dataclasses import dataclass


@contextlib.contextmanager
def connect(db_path: str):
    with sqlite3.connect(db_path) as conn:
        yield conn.cursor()


@dataclass
class Restaurant:
    id: int
    name: str
    address: str

    @classmethod
    def create_table(cls, db_path: str) -> None:
        with connect(db_path) as cursor:
            cursor.execute(
                """
                CREATE TABLE restaurants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    address TEXT NOT NULL
                )
                """
            )

    @classmethod
    def get_restaurant(cls, db_path: str, id: int) -> "Restaurant":
        with connect(db_path) as cursor:
            cursor.execute(
                """
                SELECT name, address
                FROM restaurants
                WHERE id=:id
                """,
                {"id": id},
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"No restaurant with id {id}")
            return Restaurant(name=row[0], address=row[1], id=id)

    @classmethod
    def get_all_restaurants(cls, db_path: str) -> list["Restaurant"]:
        with connect(db_path) as cursor:
            cursor.execute(
                """
                SELECT id, name, address
                FROM restaurants
                """
            )
            return [
                Restaurant(name=row[1], address=row[2], id=row[0])
                for row in cursor.fetchall()
            ]

    @classmethod
    def add_restaurant(cls, db_path: str, name: str, address: str) -> None:
        with connect(db_path) as cursor:
            cursor.execute(
                """
                INSERT INTO restaurants (name, address)
                VALUES (:name, :address)
                """,
                {
                    "name": name,
                    "address": address,
                },
            )

    @classmethod
    def update_restaurant(cls, db_path: str, id: int, name: str, address: str) -> None:
        with connect(db_path) as cursor:
            cursor.execute(
                """
                UPDATE restaurants
                SET name=:name, address=:address
                WHERE id=:id
                """,
                {
                    "name": name,
                    "address": address,
                    "id": id,
                },
            )

    @classmethod
    def delete_restaurant(cls, db_path: str, id: int) -> None:
        with connect(db_path) as cursor:
            cursor.execute(
                """
                DELETE FROM restaurants
                WHERE id=:id
                """,
                {"id": id},
            )


def main():
    Restaurant.create_table("restaurants_before.db")
    # Restaurant.create_empty_csv_file("restaurants_before.db")
    # Restaurant.create_spreedsheet("urlll....")
    Restaurant.add_restaurant("restaurants_before.db", "Venom", "123 Main St")
    Restaurant.add_restaurant("restaurants_before.db", "Tanta", "456 Elm St")
    Restaurant.add_restaurant("restaurants_before.db", "McDonalds", "456 Elm St")
    Restaurant.add_restaurant(
        "restaurants_before.db", "Central mercado", "789 Maple St"
    )
    # Print all the restaurants.
    restaurants = Restaurant.get_all_restaurants("restaurants_before.db")
    for restaurant in restaurants:
        print(restaurant)


if __name__ == "__main__":
    main()
