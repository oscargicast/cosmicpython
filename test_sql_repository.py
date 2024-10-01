# pylint: disable=protected-access
import model
import repository

from sqlalchemy import text


def test_repository_can_save_a_batch(raw_sql_session):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)

    repo = repository.SqlRepository(raw_sql_session)
    repo.add(batch)
    raw_sql_session.commit()

    rows = raw_sql_session.execute(
        text('SELECT reference, sku, _purchased_quantity, eta FROM "batches"')
    )
    assert list(rows) == [("batch1", "RUSTY-SOAPDISH", 100, None)]


def insert_order_line(raw_sql_session):
    raw_sql_session.execute(
        text(
            "INSERT INTO order_lines (orderid, sku, qty)"
            ' VALUES ("order1", "GENERIC-SOFA", 12)'
        )
    )
    # We use parameter binding to avoid SQL injection rather
    # than interpolating the values directly into the SQL string (with f-strings).
    [[orderline_id]] = raw_sql_session.execute(
        text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        dict(orderid="order1", sku="GENERIC-SOFA"),
    )
    return orderline_id


def insert_batch(raw_sql_session, batch_id):
    raw_sql_session.execute(
        text(
            "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
            ' VALUES (:batch_id, "GENERIC-SOFA", 100, null)'
        ),
        dict(batch_id=batch_id),
    )
    [[batch_id]] = raw_sql_session.execute(
        text('SELECT id FROM batches WHERE reference=:batch_id AND sku="GENERIC-SOFA"'),
        dict(batch_id=batch_id),
    )
    return batch_id


def insert_allocation(raw_sql_session, orderline_id, batch_id):
    raw_sql_session.execute(
        text(
            "INSERT INTO allocations (orderline_id, batch_id)"
            " VALUES (:orderline_id, :batch_id)"
        ),
        dict(orderline_id=orderline_id, batch_id=batch_id),
    )


def test_repository_can_retrieve_a_batch_with_allocations(raw_sql_session):
    orderline_id = insert_order_line(raw_sql_session)
    batch1_id = insert_batch(raw_sql_session, "batch1")
    insert_batch(raw_sql_session, "batch2")
    insert_allocation(raw_sql_session, orderline_id, batch1_id)

    repo = repository.SqlRepository(raw_sql_session)
    retrieved = repo.get("batch1")

    expected = model.Batch("batch1", "GENERIC-SOFA", 100, eta=None)
    assert retrieved == expected  # Batch.__eq__ only compares reference
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12),
    }


def get_allocations(raw_sql_session, batchid):
    rows = list(
        raw_sql_session.execute(
            text(
                "SELECT order_lines.orderid"
                " FROM allocations"
                " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                " JOIN batches ON allocations.batch_id = batches.id"
                " WHERE batches.reference = :batchid"
            ),
            dict(batchid=batchid),
        )
    )
    return {row[0] for row in rows}


def test_updating_a_batch(raw_sql_session):
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)
    batch.allocate(order1)

    repo = repository.SqlRepository(raw_sql_session)
    repo.add(batch)
    raw_sql_session.commit()

    batch.allocate(order2)
    repo.add(batch)
    raw_sql_session.commit()

    assert get_allocations(raw_sql_session, "batch1") == {"order1", "order2"}
