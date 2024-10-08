import abc
import model

from sqlalchemy import text


# Port.
class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self, reference) -> model.Batch:
        raise NotImplementedError


# Adapters:
class SqlAlchemyRepository(AbstractRepository):
    # Mappers.
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, reference):
        return self.session.query(model.Batch).filter_by(reference=reference).one()

    def list(self):
        return self.session.query(model.Batch).all()


class FakeRepository(AbstractRepository):
    def __init__(self, batches=None):
        if batches:
            self._batches = set(batches)
        else:
            self._batches = set()

    def add(self, batch):
        self._batches.add(batch)

    def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    def list(self):
        return list(self._batches)


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        # Check if batch exists
        existing_batch_row = self.session.execute(
            text("SELECT id FROM batches WHERE reference = :reference"),
            {"reference": batch.reference},
        ).fetchone()
        if existing_batch_row is None:
            # Insert batch
            self.session.execute(
                text(
                    "INSERT INTO batches (reference, sku, _purchased_quantity, eta) "
                    "VALUES (:reference, :sku, :purchased_quantity, :eta)"
                ),
                {
                    "reference": batch.reference,
                    "sku": batch.sku,
                    "purchased_quantity": batch._purchased_quantity,
                    "eta": batch.eta,
                },
            )
            batch_id = self.session.execute(
                text("SELECT id FROM batches WHERE reference = :reference"),
                {"reference": batch.reference},
            ).fetchone()[0]
        else:
            batch_id = existing_batch_row[0]

        # Now handle allocations
        for line in batch._allocations:
            # Ensure the order line exists
            order_line_row = self.session.execute(
                text(
                    "SELECT id FROM order_lines WHERE orderid = :orderid AND sku = :sku"
                ),
                {"orderid": line.orderid, "sku": line.sku},
            ).fetchone()
            if order_line_row is None:
                # Insert order line
                self.session.execute(
                    text(
                        "INSERT INTO order_lines (orderid, sku, qty) "
                        "VALUES (:orderid, :sku, :qty)"
                    ),
                    {"orderid": line.orderid, "sku": line.sku, "qty": line.qty},
                )
                order_line_id = self.session.execute(
                    text(
                        "SELECT id FROM order_lines WHERE orderid = :orderid AND sku = :sku"
                    ),
                    {"orderid": line.orderid, "sku": line.sku},
                ).fetchone()[0]
            else:
                order_line_id = order_line_row[0]

            # Check if allocation exists
            allocation_exists = self.session.execute(
                text(
                    "SELECT 1 FROM allocations WHERE orderline_id = :orderline_id AND batch_id = :batch_id"
                ),
                {"orderline_id": order_line_id, "batch_id": batch_id},
            ).fetchone()
            if allocation_exists is None:
                # Insert allocation
                self.session.execute(
                    text(
                        "INSERT INTO allocations (orderline_id, batch_id) "
                        "VALUES (:orderline_id, :batch_id)"
                    ),
                    {"orderline_id": order_line_id, "batch_id": batch_id},
                )

    def _get_allocations_lines(self, batchid):
        rows = list(
            self.session.execute(
                text(
                    "SELECT order_lines.orderid, order_lines.sku, order_lines.qty"
                    " FROM allocations"
                    " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                    " WHERE allocations.batch_id = :batchid"
                ),
                {"batchid": batchid},
            )
        )
        return rows

    def get(self, reference):
        rows = self.session.execute(
            text(
                "SELECT id, reference, sku, _purchased_quantity, eta "
                "FROM batches WHERE reference = :reference"
            ),
            {"reference": reference},
        )
        row = next(rows)
        batchid = row[0]
        batch = model.Batch(ref=row[1], sku=row[2], qty=row[3], eta=row[4])
        # Get allocations.
        lines = self._get_allocations_lines(batchid)
        allocations = {
            model.OrderLine(
                orderid=line[0],
                sku=line[1],
                qty=line[2],
            )
            for line in lines
        }
        for allocated_line in allocations:
            batch.allocate(allocated_line)
        return batch

    def list(self) -> list:
        return list(
            self.session.execute(
                text("SELECT reference, sku, _purchased_quantity, eta FROM batches")
            )
        )
